package io.stintflow.example.smoke;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stintflow.wire.Json;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.scheduler.SchedulerClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

/**
 * SDD 2.7, CA2: the packaged smoke app — the JVM jar on {@code mvn verify}, the native binary with {@code -Pnative} —
 * runs {@code aws-smoke} end to end against floci, with the clients produced by the Quarkus AWS extensions:
 * <ul>
 *   <li>the task goes over SQS;</li>
 *   <li>its 20-minute timeout is armed on EventBridge Scheduler;</li>
 *   <li>state lives in DynamoDB;</li>
 *   <li>the fact reaches the EventBridge domain bus, which routes it to an SQS queue;</li>
 *   <li>the definition's cron is reconciled into EventBridge Scheduler.</li>
 * </ul>
 */
@QuarkusIntegrationTest
@QuarkusTestResource(FlociResource.class)
class AwsSmokeIT {

    @TestHTTPResource("/smoke")
    URL smoke;

    @Test
    void the_packaged_app_runs_a_workflow_across_every_aws_connector() throws Exception {
        HttpResponse<String> response = HttpClient.newHttpClient().send(HttpRequest.newBuilder(smoke.toURI())
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("{\"name\":\"smoke\"}"))
                        .timeout(Duration.ofSeconds(120)).build(),
                HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
        JsonNode context = Json.read(response.body().getBytes(StandardCharsets.UTF_8));
        assertThat(context.get("echo").asText()).isEqualTo("smoke");
        assertThat(context.get("chainId").asText()).startsWith("chn-");

        try (SqsClient sqs = SqsClient.builder().endpointOverride(FlociResource.endpoint).region(Region.US_EAST_1)
                .credentialsProvider(FlociResource.creds()).httpClient(UrlConnectionHttpClient.create()).build();
             SchedulerClient scheduler = SchedulerClient.builder().endpointOverride(FlociResource.endpoint)
                     .region(Region.US_EAST_1).credentialsProvider(FlociResource.creds())
                     .httpClient(UrlConnectionHttpClient.create()).build()) {
            Message fact = awaitMessage(sqs, FlociResource.factsQueue);
            JsonNode event = Json.read(fact.body().getBytes(StandardCharsets.UTF_8));
            assertThat(event.get("type").asText()).isEqualTo("io.stintflow.smoke.done.v1");
            assertThat(event.get("data").get("echo").asText()).isEqualTo("smoke");
            assertThat(event.get("chainid").asText()).isEqualTo(context.get("chainId").asText());

            assertThat(scheduler.listSchedules(r -> r.groupName("stint-schedules")).schedules()).hasSize(1);
        }
    }

    /** Long-poll receives (no sleep) until the fact shows up. */
    private static Message awaitMessage(SqsClient sqs, String queueUrl) {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            List<Message> messages = sqs.receiveMessage(r -> r.queueUrl(queueUrl).waitTimeSeconds(5)).messages();
            if (!messages.isEmpty()) {
                return messages.get(0);
            }
        }
        throw new AssertionError("No fact on " + queueUrl);
    }
}
