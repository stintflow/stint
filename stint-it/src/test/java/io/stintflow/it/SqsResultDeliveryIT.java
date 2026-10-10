package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.aws.SqsTaskTransport;
import io.stintflow.wire.CeWire;
import io.stintflow.wire.Json;
import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.TaskResultHandler;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * SDD 1.3: CA6 (a failure in the result handler does not lose the SQS message — it's left for
 * redelivery, RF9) and CA7's SQS half (a handler failure must not silently swallow the message).
 * Real floci queue, short visibility timeout so redelivery is observed quickly, not with a sleep —
 * we poll for the observable effect (a second handler invocation) instead.
 */
class SqsResultDeliveryIT {

    @Test
    void a_failing_result_handler_leaves_the_message_for_redelivery_then_succeeds() throws Exception {
        try (GenericContainer<?> floci = new GenericContainer<>(DockerImageName.parse("floci/floci:latest"))
                .withExposedPorts(4566)) {
            floci.start();
            URI endpoint = URI.create("http://" + floci.getHost() + ":" + floci.getMappedPort(4566));
            var creds = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));

            try (SqsClient sqs = SqsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                    .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build()) {

                String invokeUrl = sqs.createQueue(q -> q.queueName("stint-invoke-redelivery")).queueUrl();
                String resultUrl = sqs.createQueue(q -> q.queueName("stint-result-redelivery")
                        .attributes(Map.of(software.amazon.awssdk.services.sqs.model.QueueAttributeName.VISIBILITY_TIMEOUT, "2")))
                        .queueUrl();

                SqsTaskTransport transport = new SqsTaskTransport(sqs, invokeUrl, resultUrl);

                TaskResult result = TaskResult.completed("corr-redelivery-1", Json.obj());
                var event = new io.stintflow.wire.DefaultCloudEventCodec().toEvent(result, "inst-redelivery-1");
                sqs.sendMessage(b -> b.queueUrl(resultUrl).messageBody(new String(CeWire.toJson(event))));

                AtomicInteger attempts = new AtomicInteger();
                CompletableFuture<Void> secondAttemptSeen = new CompletableFuture<>();
                TaskResultHandler handler = r -> {
                    int n = attempts.incrementAndGet();
                    if (n == 1) {
                        return CompletableFuture.failedFuture(new RuntimeException("simulated transient failure"));
                    }
                    secondAttemptSeen.complete(null);
                    return CompletableFuture.completedFuture(null);
                };
                transport.onResult(handler);

                // The first attempt fails and must NOT delete the message; SQS redelivers it after the
                // (short) visibility timeout, and the second attempt succeeds.
                secondAttemptSeen.get(30, TimeUnit.SECONDS);
                assertThat(attempts.get()).isGreaterThanOrEqualTo(2);
            }
        }
    }
}
