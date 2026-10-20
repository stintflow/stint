package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import io.stintflow.aws.DynamoDbStateStore;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.SaveOutcome;
import io.stintflow.spi.Wait;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/** SDD 3a.10: consuming a wait key and reopening it in the same save, against a real DynamoDB (floci). */
class DynamoDbReopenConsumedWaitIT {

    private static final WorkflowRef REF = new WorkflowRef("test", "reopen", "1.0.0");
    private static final String INSTANCES_TABLE = "stint-instances";
    private static final String WAITS_TABLE = "stint-waits";

    @Test
    void a_save_that_consumes_and_reopens_the_same_key_is_ok() throws Exception {
        try (GenericContainer<?> floci = new GenericContainer<>(DockerImageName.parse("floci/floci:latest"))
                .withExposedPorts(4566)) {
            floci.start();
            URI endpoint = URI.create("http://" + floci.getHost() + ":" + floci.getMappedPort(4566));
            var creds = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));
            try (DynamoDbClient ddb = DynamoDbClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                    .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build()) {
                DynamoDbTestTables.create(ddb, INSTANCES_TABLE, WAITS_TABLE);
                DynamoDbStateStore store = new DynamoDbStateStore(ddb, INSTANCES_TABLE, WAITS_TABLE);
                String key = "event:sdd.done:id:abc";

                store.save(snapshot(1), 0, List.of(new Wait(key, "inst-1", "/do/0/listen", Instant.now())), List.of())
                        .toCompletableFuture().get(10, TimeUnit.SECONDS);
                SaveOutcome outcome = store.save(snapshot(2), 1,
                        List.of(new Wait(key, "inst-1", "/do/1/listen", Instant.now())), List.of(key))
                        .toCompletableFuture().get(10, TimeUnit.SECONDS);

                assertThat(outcome).isEqualTo(SaveOutcome.OK);
                assertThat(store.findWait(key).toCompletableFuture().get().orElseThrow().position())
                        .isEqualTo("/do/1/listen");

                // A wait written before `gen` existed (no such attribute) reopens too; the counter then starts at 1.
                String old = "event:sdd.done:id:old";
                ddb.putItem(r -> r.tableName(WAITS_TABLE).item(Map.of(
                        "waitKey", AttributeValue.fromS(old), "instanceId", AttributeValue.fromS("inst-1"),
                        "position", AttributeValue.fromS("/do/0/listen"),
                        "createdAt", AttributeValue.fromN("1"))));
                assertThat(store.save(snapshot(3), 2,
                        List.of(new Wait(old, "inst-1", "/do/2/listen", Instant.now())), List.of(old))
                        .toCompletableFuture().get(10, TimeUnit.SECONDS)).isEqualTo(SaveOutcome.OK);
                assertThat(ddb.getItem(r -> r.tableName(WAITS_TABLE).key(Map.of("waitKey", AttributeValue.fromS(old))))
                        .item().get("gen").n()).isEqualTo("1");

                // CA6: a key that is gone, or held by another instance, is still a conflict.
                String gone = "event:sdd.done:id:gone";
                assertThat(store.save(snapshot(4), 3,
                        List.of(new Wait(gone, "inst-1", "/do/3/listen", Instant.now())), List.of(gone))
                        .toCompletableFuture().get(10, TimeUnit.SECONDS)).isEqualTo(SaveOutcome.CONFLICT);
                assertThat(store.save(snapshot(4), 3,
                        List.of(new Wait(old, "inst-other", "/do/3/listen", Instant.now())), List.of(old))
                        .toCompletableFuture().get(10, TimeUnit.SECONDS)).isEqualTo(SaveOutcome.CONFLICT);
            }
        }
    }

    private static InstanceSnapshot snapshot(long version) {
        return new InstanceSnapshot("inst-1", REF, "/do/0/listen", null, Json.obj(), InstanceStatus.WAITING,
                version, null, Instant.now(), null, null);
    }
}
