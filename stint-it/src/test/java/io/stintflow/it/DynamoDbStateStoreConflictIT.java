package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import io.stintflow.aws.DynamoDbStateStore;
import io.stintflow.core.Json;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.SaveOutcome;
import io.stintflow.spi.Wait;
import io.stintflow.spi.WorkflowRef;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * CA3 (SDD 1.2): a real DynamoDB (via floci) version conflict — two threads racing
 * {@code TransactWriteItems} for the same instance/wait — resolves to exactly one {@code OK} and one
 * {@code CONFLICT}, synchronized with a {@link CyclicBarrier} rather than a sleep.
 */
class DynamoDbStateStoreConflictIT {

    private static final WorkflowRef REF = new WorkflowRef("test", "dynamo-conflict", "1.0.0");
    private static final String INSTANCES_TABLE = "stint-instances";
    private static final String WAITS_TABLE = "stint-waits";

    @Test
    void two_threads_racing_a_transact_write_items_save_only_one_wins() throws Exception {
        try (GenericContainer<?> floci = new GenericContainer<>(DockerImageName.parse("floci/floci:latest"))
                .withExposedPorts(4566)) {
            floci.start();
            URI endpoint = URI.create("http://" + floci.getHost() + ":" + floci.getMappedPort(4566));
            var creds = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));

            try (DynamoDbClient ddb = DynamoDbClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                    .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build()) {

                DynamoDbTestTables.create(ddb, INSTANCES_TABLE, WAITS_TABLE);
                DynamoDbStateStore store = new DynamoDbStateStore(ddb, INSTANCES_TABLE, WAITS_TABLE);

                String instanceId = "inst-conflict-1";
                String waitKey = "task:corr-conflict-1";

                InstanceSnapshot initial = new InstanceSnapshot(instanceId, REF, "/do/0/echo", waitKey,
                        Json.obj(), InstanceStatus.WAITING, 1, Instant.now());
                store.save(initial, 0, List.of(new Wait(waitKey, instanceId, "/do/0/echo", Instant.now())), List.of())
                        .toCompletableFuture().get(10, TimeUnit.SECONDS);

                CyclicBarrier barrier = new CyclicBarrier(2);
                AtomicReference<SaveOutcome> outcomeA = new AtomicReference<>();
                AtomicReference<SaveOutcome> outcomeB = new AtomicReference<>();

                CompletableFuture<Void> threadA = CompletableFuture.runAsync(
                        () -> raceAttempt(store, instanceId, waitKey, barrier, outcomeA));
                CompletableFuture<Void> threadB = CompletableFuture.runAsync(
                        () -> raceAttempt(store, instanceId, waitKey, barrier, outcomeB));
                CompletableFuture.allOf(threadA, threadB).get(20, TimeUnit.SECONDS);

                assertThat(List.of(outcomeA.get(), outcomeB.get()))
                        .containsExactlyInAnyOrder(SaveOutcome.OK, SaveOutcome.CONFLICT);

                assertThat(store.findWait(waitKey).toCompletableFuture().get(10, TimeUnit.SECONDS)).isEmpty();
                InstanceSnapshot finalSnap = store.load(instanceId).toCompletableFuture().get(10, TimeUnit.SECONDS)
                        .orElseThrow();
                assertThat(finalSnap.version()).isEqualTo(2);
                assertThat(finalSnap.status()).isEqualTo(InstanceStatus.COMPLETED);
            }
        }
    }

    private static void raceAttempt(DynamoDbStateStore store, String instanceId, String waitKey,
            CyclicBarrier barrier, AtomicReference<SaveOutcome> result) {
        try {
            barrier.await(10, TimeUnit.SECONDS);
            InstanceSnapshot next = new InstanceSnapshot(instanceId, REF, "/do/1/after", null, Json.obj(),
                    InstanceStatus.COMPLETED, 2, Instant.now());
            SaveOutcome outcome = store.save(next, 1, List.of(), List.of(waitKey))
                    .toCompletableFuture().get(10, TimeUnit.SECONDS);
            result.set(outcome);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
