package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.aws.DynamoDbStateStore;
import io.stintflow.aws.SqsTaskTransport;
import io.stintflow.aws.worker.SqsWorkerBinding;
import io.stintflow.core.CancelOutcome;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerRuntime;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * SDD 3a.8, CA9, on floci: a {@code for} over {@code run: workflow} with the state in DynamoDB and the tasks of the
 * children over SQS, answered by a worker behind the SQS binding. The join gives the outputs in the order of {@code in};
 * cancelling the parent aborts the children that wait on the worker and leaves none of their waits behind. No sleeps.
 */
class FlociForRunIT {

    private static final WorkflowRef FAN = new WorkflowRef("fan", "fan", "1.0.0");
    private static final String HOLDING_LEAF = FanOutFixture.leaf("held").replace("task: work", "task: hold");

    @Test
    void the_join_and_the_cascade_work_over_dynamodb_and_sqs() throws Exception {
        try (GenericContainer<?> floci = new GenericContainer<>(DockerImageName.parse("floci/floci:latest"))
                .withExposedPorts(4566)) {
            floci.start();
            URI endpoint = URI.create("http://" + floci.getHost() + ":" + floci.getMappedPort(4566));
            var creds = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));
            CountDownLatch release = new CountDownLatch(1);
            try (SqsClient sqs = SqsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                    .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build();
                 DynamoDbClient ddb = DynamoDbClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                         .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build()) {
                DynamoDbTestTables.create(ddb, "stint-instances", "stint-waits");
                DynamoDbTestTables.createOutbox(ddb, "stint-outbox");
                StateStore state = new DynamoDbStateStore(ddb, "stint-instances", "stint-waits", "stint-outbox", 4);

                String invoke = sqs.createQueue(q -> q.queueName("fan-invoke")).queueUrl();
                String results = sqs.createQueue(q -> q.queueName("fan-results")).queueUrl();
                var blobs = new FilesystemBlobStore(Files.createTempDirectory("stint-3a8-floci"));
                TaskHandlerRegistry handlers = new TaskHandlerRegistry()
                        .register("work", ctx -> CompletableFuture.completedFuture(
                                Json.obj().put("n", ctx.input().path("n").asInt() * 10)))
                        .register("hold", ctx -> {
                            try {
                                release.await(60, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            return CompletableFuture.completedFuture(Json.obj().put("n", -1));
                        });
                try (SqsWorkerBinding worker = SqsWorkerBinding.builder().sqs(sqs).invokeQueueUrl(invoke)
                        .resultQueueUrl(results).runtime(new WorkerRuntime(handlers, blobs)).concurrency(8)
                        .waitTime(Duration.ofSeconds(1)).visibilityTimeout(Duration.ofSeconds(60)).build().start()) {
                    WorkflowRegistry registry = new WorkflowRegistry();
                    registry.register(FanOutFixture.load(FanOutFixture.leaf("leaf")));
                    registry.register(FanOutFixture.load(HOLDING_LEAF));
                    registry.register(FanOutFixture.load(FanOutFixture.parent("fan", "leaf", 3, "all", 50)));
                    registry.register(FanOutFixture.load(FanOutFixture.parent("held-fan", "held", 3, "all", 50)));
                    SqsTaskTransport transport = new SqsTaskTransport(sqs, invoke, results);
                    WorkflowEngine engine = new WorkflowEngine(registry, transport, state, new InMemoryTimerService(),
                            blobs);

                    // the join: six children, three at a time, the outputs in the order of in
                    String id = engine.start(FAN, FanOutFixture.items(6));
                    JsonNode done = engine.awaitCompletion(id, Duration.ofSeconds(60)).toCompletableFuture()
                            .get(90, TimeUnit.SECONDS);
                    assertThat(done.path("results")).extracting(r -> r.path("n").asInt())
                            .containsExactly(0, 10, 20, 30, 40, 50);

                    // the cascade: three children held by the worker, then the parent is cancelled
                    WorkflowRef held = new WorkflowRef("fan", "held-fan", "1.0.0");
                    String parent = engine.start(held, FanOutFixture.items(3));
                    List<String> children = List.of(parent + ":/do/0/fan:0", parent + ":/do/0/fan:1",
                            parent + ":/do/0/fan:2");
                    List<String> taskKeys = new java.util.ArrayList<>();
                    for (String child : children) {
                        taskKeys.add(awaitTaskWait(state, child));
                    }
                    assertThat(engine.cancel(parent, "withdrawn").toCompletableFuture().get(30, TimeUnit.SECONDS))
                            .isEqualTo(CancelOutcome.CANCELLED);

                    assertThat(status(state, parent)).isEqualTo(InstanceStatus.ABORTED);
                    for (String child : children) {
                        awaitStatus(state, child, InstanceStatus.ABORTED);
                    }
                    for (String key : taskKeys) {
                        assertThat(state.findWait(key).toCompletableFuture().get(10, TimeUnit.SECONDS)).isEmpty();
                    }
                    // the worker answers at last: the results are dropped, nothing comes back to life
                    release.countDown();
                    for (String child : children) {
                        assertThat(status(state, child)).isEqualTo(InstanceStatus.ABORTED);
                    }
                    assertThat(status(state, parent)).isEqualTo(InstanceStatus.ABORTED);
                } finally {
                    release.countDown();
                }
            }
        }
    }

    private static InstanceStatus status(StateStore state, String id) throws Exception {
        return state.load(id).toCompletableFuture().get(10, TimeUnit.SECONDS).map(InstanceSnapshot::status)
                .orElse(null);
    }

    private static String awaitTaskWait(StateStore state, String child) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < until) {
            String key = state.load(child).toCompletableFuture().get(10, TimeUnit.SECONDS)
                    .map(InstanceSnapshot::waitingKey).orElse(null);
            if (key != null && key.startsWith("task:")) {
                return key;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("child " + child + " never waited on its task");
    }

    private static void awaitStatus(StateStore state, String id, InstanceStatus expected) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < until && status(state, id) != expected) {
            Thread.onSpinWait();
        }
        assertThat(status(state, id)).isEqualTo(expected);
    }
}
