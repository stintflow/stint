package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import io.stintflow.aws.DynamoDbStateStore;
import io.stintflow.core.CancelOutcome;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryAuditLog;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.spi.AdapterCapabilities;
import io.stintflow.spi.AdapterCapabilities.DeliveryGuarantee;
import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.TaskResultHandler;
import io.stintflow.spi.TaskTransport;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * SDD 3a.11, CA10, on real floci: instances suspended on each kind of wait — a task in flight, a retry delay, a
 * {@code listen} with and without a timeout — are cancelled over {@link DynamoDbStateStore}, and the waits table is left
 * with none of their keys. No sleeps.
 */
class FlociCancelIT {

    private static final WorkflowRef REMOTE = new WorkflowRef("meetings", "cancel-remote", "1.0.0");
    private static final WorkflowRef RETRY = new WorkflowRef("meetings", "cancel-retry", "1.0.0");
    private static final WorkflowRef LISTEN = new WorkflowRef("meetings", "cancel-listen", "1.0.0");
    private static final WorkflowRef LISTEN_TIMEOUT = new WorkflowRef("meetings", "cancel-listen-timeout", "1.0.0");

    /** A transport that keeps every invoke and answers only when a test says so. */
    private static final class Holding implements TaskTransport {
        final List<TaskInvocation> dispatched = new CopyOnWriteArrayList<>();
        volatile TaskResultHandler results;

        @Override
        public CompletionStage<Void> dispatch(TaskInvocation invocation) {
            dispatched.add(invocation);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void onResult(TaskResultHandler handler) {
            results = handler;
        }

        @Override
        public AdapterCapabilities capabilities() {
            return new AdapterCapabilities(DeliveryGuarantee.AT_LEAST_ONCE, false, 256 * 1024, null, false, false);
        }
    }

    private static String named(String yaml, String name) {
        return yaml.replace("name: cancel", "name: " + name);
    }

    @Test
    void every_kind_of_wait_is_cancelled_over_dynamodb_and_leaves_no_wait_behind() throws Exception {
        try (GenericContainer<?> floci = new GenericContainer<>(DockerImageName.parse("floci/floci:latest"))
                .withExposedPorts(4566)) {
            floci.start();
            URI endpoint = URI.create("http://" + floci.getHost() + ":" + floci.getMappedPort(4566));
            var creds = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));
            try (DynamoDbClient ddb = DynamoDbClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                    .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build()) {
                DynamoDbTestTables.create(ddb, "stint-instances", "stint-waits");
                DynamoDbTestTables.createInbox(ddb, "stint-inbox");
                StateStore state = new DynamoDbStateStore(ddb, "stint-instances", "stint-waits", "stint-outbox", 4,
                        "stint-inbox");

                WorkflowRegistry registry = new WorkflowRegistry();
                registry.register(PolicyFixture.load(named(CancelInstanceTest.REMOTE, "cancel-remote")));
                registry.register(PolicyFixture.load(named(CancelInstanceTest.RETRYING, "cancel-retry")));
                registry.register(PolicyFixture.load(named(CancelInstanceTest.listening(false), "cancel-listen")));
                registry.register(PolicyFixture.load(named(CancelInstanceTest.listening(true),
                        "cancel-listen-timeout")));
                Holding transport = new Holding();
                InMemoryAuditLog audit = new InMemoryAuditLog();
                WorkflowEngine engine = new WorkflowEngine(registry, transport, state, new InMemoryTimerService(),
                        new FilesystemBlobStore(Files.createTempDirectory("stint-3a11-blobs"))).auditLog(audit);

                List<String> ids = new ArrayList<>();
                List<String> keys = new ArrayList<>();
                for (WorkflowRef ref : List.of(REMOTE, RETRY, LISTEN, LISTEN_TIMEOUT)) {
                    String id = engine.start(ref, Json.obj(), Sensitivity.INTERNAL);
                    ids.add(id);
                    if (ref.equals(RETRY)) {
                        awaitDispatched(transport, id);
                        TaskInvocation first = transport.dispatched.stream()
                                .filter(i -> i.workflowInstanceId().equals(id)).findFirst().orElseThrow();
                        transport.results.handle(TaskResult.failed(first.correlationId(),
                                new ErrorInfo(ErrorInfo.TYPE_COMMUNICATION, 503, "down", null, null, null)))
                                .toCompletableFuture().get(20, TimeUnit.SECONDS);
                        awaitKey(state, id, "retry:");
                    } else if (ref.equals(REMOTE)) {
                        awaitDispatched(transport, id);
                        awaitKey(state, id, "task:");
                    } else {
                        awaitKey(state, id, "event:");
                    }
                    keys.addAll(waitKeysOf(state, id));
                }
                assertThat(keys).hasSize(6);   // task+timer, retry, event, event+timer
                for (String key : keys) {
                    assertThat(state.findWait(key).toCompletableFuture().get(10, TimeUnit.SECONDS))
                            .as("wait %s before the cancel", key).isPresent();
                }

                for (String id : ids) {
                    assertThat(engine.cancel(id, "floci").toCompletableFuture().get(20, TimeUnit.SECONDS))
                            .isEqualTo(CancelOutcome.CANCELLED);
                    InstanceSnapshot after = state.load(id).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow();
                    assertThat(after.status()).isEqualTo(InstanceStatus.ABORTED);
                    assertThat(after.waitingKey()).isNull();
                    assertThat(engine.cancel(id, "again").toCompletableFuture().get(20, TimeUnit.SECONDS))
                            .isEqualTo(CancelOutcome.ALREADY_TERMINAL);
                }
                for (String key : keys) {
                    assertThat(state.findWait(key).toCompletableFuture().get(10, TimeUnit.SECONDS))
                            .as("wait %s after the cancel", key).isEmpty();
                }
            }
        }
    }

    private static void awaitDispatched(Holding transport, String id) {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (System.nanoTime() < until
                && transport.dispatched.stream().noneMatch(i -> i.workflowInstanceId().equals(id))) {
            Thread.onSpinWait();
        }
        assertThat(transport.dispatched).anyMatch(i -> i.workflowInstanceId().equals(id));
    }

    private static void awaitKey(StateStore state, String id, String prefix) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (System.nanoTime() < until) {
            String key = state.load(id).toCompletableFuture().get(10, TimeUnit.SECONDS)
                    .map(InstanceSnapshot::waitingKey).orElse(null);
            if (key != null && key.startsWith(prefix)) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("instance " + id + " never waited on " + prefix);
    }

    /** The wait keys the snapshot implies, read back from the table: the waiting key, its timer, the listen's keys. */
    private static List<String> waitKeysOf(StateStore state, String id) throws Exception {
        InstanceSnapshot snap = state.load(id).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow();
        List<String> keys = new ArrayList<>();
        var active = snap.listenState() == null ? null : snap.listenState().get("active");
        if (active != null) {
            active.get("pending").forEach(k -> keys.add(k.asText()));
            if (active.hasNonNull("timerKey")) {
                keys.add(active.get("timerKey").asText());
            }
            return keys;
        }
        keys.add(snap.waitingKey());
        if (snap.waitingKey().startsWith("task:")) {
            keys.add("timer:" + snap.waitingKey().substring("task:".length()));
        }
        return keys;
    }
}
