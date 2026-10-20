package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.dsl.DslLoader;
import io.stintflow.dsl.LoadOptions;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryStateStore;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.spi.AdapterCapabilities;
import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.InboxEntry;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.OutboxEntry;
import io.stintflow.spi.SaveOutcome;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.TaskResultHandler;
import io.stintflow.spi.TaskTransport;
import io.stintflow.spi.Wait;
import io.stintflow.wire.Json;

/**
 * SDD 3a.8: an engine whose tasks wait for a test to answer them, over a state store that remembers every outbox entry it
 * was given and can be made to fail loads of some instances. Definitions come from YAML; time moves only when told.
 */
final class FanOutFixture {

    static final Instant T0 = Instant.parse("2026-10-19T12:00:00Z");

    final AtomicReference<Instant> now = new AtomicReference<>(T0);
    final Store store = new Store(new InMemoryStateStore());
    final InMemoryTimerService timer = new InMemoryTimerService(now::get);
    final List<TaskInvocation> dispatched = new CopyOnWriteArrayList<>();
    final List<TaskInvocation> answered = new CopyOnWriteArrayList<>();
    final WorkflowRegistry registry = new WorkflowRegistry();
    final WorkflowEngine engine;
    private volatile TaskResultHandler results;

    FanOutFixture(String... yamls) throws Exception {
        for (String yaml : yamls) {
            registry.register(load(yaml));
        }
        engine = new WorkflowEngine(registry, new Transport(), store, timer,
                new FilesystemBlobStore(Files.createTempDirectory("stint-3a8-blobs")),
                new TreeInterpreter(new JqExpressionEvaluator()), now::get, null, null);
    }

    static WorkflowDefinition load(String yaml) {
        return new DslLoader().load(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)), "fan-out.yaml",
                LoadOptions.STRICT);
    }

    /** A child that runs one remote task {@code work} and ends with that task's output as its own. */
    static String leaf(String name) {
        return """
                document: { dsl: '1.0.0', namespace: fan, name: %s, version: '1.0.0' }
                do:
                  - work:
                      call: remote
                      with: { task: work, input: '${ . }' }
                      export: { as: '${ . }' }
                """.formatted(name);
    }

    /** A parent that runs {@code child} for each item of {@code .items}, its output under {@code results}. */
    static String parent(String name, String child, int concurrency, String join, int maxIterations) {
        return """
                document: { dsl: '1.0.0', namespace: fan, name: %s, version: '1.0.0' }
                do:
                  - fan:
                      for:
                        each: n
                        in: '${ .items }'
                        at: i
                      do:
                        - run:
                            run:
                              workflow:
                                namespace: fan
                                name: %s
                                version: '1.0.0'
                                input: '${ {n: $n, i: $i} }'
                      metadata:
                        x-stint-concurrency: %d
                        x-stint-join: %s
                        x-stint-max-iterations: %d
                      export: { as: '${ {results: .} }' }
                """.formatted(name, child, concurrency, join, maxIterations);
    }

    static JsonNode items(int count) {
        var array = Json.MAPPER.createArrayNode();
        for (int i = 0; i < count; i++) {
            array.add(i);
        }
        return Json.obj().set("items", array);
    }

    String childId(String parentId, int index) {
        return parentId + ":/do/0/fan:" + index;
    }

    // --- answering the tasks -------------------------------------------------------------------------------------

    /** The invocations sent for {@code instanceId}, oldest first. */
    List<TaskInvocation> invocationsOf(String instanceId) {
        return dispatched.stream().filter(i -> i.workflowInstanceId().equals(instanceId)).toList();
    }

    TaskInvocation awaitInvocation(String instanceId) {
        awaitUntil(() -> !invocationsOf(instanceId).isEmpty(), "an invocation for " + instanceId);
        return invocationsOf(instanceId).get(0);
    }

    /** Completes the task of {@code instanceId} with {@code {n: <its n> * 10}}. */
    void complete(String instanceId) {
        TaskInvocation inv = awaitInvocation(instanceId);
        answered.add(inv);
        results.handle(TaskResult.completed(inv.correlationId(),
                Json.obj().put("n", inv.input().path("n").asInt() * 10))).toCompletableFuture().join();
    }

    void fail(String instanceId) {
        TaskInvocation inv = awaitInvocation(instanceId);
        answered.add(inv);
        results.handle(TaskResult.failed(inv.correlationId(),
                new ErrorInfo(ErrorInfo.TYPE_RUNTIME, 500, "boom", null, null, null))).toCompletableFuture().join();
    }

    // --- looking at the instances --------------------------------------------------------------------------------

    InstanceSnapshot snapshot(String id) {
        try {
            return store.load(id).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow();
        } catch (Exception e) {
            throw new AssertionError("no instance " + id, e);
        }
    }

    Optional<InstanceSnapshot> find(String id) throws Exception {
        return store.load(id).toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    InstanceStatus awaitTerminal(String id) {
        awaitUntil(() -> {
            try {
                return find(id).map(s -> s.status() != InstanceStatus.WAITING && s.status() != InstanceStatus.RUNNING)
                        .orElse(false);
            } catch (Exception e) {
                return false;
            }
        }, "instance " + id + " to end");
        return snapshot(id).status();
    }

    JsonNode awaitCompleted(String id) {
        assertThat(awaitTerminal(id)).isEqualTo(InstanceStatus.COMPLETED);
        return snapshot(id).context();
    }

    static void awaitUntil(java.util.function.BooleanSupplier condition, String what) {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > until) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.onSpinWait();
        }
    }

    Duration sinceStart() {
        return Duration.between(T0, now.get());
    }

    // --- the store: remembers outbox entries, can fail loads -----------------------------------------------------

    static final class Store implements StateStore {
        final StateStore delegate;
        /** Every outbox entry any save was given, in order. */
        final List<OutboxEntry> outbox = new CopyOnWriteArrayList<>();
        /** A load of an instance this accepts fails (simulating an outage). */
        volatile Predicate<String> failLoadsOf = id -> false;

        Store(StateStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion, List<Wait> addWaits,
                List<String> consumeWaitKeys, List<OutboxEntry> addOutbox) {
            return save(snapshot, expectedVersion, addWaits, consumeWaitKeys, addOutbox, List.of());
        }

        @Override
        public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion, List<Wait> addWaits,
                List<String> consumeWaitKeys, List<OutboxEntry> addOutbox, List<InboxEntry.Key> removeInbox) {
            return delegate.save(snapshot, expectedVersion, addWaits, consumeWaitKeys, addOutbox, removeInbox)
                    .thenApply(outcome -> {
                        if (outcome == SaveOutcome.OK) {
                            outbox.addAll(addOutbox);
                        }
                        return outcome;
                    });
        }

        @Override
        public CompletionStage<Optional<InstanceSnapshot>> load(String instanceId) {
            if (failLoadsOf.test(instanceId)) {
                return CompletableFuture.failedFuture(new IllegalStateException("simulated outage loading " + instanceId));
            }
            return delegate.load(instanceId);
        }

        @Override
        public CompletionStage<Optional<Wait>> findWait(String waitKey) {
            return delegate.findWait(waitKey);
        }

        @Override
        public CompletionStage<Void> delete(String instanceId) {
            return delegate.delete(instanceId);
        }

        @Override
        public CompletionStage<List<OutboxEntry>> pendingOutbox(Instant createdAtOrBefore, int limit) {
            return delegate.pendingOutbox(createdAtOrBefore, limit);
        }

        @Override
        public CompletionStage<Void> removeOutbox(String eventId) {
            return delegate.removeOutbox(eventId);
        }

        @Override
        public CompletionStage<Void> putInbox(InboxEntry entry) {
            return delegate.putInbox(entry);
        }

        @Override
        public CompletionStage<List<InboxEntry>> findInbox(String waitKey) {
            return delegate.findInbox(waitKey);
        }

        @Override
        public CompletionStage<Void> removeInbox(InboxEntry.Key key) {
            return delegate.removeInbox(key);
        }
    }

    private final class Transport implements TaskTransport {
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
            return new AdapterCapabilities(AdapterCapabilities.DeliveryGuarantee.AT_LEAST_ONCE, false, 256 * 1024, null,
                    false, false);
        }
    }
}
