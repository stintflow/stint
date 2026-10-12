package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

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
import io.stintflow.inmemory.InMemoryTaskTransport;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.InboxEntry;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.OutboxEntry;
import io.stintflow.spi.SaveOutcome;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.TimerFireHandler;
import io.stintflow.spi.TimerRequest;
import io.stintflow.spi.TimerService;
import io.stintflow.spi.Wait;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerRuntime;

/**
 * SDD 2.9, RF4/CA5 (S.2 finding 8h): what a {@code call: remote} sends its worker — inside a {@code try} too, with
 * {@code input.from} and {@code with.input} together, and again on every retry.
 */
class TryInputTest {

    private static final Instant T0 = Instant.parse("2026-10-11T12:00:00Z");

    @Test
    void a_remote_call_inside_try_receives_its_declared_with_input() throws Exception {
        Fixture f = new Fixture("""
                do:
                  - order:
                      set: '${ {orderId: "o-1", amount: 10} }'
                      export:
                        as: '${ . }'
                  - render:
                      try:
                        - work:
                            call: remote
                            with:
                              task: echo
                              input:
                                request: '${ $context }'
                      catch:
                        errors:
                          with:
                            type: https://serverlessworkflow.io/spec/1.0.0/errors/communication
                """);

        f.run();

        assertThat(f.inputs).singleElement().satisfies(input ->
                assertThat(input).isEqualTo(Json.read("{\"request\":{\"orderId\":\"o-1\",\"amount\":10}}".getBytes())));
    }

    @Test
    void with_input_is_evaluated_over_the_result_of_input_from() throws Exception {
        Fixture f = new Fixture("""
                do:
                  - order:
                      set: '${ {orderId: "o-2", amount: 20} }'
                  - work:
                      input:
                        from: '${ {id: .orderId} }'
                      call: remote
                      with:
                        task: echo
                        input:
                          argument: '${ .id }'
                          total: '${ $workflow.input.extra }'
                """);

        f.run();

        // input.from turned the task input into {id}, and with.input was evaluated over it (before 2.9 it was ignored).
        assertThat(f.inputs).singleElement().satisfies(input ->
                assertThat(input).isEqualTo(Json.read("{\"argument\":\"o-2\",\"total\":7}".getBytes())));
    }

    @Test
    void every_retry_dispatches_the_same_input_as_the_first_attempt() throws Exception {
        Fixture f = new Fixture("""
                do:
                  - stage:
                      set: '${ {from: "previous task output"} }'
                  - work:
                      try:
                        - flaky:
                            call: remote
                            with:
                              task: flaky
                      catch:
                        errors:
                          with:
                            type: https://serverlessworkflow.io/spec/1.0.0/errors/communication
                        retry:
                          delay: { seconds: 2 }
                          limit:
                            attempt: { count: 3 }
                """);
        f.failFirstAttempts = 2;

        String id = f.start();
        f.fireRetries(2);
        f.engine.awaitCompletion(id, Duration.ofSeconds(10)).toCompletableFuture().get(15, TimeUnit.SECONDS);

        // Attempt 1 got the previous task's output; before 2.9 the retries got $context (here: {extra: 7}).
        assertThat(f.inputs).hasSize(3).containsOnly(Json.read("{\"from\":\"previous task output\"}".getBytes()));
    }

    static final class Fixture {
        final AtomicReference<Instant> now = new AtomicReference<>(T0);
        final InMemoryTimerService timers = new InMemoryTimerService(now::get);
        final List<JsonNode> inputs = new CopyOnWriteArrayList<>();
        final List<TimerRequest> armed = new CopyOnWriteArrayList<>();
        final RetryWaits state = new RetryWaits(new InMemoryStateStore());
        final WorkflowDefinition def;
        final WorkflowEngine engine;
        volatile int failFirstAttempts;

        Fixture(String tasks) throws Exception {
            String yaml = """
                    document:
                      dsl: '1.0.0'
                      namespace: input
                      name: try-input
                      version: '1.0.0'
                    """ + tasks;
            def = new DslLoader().load(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)), "try-input.yaml",
                    LoadOptions.STRICT);
            WorkflowRegistry registry = new WorkflowRegistry();
            registry.register(def);
            BlobStore blobs = new FilesystemBlobStore(Files.createTempDirectory("stint-sdd29-input"));
            WorkerRuntime worker = new WorkerRuntime(new TaskHandlerRegistry()
                    .register("echo", ctx -> {
                        inputs.add(ctx.input());
                        return CompletableFuture.completedFuture(ctx.input());
                    })
                    .register("flaky", ctx -> {
                        inputs.add(ctx.input());
                        return ctx.attempt() <= failFirstAttempts
                                ? CompletableFuture.failedFuture(new TaskFailure(ErrorInfo.TYPE_COMMUNICATION, 503, "down", null))
                                : CompletableFuture.completedFuture(Json.obj().put("ok", true));
                    }), blobs);
            InMemoryTaskTransport transport = new InMemoryTaskTransport();
            transport.connectWorker(worker::handle);
            TimerService recording = new TimerService() {
                @Override
                public CompletionStage<String> schedule(TimerRequest req) {
                    armed.add(req);
                    return timers.schedule(req);
                }

                @Override
                public CompletionStage<Void> cancel(String timerId) {
                    return timers.cancel(timerId);
                }

                @Override
                public void onFire(TimerFireHandler handler) {
                    timers.onFire(handler);
                }

                @Override
                public Duration maxDelay() {
                    return timers.maxDelay();
                }
            };
            engine = new WorkflowEngine(registry, transport, state, recording, blobs,
                    new TreeInterpreter(new JqExpressionEvaluator()), now::get, null, null);
        }

        String start() {
            return engine.start(def.ref(), Json.obj().put("extra", 7));
        }

        void run() throws Exception {
            String id = start();
            engine.awaitCompletion(id, Duration.ofSeconds(10)).toCompletableFuture().get(15, TimeUnit.SECONDS);
        }

        /** Each retry: wait until its wait is saved, then move the clock to its fireAt and tick (no sleeps). */
        void fireRetries(int count) throws Exception {
            for (int i = 0; i < count; i++) {
                InstanceSnapshot waiting = state.retryWaits.poll(10, TimeUnit.SECONDS);
                assertThat(waiting).as("retry %d was scheduled", i + 1).isNotNull();
                TimerRequest retry = armed.stream().filter(t -> t.timerId().equals(waiting.waitingKey())).findFirst()
                        .orElseThrow();
                now.set(retry.fireAt());
                timers.tick();
            }
        }
    }

    /** Reports each snapshot saved waiting on a retry delay, after the save. */
    static final class RetryWaits implements StateStore {
        final StateStore delegate;
        final BlockingQueue<InstanceSnapshot> retryWaits = new LinkedBlockingQueue<>();

        RetryWaits(StateStore delegate) {
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
                        if (outcome == SaveOutcome.OK && snapshot.waitingKey() != null
                                && snapshot.waitingKey().startsWith("retry:")) {
                            retryWaits.add(snapshot);
                        }
                        return outcome;
                    });
        }

        @Override
        public CompletionStage<Optional<InstanceSnapshot>> load(String instanceId) {
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
    }
}
