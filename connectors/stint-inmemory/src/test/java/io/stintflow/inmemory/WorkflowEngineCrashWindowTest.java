package io.stintflow.inmemory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.model.FlowDirective;
import io.stintflow.core.model.RetryPolicy;
import io.stintflow.spi.AdapterCapabilities;
import io.stintflow.spi.AdapterCapabilities.DeliveryGuarantee;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.OutboxEntry;
import io.stintflow.spi.SaveOutcome;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResultHandler;
import io.stintflow.spi.TaskTransport;
import io.stintflow.spi.Wait;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;

/**
 * SDD 2.1, CA8 (RF8/sec. 8g): the process "dies" right after the save that registers a task wait —
 * before anything else runs (simulated by a StateStore that persists the write and then fails, so
 * neither a timer nor a dispatch can follow it). Because the timer is armed before that save, the
 * timeout still fires and the instance follows its SDD 1.3 policy — FAILED outside a
 * {@code try}, a retry inside one — instead of staying WAITING forever. Mutable clock + {@code tick()},
 * never a sleep.
 */
class WorkflowEngineCrashWindowTest {

    private static final Duration TASK_TIMEOUT = Duration.ofMinutes(1);

    @Test
    void ca8_crash_right_after_the_save_times_out_to_failed_instead_of_waiting_forever() throws Exception {
        MutableClock clock = new MutableClock();
        WorkflowRef ref = new WorkflowRef("test", "crash-window", "1.0.0");
        WorkflowDefinition def = WorkflowBuilder.create()
                .callRemote("echo", "route-echo", DataFlow.NONE, FlowDirective.CONTINUE, TASK_TIMEOUT)
                .build(ref);
        WorkflowRegistry registry = new WorkflowRegistry();
        registry.register(def);
        CrashAfterSaveStore state = new CrashAfterSaveStore(new InMemoryStateStore(), 1);
        InMemoryTimerService timer = new InMemoryTimerService(clock);
        RecordingTransport transport = new RecordingTransport();
        WorkflowEngine engine = newEngine(registry, transport, state, timer, clock);

        String instanceId = engine.start(ref, Json.obj());
        state.awaitCrash();

        InstanceSnapshot waiting = load(state, instanceId);
        assertThat(waiting.status()).isEqualTo(InstanceStatus.WAITING); // the save itself is durable
        assertThat(transport.dispatched()).isZero(); // ...but nothing after it ever ran

        clock.advance(TASK_TIMEOUT.plusSeconds(1));
        timer.tick();

        assertThatThrownBy(() -> engine.awaitCompletion(instanceId, Duration.ofSeconds(5))
                .toCompletableFuture().get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("failed");
        assertThat(load(state, instanceId).status()).isEqualTo(InstanceStatus.FAILED);
    }

    @Test
    void ca8_crash_right_after_the_save_inside_a_try_is_retried_after_the_timeout() throws Exception {
        MutableClock clock = new MutableClock();
        RetryPolicy retry = new RetryPolicy(Duration.ofMillis(50), RetryPolicy.Backoff.CONSTANT, 3, null, 0.0);
        WorkflowRef ref = new WorkflowRef("test", "crash-window-try", "1.0.0");
        WorkflowDefinition def = WorkflowBuilder.create()
                .tryRemote("attempt", DataFlow.NONE, "call", "route-a", DataFlow.NONE, TASK_TIMEOUT,
                        WorkflowBuilder.catchAnyWithRetry(retry, null, FlowDirective.CONTINUE), FlowDirective.END)
                .build(ref);
        WorkflowRegistry registry = new WorkflowRegistry();
        registry.register(def);
        CrashAfterSaveStore state = new CrashAfterSaveStore(new InMemoryStateStore(), 1);
        InMemoryTimerService timer = new InMemoryTimerService(clock);
        RecordingTransport transport = new RecordingTransport();
        WorkflowEngine engine = newEngine(registry, transport, state, timer, clock);

        String instanceId = engine.start(ref, Json.obj());
        state.awaitCrash();
        assertThat(transport.dispatched()).isZero();

        clock.advance(TASK_TIMEOUT.plusSeconds(1));
        timer.tick(); // attempt 1 times out -> retry delay armed
        clock.advance(Duration.ofMillis(60));
        timer.tick(); // retry delay elapses -> attempt 2 dispatched

        TaskInvocation second = transport.awaitDispatch();
        assertThat(second.attempt()).isEqualTo(2);
        assertThat(load(state, instanceId).status()).isEqualTo(InstanceStatus.WAITING);
    }

    private static WorkflowEngine newEngine(WorkflowRegistry registry, TaskTransport transport, StateStore state,
            InMemoryTimerService timer, InstantSource clock) throws Exception {
        return new WorkflowEngine(registry, transport, state, timer,
                new FilesystemBlobStore(Files.createTempDirectory("stint-ca8-blobs")),
                new TreeInterpreter(new JqExpressionEvaluator()), clock);
    }

    private static InstanceSnapshot load(StateStore state, String instanceId) throws Exception {
        return state.load(instanceId).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
    }

    /**
     * Persists the first {@code crashes} successful saves and then fails them — the write is durable,
     * but the caller sees the process "die" right after it, so nothing chained after the save runs.
     */
    private static final class CrashAfterSaveStore implements StateStore {
        private final StateStore delegate;
        private final AtomicInteger remainingCrashes;
        private final LinkedBlockingQueue<Boolean> crashSignals = new LinkedBlockingQueue<>();

        CrashAfterSaveStore(StateStore delegate, int crashes) {
            this.delegate = delegate;
            this.remainingCrashes = new AtomicInteger(crashes);
        }

        @Override
        public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion,
                List<Wait> addWaits, List<String> consumeWaitKeys, List<OutboxEntry> addOutbox) {
            return delegate.save(snapshot, expectedVersion, addWaits, consumeWaitKeys, addOutbox).thenCompose(outcome -> {
                if (outcome == SaveOutcome.OK && remainingCrashes.getAndDecrement() > 0) {
                    crashSignals.add(Boolean.TRUE);
                    return CompletableFuture.failedFuture(new IllegalStateException("simulated crash after save"));
                }
                return CompletableFuture.completedFuture(outcome);
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

        void awaitCrash() throws InterruptedException, TimeoutException {
            if (crashSignals.poll(5, TimeUnit.SECONDS) == null) {
                throw new TimeoutException("No crash-after-save observed");
            }
        }
    }

    private static final class RecordingTransport implements TaskTransport {
        private final LinkedBlockingQueue<TaskInvocation> dispatched = new LinkedBlockingQueue<>();

        @Override
        public CompletionStage<Void> dispatch(TaskInvocation invocation) {
            dispatched.add(invocation);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void onResult(TaskResultHandler handler) {
        }

        @Override
        public AdapterCapabilities capabilities() {
            return new AdapterCapabilities(DeliveryGuarantee.AT_LEAST_ONCE, true, Long.MAX_VALUE, null, true, true);
        }

        TaskInvocation awaitDispatch() throws InterruptedException, TimeoutException {
            TaskInvocation inv = dispatched.poll(5, TimeUnit.SECONDS);
            if (inv == null) {
                throw new TimeoutException("No dispatch observed");
            }
            return inv;
        }

        int dispatched() {
            return dispatched.size();
        }
    }

    private static final class MutableClock implements InstantSource {
        private final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-01-01T00:00:00Z"));

        @Override
        public Instant instant() {
            return now.get();
        }

        void advance(Duration duration) {
            now.updateAndGet(i -> i.plus(duration));
        }
    }
}
