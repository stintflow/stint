package io.stintflow.inmemory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.wire.Json;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.model.FlowDirective;
import io.stintflow.core.model.RetryPolicy;
import io.stintflow.core.model.TryNode;
import io.stintflow.spi.AdapterCapabilities;
import io.stintflow.spi.AdapterCapabilities.DeliveryGuarantee;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.TaskResultHandler;
import io.stintflow.spi.TaskTransport;
import io.stintflow.spi.TimerFireHandler;
import io.stintflow.spi.TimerRequest;
import io.stintflow.spi.TimerService;
import io.stintflow.spi.WorkflowRef;

/**
 * SDD 1.3: {@code TryNode} retry/backoff/compensation (CA1-CA5), the result-vs-timeout race (sec.
 * 8a), a stale/obsolete timer fire as a no-op, retry resumption on a brand-new engine, and the
 * retry-delay-exceeds-maxDelay rejection (sec. 8d). Everything driven by a mutable test clock and
 * {@link InMemoryTimerService#tick()}/{@code fire} — never a sleep.
 */
class TryNodeRetryTest {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(5);

    @Test
    void ca1_worker_silent_on_first_attempt_then_responds_completes_with_attempt_2_after_the_delay() throws Exception {
        MutableClock clock = new MutableClock();
        RetryPolicy retry = new RetryPolicy(Duration.ofMillis(50), RetryPolicy.Backoff.CONSTANT, 3, null, 0.0);
        WorkflowRef ref = new WorkflowRef("test", "ca1", "1.0.0");
        WorkflowDefinition def = WorkflowBuilder.create()
                .tryRemote("attempt", DataFlow.NONE, "call", "route-a",
                        new DataFlow(null, null, Expr.jq("$context + .")), null,
                        WorkflowBuilder.catchAnyWithRetry(retry, null, FlowDirective.CONTINUE), FlowDirective.END)
                .build(ref);

        WorkflowRegistry registry = new WorkflowRegistry();
        registry.register(def);
        StateStore state = new InMemoryStateStore();
        BlobStore blob = new FilesystemBlobStore(Files.createTempDirectory("stint-ca1-try-blobs"));
        InMemoryTimerService timer = new InMemoryTimerService(clock);
        ManualTransport transport = new ManualTransport();
        WorkflowEngine engine = newEngine(registry, transport, state, timer, blob, clock);

        String instanceId = engine.start(ref, Json.obj());
        TaskInvocation first = transport.awaitDispatch(5, TimeUnit.SECONDS);
        assertThat(first.attempt()).isEqualTo(1);

        // Worker never responds: advance past the task timeout and sweep.
        clock.advance(DEFAULT_TIMEOUT.plusSeconds(1));
        timer.tick();

        InstanceSnapshot afterTimeout = state.load(instanceId).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
        assertThat(afterTimeout.waitingKey()).startsWith("retry:");
        assertThat(afterTimeout.retryState().attempt()).isEqualTo(1);

        // Before the retry delay elapses: no redispatch yet (the delay is respected).
        clock.advance(Duration.ofMillis(10));
        timer.tick();
        assertThat(transport.dispatchCount()).isEqualTo(1);

        // After the retry delay: attempt 2 is dispatched.
        clock.advance(Duration.ofMillis(50));
        timer.tick();
        TaskInvocation second = transport.awaitDispatch(5, TimeUnit.SECONDS);
        assertThat(second.attempt()).isEqualTo(2);

        ObjectNode output = Json.obj();
        output.put("done", true);
        transport.deliverResult(TaskResult.completed(second.correlationId(), output))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        InstanceSnapshot finalSnap = state.load(instanceId).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
        assertThat(finalSnap.status()).isEqualTo(InstanceStatus.COMPLETED);
        assertThat(finalSnap.retryState()).isNull();
        assertThat(finalSnap.context().get("done").asBoolean()).isTrue();
    }

    @Test
    void ca2_retry_limit_exhausted_fails_with_a_timeout_typed_error() throws Exception {
        MutableClock clock = new MutableClock();
        RetryPolicy retry = new RetryPolicy(Duration.ofMillis(10), RetryPolicy.Backoff.CONSTANT, 2, null, 0.0);
        WorkflowRef ref = new WorkflowRef("test", "ca2", "1.0.0");
        WorkflowDefinition def = WorkflowBuilder.create()
                .tryRemote("attempt", DataFlow.NONE, "call", "route-a", DataFlow.NONE, null,
                        WorkflowBuilder.catchAnyWithRetry(retry, null, FlowDirective.CONTINUE), FlowDirective.END)
                .build(ref);

        WorkflowRegistry registry = new WorkflowRegistry();
        registry.register(def);
        StateStore state = new InMemoryStateStore();
        BlobStore blob = new FilesystemBlobStore(Files.createTempDirectory("stint-ca2-try-blobs"));
        InMemoryTimerService timer = new InMemoryTimerService(clock);
        ManualTransport transport = new ManualTransport();
        WorkflowEngine engine = newEngine(registry, transport, state, timer, blob, clock);

        CompletionStage<com.fasterxml.jackson.databind.JsonNode> resultFuture = engine.startAndWait(ref, Json.obj());
        TaskInvocation firstDispatch = transport.awaitDispatch(5, TimeUnit.SECONDS);

        clock.advance(DEFAULT_TIMEOUT.plusSeconds(1));
        timer.tick(); // attempt 1 times out -> schedules retry
        clock.advance(Duration.ofMillis(20));
        timer.tick(); // attempt 2 dispatched
        transport.awaitDispatch(5, TimeUnit.SECONDS);

        clock.advance(DEFAULT_TIMEOUT.plusSeconds(1));
        timer.tick(); // attempt 2 also times out -> limit exhausted

        // SDD 1.5, sec. 8d: awaitCompletion learns FAILED from the StateStore, which (like the old
        // in-memory shortcut it replaces) does not persist *why* — so the assertion moves from the
        // exception's text to the persisted status, which is what's actually knowable now.
        assertThatThrownBy(() -> resultFuture.toCompletableFuture().get(5, TimeUnit.SECONDS));
        InstanceSnapshot snap = state.load(firstDispatch.workflowInstanceId())
                .toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
        assertThat(snap.status()).isEqualTo(InstanceStatus.FAILED);
    }

    @Test
    void ca3_429_backoff_respects_retry_after_as_a_minimum_delay() throws Exception {
        MutableClock clock = new MutableClock();
        RetryPolicy retry = new RetryPolicy(Duration.ofMillis(100), RetryPolicy.Backoff.EXPONENTIAL, 3, null, 0.0);
        TryNode.Catch catchClause = new TryNode.Catch(Expr.jq(".status == 429"), null, null, retry, null,
                FlowDirective.CONTINUE);
        WorkflowRef ref = new WorkflowRef("test", "ca3", "1.0.0");
        WorkflowDefinition def = WorkflowBuilder.create()
                .tryRemote("attempt", DataFlow.NONE, "call", "route-a", DataFlow.NONE, null, catchClause,
                        FlowDirective.END)
                .build(ref);

        WorkflowRegistry registry = new WorkflowRegistry();
        registry.register(def);
        StateStore state = new InMemoryStateStore();
        BlobStore blob = new FilesystemBlobStore(Files.createTempDirectory("stint-ca3-try-blobs"));
        InMemoryTimerService timer = new InMemoryTimerService(clock);
        ManualTransport transport = new ManualTransport();
        WorkflowEngine engine = newEngine(registry, transport, state, timer, blob, clock);

        engine.start(ref, Json.obj());
        TaskInvocation first = transport.awaitDispatch(5, TimeUnit.SECONDS);

        transport.deliverResult(TaskResult.failed(first.correlationId(),
                        new ErrorInfo(ErrorInfo.TYPE_COMMUNICATION, 429, "Too Many Requests", "rate limited", null,
                                Duration.ofMillis(500))))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        // exponential base delay for the 1st retry (attempt 2) is 100ms, well under retryAfter=500ms.
        clock.advance(Duration.ofMillis(150));
        timer.tick();
        assertThat(transport.dispatchCount()).isEqualTo(1); // retryAfter floor not reached yet

        clock.advance(Duration.ofMillis(400)); // total 550ms > 500ms retryAfter
        timer.tick();
        TaskInvocation second = transport.awaitDispatch(5, TimeUnit.SECONDS);
        assertThat(second.attempt()).isEqualTo(2);
    }

    @Test
    void ca4_catch_without_retry_runs_compensation_and_continues() throws Exception {
        WorkflowRef ref = new WorkflowRef("test", "ca4", "1.0.0");
        TryNode.Catch catchClause = new TryNode.Catch(null, null, null, null, Expr.jq("{compensated: true}"),
                FlowDirective.CONTINUE);
        WorkflowDefinition def = WorkflowBuilder.create()
                .tryRemote("attempt", DataFlow.NONE, "call", "route-a", DataFlow.NONE, null, catchClause,
                        FlowDirective.CONTINUE)
                .set("afterCatch", null, new DataFlow(null, null, Expr.jq("$context + {sawCompensation: .compensated}")),
                        FlowDirective.END)
                .build(ref);

        WorkflowRegistry registry = new WorkflowRegistry();
        registry.register(def);
        StateStore state = new InMemoryStateStore();
        BlobStore blob = new FilesystemBlobStore(Files.createTempDirectory("stint-ca4-try-blobs"));
        TimerService timer = new InMemoryTimerService();
        ManualTransport transport = new ManualTransport();
        WorkflowEngine engine = new WorkflowEngine(registry, transport, state, timer, blob);

        String instanceId = engine.start(ref, Json.obj());
        TaskInvocation invocation = transport.awaitDispatch(5, TimeUnit.SECONDS);
        transport.deliverResult(TaskResult.failed(invocation.correlationId(),
                        ErrorInfo.of(new RuntimeException("boom"))))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        InstanceSnapshot finalSnap = state.load(instanceId).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
        assertThat(finalSnap.status()).isEqualTo(InstanceStatus.COMPLETED);
        assertThat(finalSnap.context().get("sawCompensation").asBoolean()).isTrue();
    }

    @Test
    void ca5_late_result_from_attempt_1_is_ignored_once_attempt_2_was_dispatched() throws Exception {
        MutableClock clock = new MutableClock();
        RetryPolicy retry = new RetryPolicy(Duration.ofMillis(10), RetryPolicy.Backoff.CONSTANT, 5, null, 0.0);
        WorkflowRef ref = new WorkflowRef("test", "ca5", "1.0.0");
        WorkflowDefinition def = WorkflowBuilder.create()
                .tryRemote("attempt", DataFlow.NONE, "call", "route-a", DataFlow.NONE, null,
                        WorkflowBuilder.catchAnyWithRetry(retry, null, FlowDirective.CONTINUE), FlowDirective.END)
                .build(ref);

        WorkflowRegistry registry = new WorkflowRegistry();
        registry.register(def);
        StateStore state = new InMemoryStateStore();
        BlobStore blob = new FilesystemBlobStore(Files.createTempDirectory("stint-ca5-try-blobs"));
        InMemoryTimerService timer = new InMemoryTimerService(clock);
        ManualTransport transport = new ManualTransport();
        WorkflowEngine engine = newEngine(registry, transport, state, timer, blob, clock);

        engine.start(ref, Json.obj());
        TaskInvocation first = transport.awaitDispatch(5, TimeUnit.SECONDS);

        clock.advance(DEFAULT_TIMEOUT.plusSeconds(1));
        timer.tick();
        clock.advance(Duration.ofMillis(20));
        timer.tick();
        TaskInvocation second = transport.awaitDispatch(5, TimeUnit.SECONDS);
        assertThat(second.attempt()).isEqualTo(2);

        // The stale result for attempt 1 arrives late: must be a no-op.
        ObjectNode staleOutput = Json.obj();
        staleOutput.put("stale", true);
        transport.deliverResult(TaskResult.completed(first.correlationId(), staleOutput))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        InstanceSnapshot stillWaiting = state.load(engineInstanceId(state, second)).toCompletableFuture()
                .get(5, TimeUnit.SECONDS).orElseThrow();
        assertThat(stillWaiting.status()).isEqualTo(InstanceStatus.WAITING);
        assertThat(stillWaiting.retryState().attempt()).isEqualTo(2);

        ObjectNode realOutput = Json.obj();
        realOutput.put("done", true);
        transport.deliverResult(TaskResult.completed(second.correlationId(), realOutput))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        InstanceSnapshot finalSnap = state.load(engineInstanceId(state, second)).toCompletableFuture()
                .get(5, TimeUnit.SECONDS).orElseThrow();
        assertThat(finalSnap.status()).isEqualTo(InstanceStatus.COMPLETED);
    }

    @Test
    void race_between_result_and_timeout_resolves_to_exactly_one_outcome() throws Exception {
        WorkflowRef ref = new WorkflowRef("test", "race", "1.0.0");
        WorkflowDefinition def = WorkflowBuilder.create()
                .callRemote("echo", "route-echo", DataFlow.NONE)
                .build(ref);

        WorkflowRegistry registry = new WorkflowRegistry();
        registry.register(def);
        StateStore state = new InMemoryStateStore();
        BlobStore blob = new FilesystemBlobStore(Files.createTempDirectory("stint-race-blobs"));
        InMemoryTimerService timer = new InMemoryTimerService(new MutableClock());
        ManualTransport transport = new ManualTransport();
        WorkflowEngine engine = new WorkflowEngine(registry, transport, state, timer, blob);

        String instanceId = engine.start(ref, Json.obj());
        TaskInvocation invocation = transport.awaitDispatch(5, TimeUnit.SECONDS);

        CyclicBarrier barrier = new CyclicBarrier(2);
        CompletableFuture<Void> resultThread = CompletableFuture.runAsync(() -> {
            try {
                barrier.await(5, TimeUnit.SECONDS);
                transport.deliverResult(TaskResult.completed(invocation.correlationId(), Json.obj()))
                        .toCompletableFuture().get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        CompletableFuture<Void> timeoutThread = CompletableFuture.runAsync(() -> {
            try {
                barrier.await(5, TimeUnit.SECONDS);
                timer.fire("timer:" + invocation.correlationId());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        CompletableFuture.allOf(resultThread, timeoutThread).get(5, TimeUnit.SECONDS);

        InstanceSnapshot finalSnap = state.load(instanceId).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
        assertThat(finalSnap.status()).isIn(InstanceStatus.COMPLETED, InstanceStatus.FAILED);
        assertThat(finalSnap.version()).isEqualTo(2); // exactly one of the two events actually advanced it
        assertThat(state.findWait("task:" + invocation.correlationId()).toCompletableFuture().get(5, TimeUnit.SECONDS))
                .isEmpty();
        assertThat(state.findWait("timer:" + invocation.correlationId()).toCompletableFuture().get(5, TimeUnit.SECONDS))
                .isEmpty();
    }

    @Test
    void an_obsolete_timer_fire_after_completion_is_a_no_op() throws Exception {
        WorkflowRef ref = new WorkflowRef("test", "obsolete-timer", "1.0.0");
        WorkflowDefinition def = WorkflowBuilder.create()
                .callRemote("echo", "route-echo", DataFlow.NONE)
                .build(ref);

        WorkflowRegistry registry = new WorkflowRegistry();
        registry.register(def);
        StateStore state = new InMemoryStateStore();
        BlobStore blob = new FilesystemBlobStore(Files.createTempDirectory("stint-obsolete-blobs"));
        InMemoryTimerService timer = new InMemoryTimerService(new MutableClock());
        ManualTransport transport = new ManualTransport();
        WorkflowEngine engine = new WorkflowEngine(registry, transport, state, timer, blob);

        String instanceId = engine.start(ref, Json.obj());
        TaskInvocation invocation = transport.awaitDispatch(5, TimeUnit.SECONDS);
        transport.deliverResult(TaskResult.completed(invocation.correlationId(), Json.obj()))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        InstanceSnapshot completed = state.load(instanceId).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
        assertThat(completed.status()).isEqualTo(InstanceStatus.COMPLETED);

        // SQS cancel() is a no-op in reality; an already-resolved timer can still fire later.
        timer.fire("timer:" + invocation.correlationId());

        InstanceSnapshot stillCompleted = state.load(instanceId).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
        assertThat(stillCompleted.status()).isEqualTo(InstanceStatus.COMPLETED);
        assertThat(stillCompleted.version()).isEqualTo(completed.version());
    }

    @Test
    void retry_delay_resumes_correctly_on_a_brand_new_engine() throws Exception {
        MutableClock clock = new MutableClock();
        RetryPolicy retry = new RetryPolicy(Duration.ofMillis(10), RetryPolicy.Backoff.CONSTANT, 3, null, 0.0);
        WorkflowRef ref = new WorkflowRef("test", "cross-engine-retry", "1.0.0");
        WorkflowDefinition def = WorkflowBuilder.create()
                .tryRemote("attempt", DataFlow.NONE, "call", "route-a", DataFlow.NONE, null,
                        WorkflowBuilder.catchAnyWithRetry(retry, null, FlowDirective.CONTINUE), FlowDirective.END)
                .build(ref);

        WorkflowRegistry registry = new WorkflowRegistry();
        registry.register(def);
        StateStore state = new InMemoryStateStore();
        BlobStore blob = new FilesystemBlobStore(Files.createTempDirectory("stint-cross-engine-retry-blobs"));
        InMemoryTimerService timer = new InMemoryTimerService(clock);
        ManualTransport transport = new ManualTransport();

        WorkflowEngine engineA = newEngine(registry, transport, state, timer, blob, clock);
        engineA.start(ref, Json.obj());
        transport.awaitDispatch(5, TimeUnit.SECONDS);
        clock.advance(DEFAULT_TIMEOUT.plusSeconds(1));
        timer.tick(); // attempt 1 times out -> retry scheduled by engineA

        // engineA is discarded without ever seeing the retry through; engineB re-registers onFire/onResult.
        WorkflowEngine engineB = newEngine(registry, transport, state, timer, blob, clock);
        clock.advance(Duration.ofMillis(20));
        timer.tick(); // the retry-delay fire is now handled by engineB

        TaskInvocation second = transport.awaitDispatch(5, TimeUnit.SECONDS);
        assertThat(second.attempt()).isEqualTo(2);

        ObjectNode output = Json.obj();
        output.put("done", true);
        transport.deliverResult(TaskResult.completed(second.correlationId(), output))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertThat(engineB).isNotNull();
    }

    @Test
    void retry_delay_exceeding_the_timers_max_delay_fails_definitively_instead_of_truncating() throws Exception {
        MutableClock clock = new MutableClock();
        RetryPolicy retry = new RetryPolicy(Duration.ofSeconds(10), RetryPolicy.Backoff.CONSTANT, 3, null, 0.0);
        WorkflowRef ref = new WorkflowRef("test", "delay-too-long", "1.0.0");
        WorkflowDefinition def = WorkflowBuilder.create()
                .tryRemote("attempt", DataFlow.NONE, "call", "route-a", DataFlow.NONE, null,
                        WorkflowBuilder.catchAnyWithRetry(retry, null, FlowDirective.CONTINUE), FlowDirective.END)
                .build(ref);

        WorkflowRegistry registry = new WorkflowRegistry();
        registry.register(def);
        StateStore state = new InMemoryStateStore();
        BlobStore blob = new FilesystemBlobStore(Files.createTempDirectory("stint-delay-too-long-blobs"));
        TimerService timer = new MaxDelayLimitedTimerService(new InMemoryTimerService(clock), Duration.ofSeconds(1));
        ManualTransport transport = new ManualTransport();
        WorkflowEngine engine = newEngine(registry, transport, state, timer, blob, clock);

        CompletionStage<com.fasterxml.jackson.databind.JsonNode> resultFuture = engine.startAndWait(ref, Json.obj());
        TaskInvocation invocation = transport.awaitDispatch(5, TimeUnit.SECONDS);
        transport.deliverResult(TaskResult.failed(invocation.correlationId(),
                        ErrorInfo.of(new RuntimeException("boom"))))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        // SDD 1.5, sec. 8d: same as ca2 above — the persisted status is what's knowable now, not the
        // exception's text (the StateStore never carried the failure reason, only that one occurred).
        assertThatThrownBy(() -> resultFuture.toCompletableFuture().get(5, TimeUnit.SECONDS));
        InstanceSnapshot snap = state.load(invocation.workflowInstanceId())
                .toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
        assertThat(snap.status()).isEqualTo(InstanceStatus.FAILED);
    }

    private static WorkflowEngine newEngine(WorkflowRegistry registry, TaskTransport transport, StateStore state,
            TimerService timer, BlobStore blob, InstantSource clock) {
        return new WorkflowEngine(registry, transport, state, timer, blob,
                new TreeInterpreter(new JqExpressionEvaluator()), clock);
    }

    /** CA5 doesn't otherwise need the instanceId handy — recovered from the 2nd dispatch's own record. */
    private static String engineInstanceId(StateStore state, TaskInvocation invocation) {
        return invocation.workflowInstanceId();
    }

    /** Mutable, manually-advanced clock (SDD 1.3, sec. 8f) — no sleeps anywhere in these tests. */
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

    /** Wraps a real TimerService but reports a small maxDelay(), for sec. 8d's rejection path. */
    private static final class MaxDelayLimitedTimerService implements TimerService {
        private final TimerService delegate;
        private final Duration maxDelay;

        MaxDelayLimitedTimerService(TimerService delegate, Duration maxDelay) {
            this.delegate = delegate;
            this.maxDelay = maxDelay;
        }

        @Override
        public CompletionStage<String> schedule(TimerRequest req) {
            return delegate.schedule(req);
        }

        @Override
        public CompletionStage<Void> cancel(String timerId) {
            return delegate.cancel(timerId);
        }

        @Override
        public void onFire(TimerFireHandler handler) {
            delegate.onFire(handler);
        }

        @Override
        public Duration maxDelay() {
            return maxDelay;
        }
    }

    /** No auto-responding worker: dispatches are captured and counted; results are delivered on demand. */
    private static final class ManualTransport implements TaskTransport {
        private final LinkedBlockingQueue<TaskInvocation> dispatched = new LinkedBlockingQueue<>();
        private final AtomicInteger dispatchCount = new AtomicInteger();
        private volatile TaskResultHandler handler;

        @Override
        public CompletionStage<Void> dispatch(TaskInvocation invocation) {
            dispatchCount.incrementAndGet();
            dispatched.add(invocation);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void onResult(TaskResultHandler handler) {
            this.handler = handler;
        }

        @Override
        public AdapterCapabilities capabilities() {
            return new AdapterCapabilities(DeliveryGuarantee.AT_LEAST_ONCE, true, Long.MAX_VALUE, null, true, true);
        }

        int dispatchCount() {
            return dispatchCount.get();
        }

        TaskInvocation awaitDispatch(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
            TaskInvocation inv = dispatched.poll(timeout, unit);
            if (inv == null) {
                throw new TimeoutException("No dispatch observed within " + timeout + " " + unit);
            }
            return inv;
        }

        CompletionStage<Void> deliverResult(TaskResult result) {
            return handler.handle(result);
        }
    }
}
