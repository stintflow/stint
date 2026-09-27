package io.stintflow.core;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.core.expr.EvalScope;
import io.stintflow.core.expr.ExpressionEvaluator;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.DataFlowSupport;
import io.stintflow.core.interpreter.InterpretResult;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.core.model.CallRemoteNode;
import io.stintflow.core.model.RetryPolicy;
import io.stintflow.core.model.TaskNode;
import io.stintflow.core.model.TryNode;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.RetryState;
import io.stintflow.spi.SaveOutcome;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.TaskTransport;
import io.stintflow.spi.TimerRequest;
import io.stintflow.spi.TimerService;
import io.stintflow.spi.Wait;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.ClaimCheck;
import io.stintflow.wire.Json;

/**
 * The cloud-blind orchestrator.
 * <p>
 * Walks a {@link WorkflowDefinition} tree one local activation at a time (SDD 1.1), checkpointing
 * every transition as one conditional {@link StateStore#save} (SDD 1.2). SDD 1.3 adds real timer
 * firing, {@code timeout.after}, and {@link TryNode} retry/compensation: a task dispatch always
 * registers a {@code task:}+{@code timer:} wait pair; whichever event (result or timeout) wins the
 * conditional consume of both proceeds, the other finds them gone and is a no-op (sec. 8a) — the
 * same {@link StateStore#save}-conflict retry loop from SDD 1.2 handles both concurrency layers.
 * <p>
 * Crucially, this class imports <strong>no cloud SDK</strong> — only the SPI. That invariant is
 * enforced by an ArchUnit test.
 */
public final class WorkflowEngine {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(5);

    /** SDD 1.2, sec. 8d: retry budget for a save() that lost a version/wait race (NOT the business-level TryNode retry). */
    static final int MAX_RETRY_ATTEMPTS = 5;
    private static final long BASE_BACKOFF_MILLIS = 10;
    private static final long MAX_BACKOFF_MILLIS = 200;

    private static final ScheduledExecutorService RETRY_SCHEDULER = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "stint-engine-retry");
        t.setDaemon(true);
        return t;
    });

    private final WorkflowRegistry registry;
    private final TaskTransport transport;
    private final StateStore state;
    private final TimerService timer;
    private final BlobStore blob;
    private final TreeInterpreter interpreter;
    private final InstantSource clock;

    private final Map<String, CompletableFuture<JsonNode>> completions = new ConcurrentHashMap<>();

    public WorkflowEngine(WorkflowRegistry registry, TaskTransport transport, StateStore state,
                          TimerService timer, BlobStore blob) {
        this(registry, transport, state, timer, blob, new TreeInterpreter(new JqExpressionEvaluator()));
    }

    /** @param interpreter override point for tests (e.g. a lower local node limit for CA5). */
    public WorkflowEngine(WorkflowRegistry registry, TaskTransport transport, StateStore state,
                          TimerService timer, BlobStore blob, TreeInterpreter interpreter) {
        this(registry, transport, state, timer, blob, interpreter, InstantSource.system());
    }

    /** @param clock override point for tests (SDD 1.3, sec. 8f) — no sleeps, advance it and call the timer's tick(). */
    public WorkflowEngine(WorkflowRegistry registry, TaskTransport transport, StateStore state,
                          TimerService timer, BlobStore blob, TreeInterpreter interpreter, InstantSource clock) {
        this.registry = registry;
        this.transport = transport;
        this.state = state;
        this.timer = timer;
        this.blob = blob;
        this.interpreter = interpreter;
        this.clock = clock;
        this.transport.onResult(this::onResult);
        this.timer.onFire(this::onTimerFire);
    }

    /** Start an instance and return its id. The instance runs asynchronously, event by event. */
    public String start(WorkflowRef ref, JsonNode input) {
        WorkflowDefinition def = registry.find(ref)
                .orElseThrow(() -> new IllegalArgumentException("Unknown workflow: " + ref.canonical()));
        String instanceId = UUID.randomUUID().toString();
        completions.computeIfAbsent(instanceId, k -> new CompletableFuture<>());
        // $context starts equal to the workflow's raw input (RF5/SDD 1.1 sec. 8c): there is no prior
        // export.as yet, so the instance's accumulated context and its initial data coincide.
        InterpretResult result = interpreter.run(def, WorkflowDefinition.ROOT_POINTER, input, input);
        // A freshly minted UUID has no concurrent writer, so a CONFLICT here is a genuine bug, not a
        // race to retry — no retry loop needed for the very first save.
        commit(instanceId, def, result, 0, List.of())
                .thenAccept(outcome -> {
                    if (outcome != SaveOutcome.OK) {
                        completeExceptionally(instanceId,
                                new IllegalStateException("Unexpected CONFLICT creating new instance " + instanceId));
                    }
                })
                .exceptionally(ex -> {
                    completeExceptionally(instanceId, ex);
                    return null;
                });
        return instanceId;
    }

    /** Convenience for tests/sync callers: start and complete with the final {@code $context}. */
    public CompletionStage<JsonNode> startAndWait(WorkflowRef ref, JsonNode input) {
        return completions.get(start(ref, input));
    }

    // --- events that can resume a suspended instance (SDD 1.3, sec. 8a: handled uniformly) --------

    private sealed interface ResumeEvent {
        record TaskCompleted(TaskResult result) implements ResumeEvent {
        }

        record TimerElapsed(String timerId) implements ResumeEvent {
        }
    }

    private CompletionStage<Void> onResult(TaskResult result) {
        String taskKey = WaitKeys.task(result.correlationId());
        String timerKey = WaitKeys.timer(result.correlationId());
        return timer.cancel(timerKey).thenCompose(v ->
                resumeWithRetry(taskKey, new ResumeEvent.TaskCompleted(result), 1));
    }

    private CompletionStage<Void> onTimerFire(io.stintflow.spi.TimerFire fire) {
        return resumeWithRetry(fire.timerId(), new ResumeEvent.TimerElapsed(fire.timerId()), 1);
    }

    // --- resume path: findWait -> load -> apply -> save, retrying on CONFLICT ----------------------

    private CompletionStage<Void> resumeWithRetry(String waitKey, ResumeEvent event, int attempt) {
        return state.findWait(waitKey).thenCompose(optWait -> {
            if (optWait.isEmpty()) {
                return done(); // unknown, already consumed by a winner, or a stale/obsolete duplicate (sec. 8a)
            }
            String instanceId = optWait.get().instanceId();
            return state.load(instanceId).thenCompose(optSnap -> {
                if (optSnap.isEmpty()) {
                    return done(); // stale: instance is gone
                }
                // Note: we do NOT also require waitKey == snap.waitingKey() here. A dispatch inside a
                // TryNode registers BOTH task:<id> and timer:<id> together (sec. 8a), but a snapshot
                // only records one "primary" waitingKey — so the sibling's own firing would otherwise
                // look "stale" by that check alone. state.findWait(waitKey) already proved this exact
                // wait is still unconsumed, and the save() below is still version+wait conditional, so
                // atomicity doesn't depend on this extra check.
                InstanceSnapshot snap = optSnap.get();
                return advance(snap, event, waitKey)
                        .thenCompose(outcome -> afterAttempt(waitKey, event, attempt, outcome))
                        .exceptionally(ex -> {
                            failExistingInstance(instanceId, ex);
                            return null;
                        });
            });
        });
    }

    private CompletionStage<Void> afterAttempt(String waitKey, ResumeEvent event, int attempt, SaveOutcome outcome) {
        if (outcome == SaveOutcome.OK) {
            return done();
        }
        if (attempt >= MAX_RETRY_ATTEMPTS) {
            return handleExhaustedRetries(waitKey);
        }
        return delay(backoffMillis(attempt)).thenCompose(v -> resumeWithRetry(waitKey, event, attempt + 1));
    }

    /** Sec. 8d: never ambiguous — either someone else legitimately won, or this is a definitive FAILED. */
    private CompletionStage<Void> handleExhaustedRetries(String waitKey) {
        return state.findWait(waitKey).thenCompose(recheck -> {
            if (recheck.isEmpty()) {
                return done(); // another attempt already claimed it — not an error
            }
            return state.load(recheck.get().instanceId()).thenCompose(optSnap -> {
                if (optSnap.isEmpty()) {
                    return done();
                }
                InstanceSnapshot snap = optSnap.get();
                InstanceSnapshot failedSnap = new InstanceSnapshot(snap.instanceId(), snap.definition(),
                        snap.position(), null, snap.context(), InstanceStatus.FAILED, snap.version() + 1, null,
                        clock.instant());
                return state.save(failedSnap, snap.version(), List.of(), List.of(waitKey))
                        .thenAccept(outcome -> completeExceptionally(snap.instanceId(), new RuntimeException(
                                "State store write conflict persisted after " + MAX_RETRY_ATTEMPTS
                                        + " attempts on wait key " + waitKey)));
            });
        });
    }

    /** Applies a task result or timer fire to the snapshot it was found waiting on; returns the save outcome. */
    private CompletionStage<SaveOutcome> advance(InstanceSnapshot snap, ResumeEvent event, String waitKey) {
        WorkflowDefinition def = registry.find(snap.definition()).orElseThrow();
        String instanceId = snap.instanceId();
        TaskNode node = def.at(snap.position());

        if (WaitKeys.isRetry(waitKey)) {
            if (!(node instanceof TryNode tryNode)) {
                return commitTerminal(instanceId, snap.definition(), snap.position(), snap.context(),
                        InstanceStatus.FAILED, snap.version(), List.of(waitKey),
                        () -> completeExceptionally(instanceId, new IllegalStateException(
                                "Instance " + instanceId + " has a retry wait but is not at a TryNode: " + snap.position())));
            }
            return redispatchRetry(instanceId, def, tryNode, snap, waitKey);
        }

        String correlationId = WaitKeys.rawId(waitKey);
        List<String> consumeKeys = List.of(WaitKeys.task(correlationId), WaitKeys.timer(correlationId));
        boolean succeeded = event instanceof ResumeEvent.TaskCompleted tc
                && tc.result().status() == TaskResult.Status.COMPLETED;

        if (succeeded) {
            TaskResult result = ((ResumeEvent.TaskCompleted) event).result();
            if (!(node instanceof CallRemoteNode) && !(node instanceof TryNode)) {
                return commitTerminal(instanceId, snap.definition(), snap.position(), snap.context(),
                        InstanceStatus.FAILED, snap.version(), consumeKeys,
                        () -> completeExceptionally(instanceId, new IllegalStateException(
                                "Instance " + instanceId + " was waiting at an unexpected node: " + snap.position())));
            }
            return ClaimCheck.rehydrate(result.output(), blob).thenCompose(rawOutput -> {
                InterpretResult next = node instanceof TryNode tryNode
                        ? interpreter.resumeTry(def, tryNode, rawOutput, snap.context())
                        : interpreter.resume(def, snap.position(), (CallRemoteNode) node, rawOutput, snap.context());
                return commit(instanceId, def, next, snap.version(), consumeKeys);
            });
        }

        ErrorInfo error = event instanceof ResumeEvent.TaskCompleted tc ? tc.result().error()
                : ErrorInfo.timeout("Task at " + snap.position() + " did not respond within its timeout");

        if (node instanceof TryNode tryNode) {
            return handleTryFailure(instanceId, def, tryNode, snap, error, consumeKeys);
        }
        return commitTerminal(instanceId, snap.definition(), snap.position(), snap.context(),
                InstanceStatus.FAILED, snap.version(), consumeKeys,
                () -> completeExceptionally(instanceId, new RuntimeException("Task failed: " + error)));
    }

    // --- TryNode: catch / retry / compensation (SDD 1.3) -------------------------------------------

    private CompletionStage<SaveOutcome> handleTryFailure(String instanceId, WorkflowDefinition def, TryNode tryNode,
            InstanceSnapshot snap, ErrorInfo error, List<String> consumeKeys) {
        TryNode.Catch catchClause = tryNode.catchClause();
        ExpressionEvaluator evaluator = interpreter.evaluator();
        JsonNode errorJson = errorToJsonForEval(error);

        if (!matchesCatch(catchClause, errorJson, snap.context(), evaluator, def.ref())) {
            return commitTerminal(instanceId, snap.definition(), snap.position(), snap.context(),
                    InstanceStatus.FAILED, snap.version(), consumeKeys,
                    () -> completeExceptionally(instanceId, new RuntimeException("Uncaught task failure: " + error)));
        }

        RetryState prior = snap.retryState();
        int attempt = prior != null ? prior.attempt() : 1;
        Instant firstAttemptAt = prior != null ? prior.firstAttemptAt() : clock.instant();

        if (catchClause.retry() != null) {
            RetryPolicy retry = catchClause.retry();
            boolean underAttempts = attempt < retry.maxAttempts();
            boolean underDuration = retry.maxDuration() == null
                    || Duration.between(firstAttemptAt, clock.instant()).compareTo(retry.maxDuration()) < 0;

            if (underAttempts && underDuration) {
                Duration delay = withMinimumDelay(applyJitter(retry.delayFor(attempt + 1), retry.jitterRatio()),
                        error.retryAfter());
                if (delay.compareTo(timer.maxDelay()) <= 0) {
                    return scheduleRetryDelay(instanceId, snap, tryNode, attempt, firstAttemptAt, error, delay, consumeKeys);
                }
                return commitTerminal(instanceId, snap.definition(), snap.position(), snap.context(),
                        InstanceStatus.FAILED, snap.version(), consumeKeys,
                        () -> completeExceptionally(instanceId, new RuntimeException(
                                "Retry delay " + delay + " exceeds the timer's max delay " + timer.maxDelay())));
            }
            return commitTerminal(instanceId, snap.definition(), snap.position(), snap.context(),
                    InstanceStatus.FAILED, snap.version(), consumeKeys,
                    () -> completeExceptionally(instanceId, new RuntimeException(
                            "Retry limit exhausted after " + attempt + " attempt(s): " + error)));
        }

        // caught, no retry policy: optional local compensation, then continue via catch.then
        JsonNode resultData = catchClause.compensation() == null ? errorJson
                : evaluator.eval(catchClause.compensation(), errorJson, new EvalScope(snap.context(), def.ref()));
        InterpretResult next = interpreter.resumeFromCatch(def, tryNode, resultData, snap.context());
        return commit(instanceId, def, next, snap.version(), consumeKeys);
    }

    private CompletionStage<SaveOutcome> scheduleRetryDelay(String instanceId, InstanceSnapshot snap, TryNode tryNode,
            int attempt, Instant firstAttemptAt, ErrorInfo error, Duration delay, List<String> consumeKeys) {
        String retryId = UUID.randomUUID().toString();
        String retryKey = WaitKeys.retry(retryId);
        RetryState newRetryState = new RetryState(tryNode.pointer(), attempt, firstAttemptAt, error, null);
        InstanceSnapshot next = new InstanceSnapshot(instanceId, snap.definition(), snap.position(), retryKey,
                snap.context(), InstanceStatus.WAITING, snap.version() + 1, newRetryState, clock.instant());

        return state.save(next, snap.version(),
                        List.of(new Wait(retryKey, instanceId, snap.position(), clock.instant())), consumeKeys)
                .thenCompose(outcome -> {
                    if (outcome != SaveOutcome.OK) {
                        return CompletableFuture.completedFuture(outcome);
                    }
                    return timer.schedule(new TimerRequest(retryKey, instanceId, null, clock.instant().plus(delay)))
                            .thenApply(v -> outcome);
                });
    }

    private CompletionStage<SaveOutcome> redispatchRetry(String instanceId, WorkflowDefinition def, TryNode tryNode,
            InstanceSnapshot snap, String retryWaitKey) {
        RetryState prior = snap.retryState();
        int nextAttempt = (prior != null ? prior.attempt() : 1) + 1;
        Instant firstAttemptAt = prior != null ? prior.firstAttemptAt() : clock.instant();
        String correlationId = UUID.randomUUID().toString();
        String taskKey = WaitKeys.task(correlationId);
        String timerKey = WaitKeys.timer(correlationId);
        CallRemoteNode body = tryNode.body();

        JsonNode effectiveInput = DataFlowSupport.applyExpr(interpreter.evaluator(), tryNode.dataFlow().inputFrom(),
                snap.context(), snap.context(), def.ref());

        return ClaimCheck.offload(effectiveInput, transport.capabilities(), blob, instanceId + "/" + body.name())
                .thenCompose(wireInput -> {
                    RetryState newRetryState = new RetryState(tryNode.pointer(), nextAttempt, firstAttemptAt,
                            prior != null ? prior.lastError() : null, correlationId);
                    InstanceSnapshot next = new InstanceSnapshot(instanceId, snap.definition(), snap.position(),
                            taskKey, snap.context(), InstanceStatus.WAITING, snap.version() + 1, newRetryState,
                            clock.instant());
                    Duration timeout = effectiveTimeout(body);
                    TaskInvocation inv = new TaskInvocation(instanceId, snap.position(), correlationId,
                            snap.definition(), body.routingKey(), wireInput, nextAttempt);

                    return state.save(next, snap.version(),
                                    List.of(new Wait(taskKey, instanceId, snap.position(), clock.instant()),
                                            new Wait(timerKey, instanceId, snap.position(), clock.instant())),
                                    List.of(retryWaitKey))
                            .thenCompose(outcome -> {
                                if (outcome != SaveOutcome.OK) {
                                    return CompletableFuture.completedFuture(outcome);
                                }
                                return timer.schedule(new TimerRequest(timerKey, instanceId, correlationId,
                                                clock.instant().plus(timeout)))
                                        .thenCompose(v -> transport.dispatch(inv))
                                        .thenApply(v -> outcome);
                            });
                });
    }

    private boolean matchesCatch(TryNode.Catch catchClause, JsonNode errorJson, JsonNode context,
            ExpressionEvaluator evaluator, WorkflowRef ref) {
        if (catchClause.errorFilter() != null && !isTruthy(evaluator.eval(catchClause.errorFilter(), errorJson,
                new EvalScope(context, ref)))) {
            return false;
        }
        if (catchClause.when() != null && !isTruthy(evaluator.eval(catchClause.when(), errorJson,
                new EvalScope(context, ref)))) {
            return false;
        }
        return catchClause.exceptWhen() == null
                || !isTruthy(evaluator.eval(catchClause.exceptWhen(), errorJson, new EvalScope(context, ref)));
    }

    private static boolean isTruthy(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return false;
        }
        return !node.isBoolean() || node.booleanValue();
    }

    private static JsonNode errorToJsonForEval(ErrorInfo error) {
        var node = Json.obj();
        node.put("type", error.type() == null ? null : error.type().toString());
        if (error.status() != null) {
            node.put("status", error.status());
        }
        if (error.title() != null) {
            node.put("title", error.title());
        }
        if (error.detail() != null) {
            node.put("detail", error.detail());
        }
        if (error.instance() != null) {
            node.put("instance", error.instance());
        }
        if (error.retryAfter() != null) {
            node.put("retryAfter", error.retryAfter().toString());
        }
        return node;
    }

    private static Duration applyJitter(Duration base, double jitterRatio) {
        if (jitterRatio <= 0) {
            return base;
        }
        double factor = 1.0 + (ThreadLocalRandom.current().nextDouble() * 2 - 1) * jitterRatio;
        long millis = Math.max(0, Math.round(base.toMillis() * factor));
        return Duration.ofMillis(millis);
    }

    /** RNF2: {@code retryAfter} from the error is a floor on the computed delay, not a suggestion. */
    private static Duration withMinimumDelay(Duration computed, Duration retryAfter) {
        return retryAfter != null && computed.compareTo(retryAfter) < 0 ? retryAfter : computed;
    }

    private static Duration effectiveTimeout(CallRemoteNode node) {
        return node.timeout() != null ? node.timeout() : DEFAULT_TIMEOUT;
    }

    // --- persisting an InterpretResult: Suspend / SuspendInTry / Complete / Failed ------------------

    private CompletionStage<SaveOutcome> commit(String instanceId, WorkflowDefinition def, InterpretResult result,
            long expectedVersion, List<String> consumeWaitKeys) {
        return switch (result) {
            case InterpretResult.Suspend s -> commitSuspend(instanceId, def, s.node(), s.pointer(), s.dispatchInput(),
                    s.context(), expectedVersion, consumeWaitKeys, null);
            case InterpretResult.SuspendInTry s -> commitSuspend(instanceId, def, s.tryNode().body(),
                    s.tryNode().pointer(), s.dispatchInput(), s.context(), expectedVersion, consumeWaitKeys,
                    new RetryState(s.tryNode().pointer(), 1, clock.instant(), null, null));
            case InterpretResult.Complete c -> commitTerminal(instanceId, def.ref(), c.pointer(), c.context(),
                    InstanceStatus.COMPLETED, expectedVersion, consumeWaitKeys,
                    () -> complete(instanceId, c.context()));
            case InterpretResult.Failed f -> commitTerminal(instanceId, def.ref(), f.pointer(), f.context(),
                    InstanceStatus.FAILED, expectedVersion, consumeWaitKeys,
                    () -> completeExceptionally(instanceId, new RuntimeException(f.message())));
        };
    }

    private CompletionStage<SaveOutcome> commitSuspend(String instanceId, WorkflowDefinition def, CallRemoteNode node,
            String position, JsonNode dispatchInput, JsonNode context, long expectedVersion,
            List<String> consumeWaitKeys, RetryState initialRetryState) {
        String correlationId = UUID.randomUUID().toString();
        String taskKey = WaitKeys.task(correlationId);
        String timerKey = WaitKeys.timer(correlationId);
        RetryState retryState = initialRetryState == null ? null
                : new RetryState(initialRetryState.tryNodePointer(), initialRetryState.attempt(),
                        initialRetryState.firstAttemptAt(), initialRetryState.lastError(), correlationId);

        return ClaimCheck.offload(dispatchInput, transport.capabilities(), blob, instanceId + "/" + node.name())
                .thenCompose(wireInput -> {
                    InstanceSnapshot snap = new InstanceSnapshot(instanceId, def.ref(), position, taskKey, context,
                            InstanceStatus.WAITING, expectedVersion + 1, retryState, clock.instant());
                    Duration timeout = effectiveTimeout(node);
                    int attempt = retryState != null ? retryState.attempt() : 1;
                    TaskInvocation inv = new TaskInvocation(instanceId, position, correlationId, def.ref(),
                            node.routingKey(), wireInput, attempt);

                    return state.save(snap, expectedVersion,
                                    List.of(new Wait(taskKey, instanceId, position, clock.instant()),
                                            new Wait(timerKey, instanceId, position, clock.instant())),
                                    consumeWaitKeys)
                            .thenCompose(outcome -> {
                                if (outcome != SaveOutcome.OK) {
                                    return CompletableFuture.completedFuture(outcome);
                                }
                                return timer.schedule(new TimerRequest(timerKey, instanceId, correlationId,
                                                clock.instant().plus(timeout)))
                                        .thenCompose(v -> transport.dispatch(inv))
                                        .thenApply(v -> outcome);
                            });
                });
    }

    private CompletionStage<SaveOutcome> commitTerminal(String instanceId, WorkflowRef ref, String position,
            JsonNode context, InstanceStatus status, long expectedVersion, List<String> consumeWaitKeys,
            Runnable onSuccess) {
        InstanceSnapshot snap = new InstanceSnapshot(instanceId, ref, position, null, context, status,
                expectedVersion + 1, null, clock.instant());
        return state.save(snap, expectedVersion, List.of(), consumeWaitKeys)
                .thenApply(outcome -> {
                    if (outcome == SaveOutcome.OK) {
                        onSuccess.run();
                    }
                    return outcome;
                });
    }

    private void complete(String instanceId, JsonNode context) {
        completions.computeIfAbsent(instanceId, k -> new CompletableFuture<>()).complete(context);
    }

    private void completeExceptionally(String instanceId, Throwable t) {
        completions.computeIfAbsent(instanceId, k -> new CompletableFuture<>()).completeExceptionally(t);
    }

    /** Best-effort: an I/O failure (not a version/wait conflict) happened mid-transition. */
    private void failExistingInstance(String instanceId, Throwable t) {
        state.load(instanceId).thenAccept(opt -> opt.ifPresent(snap -> {
            InstanceSnapshot failedSnap = new InstanceSnapshot(instanceId, snap.definition(), snap.position(),
                    null, snap.context(), InstanceStatus.FAILED, snap.version() + 1, null, clock.instant());
            List<String> consume = snap.waitingKey() == null ? List.of() : List.of(snap.waitingKey());
            state.save(failedSnap, snap.version(), List.of(), consume);
        }));
        completeExceptionally(instanceId, t);
    }

    private static long backoffMillis(int attempt) {
        long value = BASE_BACKOFF_MILLIS * (1L << attempt);
        return Math.min(value, MAX_BACKOFF_MILLIS);
    }

    private static CompletionStage<Void> delay(long millis) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        RETRY_SCHEDULER.schedule(() -> future.complete(null), millis, TimeUnit.MILLISECONDS);
        return future;
    }

    private static CompletionStage<Void> done() {
        return CompletableFuture.completedFuture(null);
    }
}
