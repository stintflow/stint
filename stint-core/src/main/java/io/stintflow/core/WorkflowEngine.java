package io.stintflow.core;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.core.expr.EvalScope;
import io.stintflow.core.expr.ExpressionEvaluator;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.expr.WorkflowDescriptor;
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
import io.cloudevents.CloudEvent;
import io.stintflow.wire.CeWire;
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

    private static final Logger LOG = System.getLogger(WorkflowEngine.class.getName());

    private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(5);

    /** SDD 1.2, sec. 8d: retry budget for a save() that lost a version/wait race (NOT the business-level TryNode retry). */
    static final int MAX_RETRY_ATTEMPTS = 5;
    private static final long BASE_BACKOFF_MILLIS = 10;
    private static final long MAX_BACKOFF_MILLIS = 200;

    /** SDD 1.5, RF4: how often {@link #awaitCompletion} rechecks the StateStore — an engine
     *  implementation detail (not business time), so a real short delay is fine even in tests. */
    private static final Duration AWAIT_POLL_INTERVAL = Duration.ofMillis(20);
    private static final Duration DEFAULT_AWAIT_TIMEOUT = Duration.ofSeconds(30);

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
        Instance inst = new Instance(UUID.randomUUID().toString(), input, clock.instant());
        String instanceId = inst.id();
        // $context starts equal to the workflow's raw input (RF5/SDD 1.1 sec. 8c): there is no prior
        // export.as yet, so the instance's accumulated context and its initial data coincide.
        InterpretResult result = interpreter.run(def, inst.descriptor(ref), WorkflowDefinition.ROOT_POINTER, input, input);
        // A freshly minted UUID has no concurrent writer, so a CONFLICT here is a genuine bug, not a
        // race to retry — no retry loop needed for the very first save.
        commit(inst, def, result, 0, List.of())
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

    /**
     * SDD 2.1, RF2/RF6: starts {@code target} from an inbound domain event, <em>exactly once</em> per
     * (event, definition).
     * <p>
     * The instance id is derived from the definition and the event's {@code source} + {@code id}
     * ({@link #triggeredInstanceId}), so the idempotency key <em>is</em> the instance: creation is the
     * existing conditional {@code expectedVersion=0} save (SDD 1.2) — one write, no window between "key
     * recorded" and "instance recorded" (sec. 8a). A redelivery or a concurrent duplicate loses that
     * save with {@code CONFLICT}, finds the instance and completes normally with the same id (no-op).
     * <p>
     * Unlike {@link #start}, the returned stage covers every effect (save, timer, dispatch), so a
     * {@link io.stintflow.spi.DomainEventSource} can acknowledge the message only after it completes.
     * The workflow input is DSL 1.0's "array containing the events that trigger the execution"
     * ({@code dsl.md}): {@code [<the event in CloudEvents structured JSON>]}. A failure while running
     * the local part of the workflow (e.g. a bad {@code input.from}) persists the instance as
     * {@code FAILED} and still completes normally (sec. 8d) — redelivering it would fail the same way.
     */
    public CompletionStage<String> startFromEvent(WorkflowRef target, CloudEvent trigger) {
        WorkflowDefinition def = registry.find(target).orElse(null);
        if (def == null) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Unknown workflow: " + target.canonical()));
        }
        String instanceId = triggeredInstanceId(target, trigger);
        JsonNode input = Json.MAPPER.createArrayNode().add(Json.read(CeWire.toJson(trigger)));
        Instance inst = new Instance(instanceId, input, clock.instant());
        InterpretResult result;
        try {
            result = interpreter.run(def, inst.descriptor(target), WorkflowDefinition.ROOT_POINTER, input, input);
        } catch (RuntimeException e) {
            result = new InterpretResult.Failed(WorkflowDefinition.ROOT_POINTER, input,
                    "Failed to start from event " + trigger.getId() + ": " + e.getMessage());
        }
        return commit(inst, def, result, 0, List.of()).thenCompose(outcome -> {
            if (outcome == SaveOutcome.OK) {
                return CompletableFuture.completedFuture(instanceId);
            }
            return state.load(instanceId).thenApply(existing -> {
                if (existing.isEmpty()) {
                    throw new IllegalStateException("CONFLICT creating " + instanceId + " but no such instance exists");
                }
                LOG.log(Level.DEBUG, "Duplicate delivery of event {0} from {1}: instance {2} already exists",
                        trigger.getId(), trigger.getSource(), instanceId);
                return instanceId;
            });
        });
    }

    /**
     * SDD 2.1, sec. 8a: {@code "evt-" + UUIDv3(definition, source, id)}. Name-based only — a stable
     * name, not a security property. The definition (including its version) is part of the key
     * because one event may start several definitions (sec. 8b), each an independent execution.
     */
    public static String triggeredInstanceId(WorkflowRef target, CloudEvent trigger) {
        String name = target.canonical() + '\n' + trigger.getSource() + '\n' + trigger.getId();
        return "evt-" + UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8));
    }

    /** Convenience for tests/sync callers: start and await completion with the default timeout. */
    public CompletionStage<JsonNode> startAndWait(WorkflowRef ref, JsonNode input) {
        return awaitCompletion(start(ref, input), DEFAULT_AWAIT_TIMEOUT);
    }

    /**
     * SDD 1.5, RF4: waits for {@code instanceId} to reach a terminal {@link InstanceStatus} by
     * polling the {@link StateStore} — works across JVMs/engine instances sharing the same store,
     * unlike the old in-memory-only shortcut this replaces. Completes with the instance's final
     * {@code $context} on {@code COMPLETED}; completes exceptionally on {@code FAILED} or timeout
     * (never with an empty result — a silent empty result would hide a real failure).
     * <p>
     * Known limitation (sec. 8d): {@link InstanceSnapshot} does not persist <em>why</em> an instance
     * failed, only that it did — the exceptional completion on {@code FAILED} can report the
     * instance id and its last position, not the original {@link ErrorInfo}. Not intended for
     * high-volume production use (poll cost is linear in the number of concurrent waiters); fine for
     * tests and one-off synchronous calls.
     */
    public CompletionStage<JsonNode> awaitCompletion(String instanceId, Duration timeout) {
        CompletableFuture<JsonNode> result = new CompletableFuture<>();
        pollForCompletion(instanceId, clock.instant().plus(timeout), result);
        return result;
    }

    private void pollForCompletion(String instanceId, Instant deadline, CompletableFuture<JsonNode> result) {
        state.load(instanceId).whenComplete((optSnap, ex) -> {
            if (ex != null) {
                result.completeExceptionally(ex);
                return;
            }
            if (optSnap.isPresent()) {
                InstanceSnapshot snap = optSnap.get();
                if (snap.status() == InstanceStatus.COMPLETED) {
                    result.complete(snap.context());
                    return;
                }
                if (snap.status() == InstanceStatus.FAILED) {
                    result.completeExceptionally(new RuntimeException(
                            "Instance " + instanceId + " failed (last position: " + snap.position() + ")"));
                    return;
                }
            }
            if (clock.instant().isAfter(deadline)) {
                result.completeExceptionally(
                        new TimeoutException("Instance " + instanceId + " did not complete within the timeout"));
                return;
            }
            RETRY_SCHEDULER.schedule(() -> pollForCompletion(instanceId, deadline, result),
                    AWAIT_POLL_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
        });
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
                        clock.instant(), snap.input(), snap.startedAt());
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
        Instance inst = Instance.of(snap);
        String instanceId = inst.id();
        WorkflowDescriptor wf = inst.descriptor(snap.definition());
        TaskNode node = def.at(snap.position());

        if (WaitKeys.isRetry(waitKey)) {
            if (!(node instanceof TryNode tryNode)) {
                return commitTerminal(inst, snap.definition(), snap.position(), snap.context(),
                        InstanceStatus.FAILED, snap.version(), List.of(waitKey),
                        () -> completeExceptionally(instanceId, new IllegalStateException(
                                "Instance " + instanceId + " has a retry wait but is not at a TryNode: " + snap.position())));
            }
            return redispatchRetry(inst, def, tryNode, snap, waitKey);
        }

        String correlationId = WaitKeys.rawId(waitKey);
        List<String> consumeKeys = List.of(WaitKeys.task(correlationId), WaitKeys.timer(correlationId));
        boolean succeeded = event instanceof ResumeEvent.TaskCompleted tc
                && tc.result().status() == TaskResult.Status.COMPLETED;

        if (succeeded) {
            TaskResult result = ((ResumeEvent.TaskCompleted) event).result();
            if (!(node instanceof CallRemoteNode) && !(node instanceof TryNode)) {
                return commitTerminal(inst, snap.definition(), snap.position(), snap.context(),
                        InstanceStatus.FAILED, snap.version(), consumeKeys,
                        () -> completeExceptionally(instanceId, new IllegalStateException(
                                "Instance " + instanceId + " was waiting at an unexpected node: " + snap.position())));
            }
            return ClaimCheck.rehydrate(result.output(), blob).thenCompose(rawOutput -> {
                InterpretResult next = node instanceof TryNode tryNode
                        ? interpreter.resumeTry(def, wf, tryNode, rawOutput, snap.context())
                        : interpreter.resume(def, wf, snap.position(), (CallRemoteNode) node, rawOutput, snap.context());
                return commit(inst, def, next, snap.version(), consumeKeys);
            });
        }

        ErrorInfo error = event instanceof ResumeEvent.TaskCompleted tc ? tc.result().error()
                : ErrorInfo.timeout("Task at " + snap.position() + " did not respond within its timeout");

        if (node instanceof TryNode tryNode) {
            return handleTryFailure(inst, def, tryNode, snap, error, consumeKeys);
        }
        return commitTerminal(inst, snap.definition(), snap.position(), snap.context(),
                InstanceStatus.FAILED, snap.version(), consumeKeys,
                () -> completeExceptionally(instanceId, new RuntimeException("Task failed: " + error)));
    }

    // --- TryNode: catch / retry / compensation (SDD 1.3) -------------------------------------------

    private CompletionStage<SaveOutcome> handleTryFailure(Instance inst, WorkflowDefinition def, TryNode tryNode,
            InstanceSnapshot snap, ErrorInfo error, List<String> consumeKeys) {
        String instanceId = inst.id();
        WorkflowDescriptor wf = inst.descriptor(def.ref());
        TryNode.Catch catchClause = tryNode.catchClause();
        ExpressionEvaluator evaluator = interpreter.evaluator();
        JsonNode errorJson = errorToJsonForEval(error);

        if (!matchesCatch(catchClause, errorJson, snap.context(), evaluator, wf)) {
            return commitTerminal(inst, snap.definition(), snap.position(), snap.context(),
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
                    return scheduleRetryDelay(inst, snap, tryNode, attempt, firstAttemptAt, error, delay, consumeKeys);
                }
                return commitTerminal(inst, snap.definition(), snap.position(), snap.context(),
                        InstanceStatus.FAILED, snap.version(), consumeKeys,
                        () -> completeExceptionally(instanceId, new RuntimeException(
                                "Retry delay " + delay + " exceeds the timer's max delay " + timer.maxDelay())));
            }
            return commitTerminal(inst, snap.definition(), snap.position(), snap.context(),
                    InstanceStatus.FAILED, snap.version(), consumeKeys,
                    () -> completeExceptionally(instanceId, new RuntimeException(
                            "Retry limit exhausted after " + attempt + " attempt(s): " + error)));
        }

        // caught, no retry policy: optional local compensation, then continue via catch.then
        JsonNode resultData = catchClause.compensation() == null ? errorJson
                : evaluator.eval(catchClause.compensation(), errorJson, new EvalScope(snap.context(), wf));
        InterpretResult next = interpreter.resumeFromCatch(def, wf, tryNode, resultData, snap.context());
        return commit(inst, def, next, snap.version(), consumeKeys);
    }

    private CompletionStage<SaveOutcome> scheduleRetryDelay(Instance inst, InstanceSnapshot snap, TryNode tryNode,
            int attempt, Instant firstAttemptAt, ErrorInfo error, Duration delay, List<String> consumeKeys) {
        String instanceId = inst.id();
        String retryId = UUID.randomUUID().toString();
        String retryKey = WaitKeys.retry(retryId);
        RetryState newRetryState = new RetryState(tryNode.pointer(), attempt, firstAttemptAt, error, null);
        InstanceSnapshot next = new InstanceSnapshot(instanceId, snap.definition(), snap.position(), retryKey,
                snap.context(), InstanceStatus.WAITING, snap.version() + 1, newRetryState, clock.instant(),
                inst.input(), inst.startedAt());

        return armThenSave(new TimerRequest(retryKey, instanceId, null, clock.instant().plus(delay)),
                () -> state.save(next, snap.version(),
                        List.of(new Wait(retryKey, instanceId, snap.position(), clock.instant())), consumeKeys),
                () -> done());
    }

    private CompletionStage<SaveOutcome> redispatchRetry(Instance inst, WorkflowDefinition def, TryNode tryNode,
            InstanceSnapshot snap, String retryWaitKey) {
        String instanceId = inst.id();
        RetryState prior = snap.retryState();
        int nextAttempt = (prior != null ? prior.attempt() : 1) + 1;
        Instant firstAttemptAt = prior != null ? prior.firstAttemptAt() : clock.instant();
        String correlationId = UUID.randomUUID().toString();
        String taskKey = WaitKeys.task(correlationId);
        String timerKey = WaitKeys.timer(correlationId);
        CallRemoteNode body = tryNode.body();

        JsonNode effectiveInput = DataFlowSupport.applyExpr(interpreter.evaluator(), tryNode.dataFlow().inputFrom(),
                snap.context(), snap.context(), inst.descriptor(def.ref()));

        // Keyed per attempt: with deterministic event-started ids (SDD 2.1, 8a) two racing duplicates must
        // never overwrite each other's claim-checked payload.
        return ClaimCheck.offload(effectiveInput, transport.capabilities(), blob,
                        instanceId + "/" + body.name() + "/" + correlationId)
                .thenCompose(wireInput -> {
                    RetryState newRetryState = new RetryState(tryNode.pointer(), nextAttempt, firstAttemptAt,
                            prior != null ? prior.lastError() : null, correlationId);
                    InstanceSnapshot next = new InstanceSnapshot(instanceId, snap.definition(), snap.position(),
                            taskKey, snap.context(), InstanceStatus.WAITING, snap.version() + 1, newRetryState,
                            clock.instant(), inst.input(), inst.startedAt());
                    Duration timeout = effectiveTimeout(body);
                    TaskInvocation inv = new TaskInvocation(instanceId, snap.position(), correlationId,
                            snap.definition(), body.routingKey(), wireInput, nextAttempt);

                    return armThenSave(new TimerRequest(timerKey, instanceId, correlationId, clock.instant().plus(timeout)),
                            () -> state.save(next, snap.version(),
                                    List.of(new Wait(taskKey, instanceId, snap.position(), clock.instant()),
                                            new Wait(timerKey, instanceId, snap.position(), clock.instant())),
                                    List.of(retryWaitKey)),
                            () -> transport.dispatch(inv));
                });
    }

    private boolean matchesCatch(TryNode.Catch catchClause, JsonNode errorJson, JsonNode context,
            ExpressionEvaluator evaluator, WorkflowDescriptor wf) {
        if (catchClause.errorFilter() != null && !isTruthy(evaluator.eval(catchClause.errorFilter(), errorJson,
                new EvalScope(context, wf)))) {
            return false;
        }
        if (catchClause.when() != null && !isTruthy(evaluator.eval(catchClause.when(), errorJson,
                new EvalScope(context, wf)))) {
            return false;
        }
        return catchClause.exceptWhen() == null
                || !isTruthy(evaluator.eval(catchClause.exceptWhen(), errorJson, new EvalScope(context, wf)));
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

    private CompletionStage<SaveOutcome> commit(Instance inst, WorkflowDefinition def, InterpretResult result,
            long expectedVersion, List<String> consumeWaitKeys) {
        String instanceId = inst.id();
        return switch (result) {
            case InterpretResult.Suspend s -> commitSuspend(inst, def, s.node(), s.pointer(), s.dispatchInput(),
                    s.context(), expectedVersion, consumeWaitKeys, null);
            case InterpretResult.SuspendInTry s -> commitSuspend(inst, def, s.tryNode().body(),
                    s.tryNode().pointer(), s.dispatchInput(), s.context(), expectedVersion, consumeWaitKeys,
                    new RetryState(s.tryNode().pointer(), 1, clock.instant(), null, null));
            case InterpretResult.Complete c -> commitTerminal(inst, def.ref(), c.pointer(), c.context(),
                    InstanceStatus.COMPLETED, expectedVersion, consumeWaitKeys,
                    () -> complete(instanceId, c.context()));
            case InterpretResult.Failed f -> commitTerminal(inst, def.ref(), f.pointer(), f.context(),
                    InstanceStatus.FAILED, expectedVersion, consumeWaitKeys,
                    () -> completeExceptionally(instanceId, new RuntimeException(f.message())));
        };
    }

    private CompletionStage<SaveOutcome> commitSuspend(Instance inst, WorkflowDefinition def, CallRemoteNode node,
            String position, JsonNode dispatchInput, JsonNode context, long expectedVersion,
            List<String> consumeWaitKeys, RetryState initialRetryState) {
        String instanceId = inst.id();
        String correlationId = UUID.randomUUID().toString();
        String taskKey = WaitKeys.task(correlationId);
        String timerKey = WaitKeys.timer(correlationId);
        RetryState retryState = initialRetryState == null ? null
                : new RetryState(initialRetryState.tryNodePointer(), initialRetryState.attempt(),
                        initialRetryState.firstAttemptAt(), initialRetryState.lastError(), correlationId);

        // Keyed per attempt (see redispatchRetry): racing duplicate creates never share a blob key.
        return ClaimCheck.offload(dispatchInput, transport.capabilities(), blob,
                        instanceId + "/" + node.name() + "/" + correlationId)
                .thenCompose(wireInput -> {
                    InstanceSnapshot snap = new InstanceSnapshot(instanceId, def.ref(), position, taskKey, context,
                            InstanceStatus.WAITING, expectedVersion + 1, retryState, clock.instant(),
                            inst.input(), inst.startedAt());
                    Duration timeout = effectiveTimeout(node);
                    int attempt = retryState != null ? retryState.attempt() : 1;
                    TaskInvocation inv = new TaskInvocation(instanceId, position, correlationId, def.ref(),
                            node.routingKey(), wireInput, attempt);

                    return armThenSave(new TimerRequest(timerKey, instanceId, correlationId, clock.instant().plus(timeout)),
                            () -> state.save(snap, expectedVersion,
                                    List.of(new Wait(taskKey, instanceId, position, clock.instant()),
                                            new Wait(timerKey, instanceId, position, clock.instant())),
                                    consumeWaitKeys),
                            () -> transport.dispatch(inv));
                });
    }

    /**
     * SDD 2.1, RF8: arms the timer <em>before</em> the save that registers the wait it guards, then runs
     * {@code afterSaved} (the dispatch) only once the save is OK. A crash after the save can no longer
     * leave a WAITING instance with no timer and no task in flight: the timer is already armed, fires,
     * and the instance follows its timeout/retry policy (SDD 1.3). If the save loses (CONFLICT) or
     * fails, the armed timer is an orphan: it's cancelled best-effort, and if it fires anyway
     * {@code findWait} finds nothing and the fire is a no-op (SDD 1.3, sec. 8a).
     */
    private CompletionStage<SaveOutcome> armThenSave(TimerRequest timerRequest,
            Supplier<CompletionStage<SaveOutcome>> save,
            Supplier<CompletionStage<Void>> afterSaved) {
        return timer.schedule(timerRequest)
                .thenCompose(timerId -> save.get())
                .thenCompose(outcome -> {
                    if (outcome != SaveOutcome.OK) {
                        return timer.cancel(timerRequest.timerId()).thenApply(v -> outcome);
                    }
                    return afterSaved.get().thenApply(v -> outcome);
                });
    }

    private CompletionStage<SaveOutcome> commitTerminal(Instance inst, WorkflowRef ref, String position,
            JsonNode context, InstanceStatus status, long expectedVersion, List<String> consumeWaitKeys,
            Runnable onSuccess) {
        String instanceId = inst.id();
        InstanceSnapshot snap = new InstanceSnapshot(instanceId, ref, position, null, context, status,
                expectedVersion + 1, null, clock.instant(), inst.input(), inst.startedAt());
        return state.save(snap, expectedVersion, List.of(), consumeWaitKeys)
                .thenApply(outcome -> {
                    if (outcome == SaveOutcome.OK) {
                        onSuccess.run();
                    }
                    return outcome;
                });
    }

    /**
     * SDD 1.5, RF4: the StateStore save that reaches {@code COMPLETED} has already happened by the
     * time this runs — {@link #awaitCompletion} discovers it independently by polling the store, so
     * this is just an observability hook now (the in-memory-only completions map it used to feed is
     * gone: it never worked across JVMs and its entries were never evicted).
     */
    private void complete(String instanceId, JsonNode context) {
        LOG.log(Level.DEBUG, "Instance {0} completed", instanceId);
    }

    private void completeExceptionally(String instanceId, Throwable t) {
        LOG.log(Level.WARNING, "Instance " + instanceId + " failed", t);
    }

    /** Best-effort: an I/O failure (not a version/wait conflict) happened mid-transition. */
    private void failExistingInstance(String instanceId, Throwable t) {
        state.load(instanceId).thenAccept(opt -> opt.ifPresent(snap -> {
            InstanceSnapshot failedSnap = new InstanceSnapshot(instanceId, snap.definition(), snap.position(),
                    null, snap.context(), InstanceStatus.FAILED, snap.version() + 1, null, clock.instant(),
                    snap.input(), snap.startedAt());
            List<String> consume = snap.waitingKey() == null ? List.of() : List.of(snap.waitingKey());
            state.save(failedSnap, snap.version(), List.of(), consume);
        }));
        completeExceptionally(instanceId, t);
    }

    /**
     * What identifies a running instance across every transition (SDD 2.1, RF6): its id plus the raw
     * input and start time the DSL 1.0 {@code $workflow} descriptor exposes. Carried unchanged into
     * every snapshot so a resume on another engine sees the same {@code $workflow}.
     */
    private record Instance(String id, JsonNode input, Instant startedAt) {

        static Instance of(InstanceSnapshot snap) {
            return new Instance(snap.instanceId(), snap.input(), snap.startedAt());
        }

        WorkflowDescriptor descriptor(WorkflowRef ref) {
            return new WorkflowDescriptor(id, input, startedAt, ref);
        }
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
