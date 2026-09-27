package io.stintflow.core;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.InterpretResult;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.core.model.CallRemoteNode;
import io.stintflow.core.model.TaskNode;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.SaveOutcome;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.TaskTransport;
import io.stintflow.spi.TimerRequest;
import io.stintflow.spi.TimerService;
import io.stintflow.spi.Wait;
import io.stintflow.spi.WorkflowRef;

/**
 * The cloud-blind orchestrator.
 * <p>
 * It walks a {@link WorkflowDefinition} tree one local activation at a time (SDD 1.1): a
 * {@link TreeInterpreter} runs {@code set}/{@code switch}/{@code do} nodes locally until it either
 * reaches a {@code call: remote} node — at which point this class checkpoints the instance as
 * WAITING, arms a timeout and dispatches over the {@link TaskTransport}, then <em>suspends</em> — or
 * the tree completes/fails.
 * <p>
 * SDD 1.2: every checkpoint is one conditional {@link StateStore#save} — version-checked, and
 * atomically adding/consuming {@link Wait} rows. Resuming (result arrives) is
 * {@code findWait → load → apply → save}; on {@link SaveOutcome#CONFLICT} (a duplicate delivery, or
 * a race with another engine) it reloads and retries with a short backoff, up to
 * {@value #MAX_RETRY_ATTEMPTS} times — never leaving the instance in an ambiguous state (sec. 8a/8d).
 * <p>
 * Crucially, this class imports <strong>no cloud SDK</strong> — only the SPI. That invariant is
 * enforced by an ArchUnit test.
 */
public final class WorkflowEngine {

    /** TODO(timeout-fire): the timer is armed and cancelled here, but the fire→retry/fail path is a
     *  follow-up increment (SDD 1.3). It needs a TimerService.onFire callback port wired to a retry/fail path. */
    private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(5);

    /** SDD 1.2, sec. 8d: retry budget for a save() that lost a version/wait race. */
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

    private final Map<String, CompletableFuture<JsonNode>> completions = new ConcurrentHashMap<>();

    public WorkflowEngine(WorkflowRegistry registry, TaskTransport transport, StateStore state,
                          TimerService timer, BlobStore blob) {
        this(registry, transport, state, timer, blob, new TreeInterpreter(new JqExpressionEvaluator()));
    }

    /** @param interpreter override point for tests (e.g. a lower local node limit for CA5). */
    public WorkflowEngine(WorkflowRegistry registry, TaskTransport transport, StateStore state,
                          TimerService timer, BlobStore blob, TreeInterpreter interpreter) {
        this.registry = registry;
        this.transport = transport;
        this.state = state;
        this.timer = timer;
        this.blob = blob;
        this.interpreter = interpreter;
        this.transport.onResult(this::onResult);
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

    // --- resume path: findWait -> load -> apply -> save, retrying on CONFLICT ----------------------

    private CompletionStage<Void> onResult(TaskResult result) {
        String waitKey = WaitKeys.task(result.correlationId());
        return timer.cancel(result.correlationId()).thenCompose(v -> resumeWithRetry(waitKey, result, 1));
    }

    private CompletionStage<Void> resumeWithRetry(String waitKey, TaskResult result, int attempt) {
        return state.findWait(waitKey).thenCompose(optWait -> {
            if (optWait.isEmpty()) {
                return done(); // unknown, already consumed by a winner, or a stale duplicate
            }
            String instanceId = optWait.get().instanceId();
            return state.load(instanceId).thenCompose(optSnap -> {
                if (optSnap.isEmpty() || !waitKey.equals(optSnap.get().waitingKey())) {
                    return done(); // stale: someone else already moved this instance on
                }
                InstanceSnapshot snap = optSnap.get();
                return advance(snap, result, waitKey)
                        .thenCompose(outcome -> afterAttempt(waitKey, result, attempt, outcome))
                        .exceptionally(ex -> {
                            failExistingInstance(instanceId, ex);
                            return null;
                        });
            });
        });
    }

    private CompletionStage<Void> afterAttempt(String waitKey, TaskResult result, int attempt, SaveOutcome outcome) {
        if (outcome == SaveOutcome.OK) {
            return done();
        }
        if (attempt >= MAX_RETRY_ATTEMPTS) {
            return handleExhaustedRetries(waitKey);
        }
        return delay(backoffMillis(attempt)).thenCompose(v -> resumeWithRetry(waitKey, result, attempt + 1));
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
                        snap.position(), null, snap.context(), InstanceStatus.FAILED, snap.version() + 1,
                        Instant.now());
                return state.save(failedSnap, snap.version(), List.of(), List.of(waitKey))
                        .thenAccept(outcome -> completeExceptionally(snap.instanceId(), new RuntimeException(
                                "State store write conflict persisted after " + MAX_RETRY_ATTEMPTS
                                        + " attempts on wait key " + waitKey)));
            });
        });
    }

    /** Applies a task result to the snapshot it was found waiting on; returns the save outcome. */
    private CompletionStage<SaveOutcome> advance(InstanceSnapshot snap, TaskResult result, String waitKey) {
        WorkflowDefinition def = registry.find(snap.definition()).orElseThrow();
        String instanceId = snap.instanceId();

        if (result.status() == TaskResult.Status.FAILED) {
            return commitTerminal(instanceId, snap.definition(), snap.position(), snap.context(),
                    InstanceStatus.FAILED, snap.version(), List.of(waitKey),
                    () -> completeExceptionally(instanceId, new RuntimeException("Task failed: " + result.error())));
        }

        TaskNode node = def.at(snap.position());
        if (!(node instanceof CallRemoteNode remote)) {
            return commitTerminal(instanceId, snap.definition(), snap.position(), snap.context(),
                    InstanceStatus.FAILED, snap.version(), List.of(waitKey),
                    () -> completeExceptionally(instanceId, new IllegalStateException(
                            "Instance " + instanceId + " was waiting at a non-remote node: " + snap.position())));
        }

        return ClaimCheck.rehydrate(result.output(), blob).thenCompose(rawOutput -> {
            InterpretResult next = interpreter.resume(def, snap.position(), remote, rawOutput, snap.context());
            return commit(instanceId, def, next, snap.version(), List.of(waitKey));
        });
    }

    // --- persisting an InterpretResult: Suspend / Complete / Failed --------------------------------

    private CompletionStage<SaveOutcome> commit(String instanceId, WorkflowDefinition def, InterpretResult result,
            long expectedVersion, List<String> consumeWaitKeys) {
        return switch (result) {
            case InterpretResult.Suspend s -> commitSuspend(instanceId, def, s, expectedVersion, consumeWaitKeys);
            case InterpretResult.Complete c -> commitTerminal(instanceId, def.ref(), c.pointer(), c.context(),
                    InstanceStatus.COMPLETED, expectedVersion, consumeWaitKeys,
                    () -> complete(instanceId, c.context()));
            case InterpretResult.Failed f -> commitTerminal(instanceId, def.ref(), f.pointer(), f.context(),
                    InstanceStatus.FAILED, expectedVersion, consumeWaitKeys,
                    () -> completeExceptionally(instanceId, new RuntimeException(f.message())));
        };
    }

    private CompletionStage<SaveOutcome> commitSuspend(String instanceId, WorkflowDefinition def,
            InterpretResult.Suspend s, long expectedVersion, List<String> consumeWaitKeys) {
        CallRemoteNode node = s.node();
        String correlationId = UUID.randomUUID().toString();
        String newWaitKey = WaitKeys.task(correlationId);

        return ClaimCheck.offload(s.dispatchInput(), transport.capabilities(), blob, instanceId + "/" + node.name())
                .thenCompose(wireInput -> {
                    InstanceSnapshot snap = new InstanceSnapshot(instanceId, def.ref(), s.pointer(), newWaitKey,
                            s.context(), InstanceStatus.WAITING, expectedVersion + 1, Instant.now());
                    Wait newWait = new Wait(newWaitKey, instanceId, s.pointer(), Instant.now());
                    TaskInvocation inv = new TaskInvocation(instanceId, s.pointer(), correlationId, def.ref(),
                            node.routingKey(), wireInput, 1);

                    return state.save(snap, expectedVersion, List.of(newWait), consumeWaitKeys)
                            .thenCompose(outcome -> {
                                if (outcome != SaveOutcome.OK) {
                                    return CompletableFuture.completedFuture(outcome);
                                }
                                return timer.schedule(new TimerRequest(correlationId, instanceId, correlationId,
                                                Instant.now().plus(DEFAULT_TIMEOUT)))
                                        .thenCompose(v -> transport.dispatch(inv))
                                        .thenApply(v -> outcome);
                            });
                });
    }

    private CompletionStage<SaveOutcome> commitTerminal(String instanceId, WorkflowRef ref, String position,
            JsonNode context, InstanceStatus status, long expectedVersion, List<String> consumeWaitKeys,
            Runnable onSuccess) {
        InstanceSnapshot snap = new InstanceSnapshot(instanceId, ref, position, null, context, status,
                expectedVersion + 1, Instant.now());
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
                    null, snap.context(), InstanceStatus.FAILED, snap.version() + 1, Instant.now());
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
