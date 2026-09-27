package io.stintflow.core;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.InterpretResult;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.core.model.CallRemoteNode;
import io.stintflow.core.model.TaskNode;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.TaskTransport;
import io.stintflow.spi.TimerRequest;
import io.stintflow.spi.TimerService;
import io.stintflow.spi.WorkflowRef;

/**
 * The cloud-blind orchestrator.
 * <p>
 * It walks a {@link WorkflowDefinition} tree one local activation at a time (SDD 1.1): a
 * {@link TreeInterpreter} runs {@code set}/{@code switch}/{@code do} nodes locally until it either
 * reaches a {@code call: remote} node — at which point this class checkpoints the instance as
 * WAITING, arms a timeout and dispatches over the {@link TaskTransport}, then <em>suspends</em> — or
 * the tree completes/fails. On a result, it rehydrates any claim-check pointer and resumes the
 * interpreter from the cursor recorded in {@link InstanceSnapshot#position()}.
 * <p>
 * Crucially, this class imports <strong>no cloud SDK</strong> — only the SPI. That invariant is
 * enforced by an ArchUnit test.
 */
public final class WorkflowEngine {

    /** TODO(timeout-fire): the timer is armed and cancelled here, but the fire→retry/fail path is a
     *  follow-up increment (SDD 1.3). It needs a TimerService.onFire callback port wired to a retry/fail path. */
    private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(5);

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
        proceed(instanceId, def, interpreter.run(def, WorkflowDefinition.ROOT_POINTER, input, input));
        return instanceId;
    }

    /** Convenience for tests/sync callers: start and complete with the final {@code $context}. */
    public CompletionStage<JsonNode> startAndWait(WorkflowRef ref, JsonNode input) {
        return completions.get(start(ref, input));
    }

    private CompletionStage<Void> proceed(String instanceId, WorkflowDefinition def, InterpretResult result) {
        return switch (result) {
            case InterpretResult.Suspend s -> suspend(instanceId, def, s);
            case InterpretResult.Complete c -> completeInstance(instanceId, def, c);
            case InterpretResult.Failed f -> failNewInstance(instanceId, def, f);
        };
    }

    private CompletionStage<Void> suspend(String instanceId, WorkflowDefinition def, InterpretResult.Suspend s) {
        CallRemoteNode node = s.node();
        String correlationId = UUID.randomUUID().toString();

        return ClaimCheck.offload(s.dispatchInput(), transport.capabilities(), blob, instanceId + "/" + node.name())
                .thenCompose(wireInput -> {
                    InstanceSnapshot snap = new InstanceSnapshot(instanceId, def.ref(), s.pointer(),
                            correlationId, s.context(), InstanceStatus.WAITING, Instant.now());
                    TaskInvocation inv = new TaskInvocation(instanceId, s.pointer(), correlationId, def.ref(),
                            node.routingKey(), wireInput, 1);
                    return state.save(snap)
                            .thenCompose(v -> timer.schedule(new TimerRequest(correlationId, instanceId,
                                    correlationId, Instant.now().plus(DEFAULT_TIMEOUT))))
                            .thenCompose(v -> transport.dispatch(inv));
                })
                .exceptionally(ex -> {
                    failExistingInstance(instanceId, ex);
                    return null;
                });
    }

    private CompletionStage<Void> completeInstance(String instanceId, WorkflowDefinition def, InterpretResult.Complete c) {
        InstanceSnapshot snap = new InstanceSnapshot(instanceId, def.ref(), c.pointer(), null, c.context(),
                InstanceStatus.COMPLETED, Instant.now());
        return state.save(snap)
                .thenAccept(v -> complete(instanceId, c.context()))
                .exceptionally(ex -> {
                    completeExceptionally(instanceId, ex);
                    return null;
                });
    }

    /** RNF2/CA5: a local-only failure (e.g. the local node limit) has no prior snapshot — write one so the FAILED status is durable, not just an in-memory rejection. */
    private CompletionStage<Void> failNewInstance(String instanceId, WorkflowDefinition def, InterpretResult.Failed f) {
        InstanceSnapshot snap = new InstanceSnapshot(instanceId, def.ref(), f.pointer(), null, f.context(),
                InstanceStatus.FAILED, Instant.now());
        return state.save(snap)
                .thenAccept(v -> completeExceptionally(instanceId, new RuntimeException(f.message())))
                .exceptionally(ex -> {
                    completeExceptionally(instanceId, ex);
                    return null;
                });
    }

    private CompletionStage<Void> onResult(TaskResult result) {
        return state.instanceWaitingFor(result.correlationId()).thenCompose(optId -> {
            if (optId.isEmpty()) {
                return done(); // unknown or already-handled correlation (at-least-once duplicate)
            }
            String instanceId = optId.get();
            return state.load(instanceId).thenCompose(optSnap -> {
                if (optSnap.isEmpty() || !result.correlationId().equals(optSnap.get().waitingForCorrelationId())) {
                    return done(); // stale/duplicate result
                }
                InstanceSnapshot snap = optSnap.get();
                return timer.cancel(snap.waitingForCorrelationId())
                        .thenCompose(v -> advance(instanceId, snap, result));
            });
        });
    }

    private CompletionStage<Void> advance(String instanceId, InstanceSnapshot snap, TaskResult result) {
        WorkflowDefinition def = registry.find(snap.definition()).orElseThrow();

        if (result.status() == TaskResult.Status.FAILED) {
            return state.save(snap.withStatus(InstanceStatus.FAILED, snap.context()))
                    .thenAccept(v -> completeExceptionally(instanceId,
                            new RuntimeException("Task failed: " + result.error())));
        }

        TaskNode node = def.at(snap.position());
        if (!(node instanceof CallRemoteNode remote)) {
            return state.save(snap.withStatus(InstanceStatus.FAILED, snap.context()))
                    .thenAccept(v -> completeExceptionally(instanceId, new IllegalStateException(
                            "Instance " + instanceId + " was waiting at a non-remote node: " + snap.position())));
        }

        return ClaimCheck.rehydrate(result.output(), blob).thenCompose(rawOutput -> {
            InterpretResult next = interpreter.resume(def, snap.position(), remote, rawOutput, snap.context());
            return proceed(instanceId, def, next);
        });
    }

    private void complete(String instanceId, JsonNode context) {
        completions.computeIfAbsent(instanceId, k -> new CompletableFuture<>()).complete(context);
    }

    private void completeExceptionally(String instanceId, Throwable t) {
        completions.computeIfAbsent(instanceId, k -> new CompletableFuture<>()).completeExceptionally(t);
    }

    /** A previously-checkpointed instance failed at an I/O step (offload/save/timer/dispatch). */
    private void failExistingInstance(String instanceId, Throwable t) {
        state.load(instanceId).thenAccept(opt -> opt.ifPresent(
                snap -> state.save(snap.withStatus(InstanceStatus.FAILED, snap.context()))));
        completeExceptionally(instanceId, t);
    }

    private static CompletionStage<Void> done() {
        return CompletableFuture.completedFuture(null);
    }
}
