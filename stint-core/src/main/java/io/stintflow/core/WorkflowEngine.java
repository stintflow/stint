package io.stintflow.core;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.spi.BlobStore;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.TimerRequest;
import io.stintflow.spi.TimerService;
import io.stintflow.spi.TaskTransport;
import io.stintflow.spi.WorkflowRef;

/**
 * The cloud-blind orchestrator.
 * <p>
 * It walks a {@link WorkflowDefinition} one remote step at a time: for each step it builds a
 * {@link TaskInvocation}, checkpoints the instance as WAITING, arms a timeout, dispatches over the
 * {@link TaskTransport}, and then <em>suspends</em> — nothing runs until a {@link TaskResult} comes
 * back. On the result it rehydrates any claim-check pointer, merges the output, and dispatches the
 * next step (or completes).
 * <p>
 * Crucially, this class imports <strong>no cloud SDK</strong> — only the SPI. That invariant is
 * enforced by an ArchUnit test.
 */
public final class WorkflowEngine {

    /** TODO(timeout-fire): the timer is armed and cancelled here, but the fire→retry/fail path is a
     *  follow-up increment. It needs a TimerService.onFire callback port wired to {@link #onTimeout}. */
    private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(5);

    private final WorkflowRegistry registry;
    private final TaskTransport transport;
    private final StateStore state;
    private final TimerService timer;
    private final BlobStore blob;

    private final Map<String, CompletableFuture<JsonNode>> completions = new ConcurrentHashMap<>();

    public WorkflowEngine(WorkflowRegistry registry, TaskTransport transport, StateStore state,
                          TimerService timer, BlobStore blob) {
        this.registry = registry;
        this.transport = transport;
        this.state = state;
        this.timer = timer;
        this.blob = blob;
        this.transport.onResult(this::onResult);
    }

    /** Start an instance and return its id. The instance runs asynchronously, event by event. */
    public String start(WorkflowRef ref, JsonNode input) {
        WorkflowDefinition def = registry.find(ref)
                .orElseThrow(() -> new IllegalArgumentException("Unknown workflow: " + ref.canonical()));
        String instanceId = UUID.randomUUID().toString();
        completions.computeIfAbsent(instanceId, k -> new CompletableFuture<>());
        dispatchStep(instanceId, def, 0, input);
        return instanceId;
    }

    /** Convenience for tests/sync callers: start and complete with the final context. */
    public CompletionStage<JsonNode> startAndWait(WorkflowRef ref, JsonNode input) {
        return completions.get(start(ref, input));
    }

    private void dispatchStep(String instanceId, WorkflowDefinition def, int stepIndex, JsonNode context) {
        RemoteStep step = def.step(stepIndex);
        JsonNode taskInput = step.inputMapper().apply(context);
        String correlationId = UUID.randomUUID().toString();

        ClaimCheck.offload(taskInput, transport.capabilities(), blob, instanceId + "/" + step.id())
                .thenCompose(wireInput -> {
                    InstanceSnapshot snap = new InstanceSnapshot(instanceId, def.ref(),
                            String.valueOf(stepIndex), correlationId, context, InstanceStatus.WAITING, Instant.now());
                    TaskInvocation inv = new TaskInvocation(instanceId, step.id(), correlationId, def.ref(),
                            step.routingKey(), wireInput, 1);
                    return state.save(snap)
                            .thenCompose(v -> timer.schedule(new TimerRequest(correlationId, instanceId,
                                    correlationId, Instant.now().plus(DEFAULT_TIMEOUT))))
                            .thenCompose(v -> transport.dispatch(inv));
                })
                .exceptionally(ex -> {
                    failInstance(instanceId, ex);
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
        int stepIndex = Integer.parseInt(snap.position());

        if (result.status() == TaskResult.Status.FAILED) {
            return state.save(snap.withStatus(InstanceStatus.FAILED, snap.context()))
                    .thenAccept(v -> completeExceptionally(instanceId,
                            new RuntimeException("Task failed: " + result.error())));
        }

        RemoteStep step = def.step(stepIndex);
        return ClaimCheck.rehydrate(result.output(), blob).thenCompose(output -> {
            JsonNode newContext = step.outputMerger().apply(snap.context(), output);
            if (def.isLast(stepIndex)) {
                return state.save(snap.withStatus(InstanceStatus.COMPLETED, newContext))
                        .thenAccept(v -> complete(instanceId, newContext));
            }
            dispatchStep(instanceId, def, stepIndex + 1, newContext);
            return done();
        });
    }

    private void complete(String instanceId, JsonNode context) {
        completions.computeIfAbsent(instanceId, k -> new CompletableFuture<>()).complete(context);
    }

    private void completeExceptionally(String instanceId, Throwable t) {
        completions.computeIfAbsent(instanceId, k -> new CompletableFuture<>()).completeExceptionally(t);
    }

    private void failInstance(String instanceId, Throwable t) {
        state.load(instanceId).thenAccept(opt -> opt.ifPresent(
                snap -> state.save(snap.withStatus(InstanceStatus.FAILED, snap.context()))));
        completeExceptionally(instanceId, t);
    }

    private static CompletionStage<Void> done() {
        return CompletableFuture.completedFuture(null);
    }
}
