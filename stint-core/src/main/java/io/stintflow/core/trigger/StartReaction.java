package io.stintflow.core.trigger;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.cloudevents.CloudEvent;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.spi.WorkflowRef;

/**
 * Starts every definition bound to the event (SDD 2.1, RF2/RF3). Each target is an independent,
 * idempotent {@link WorkflowEngine#startFromEvent}; the stage completes normally only when all of
 * them are recorded. On a partial failure the event is redelivered: targets already created are
 * no-ops, the failed ones are retried (sec. 8b).
 */
public final class StartReaction implements DomainEventReaction {

    private final TriggerBindings bindings;
    private final WorkflowEngine engine;

    public StartReaction(TriggerBindings bindings, WorkflowEngine engine) {
        this.bindings = bindings;
        this.engine = engine;
    }

    @Override
    public CompletionStage<Reacted> react(CloudEvent event) {
        List<WorkflowRef> targets = bindings.targetsFor(event);
        if (targets.isEmpty()) {
            return CompletableFuture.completedFuture(Reacted.IGNORED);
        }
        CompletableFuture<?>[] starts = targets.stream()
                .map(target -> engine.startFromEvent(target, event).toCompletableFuture())
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(starts).thenApply(v -> Reacted.MATCHED);
    }
}
