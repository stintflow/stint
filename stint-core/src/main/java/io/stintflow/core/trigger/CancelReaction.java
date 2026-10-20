package io.stintflow.core.trigger;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.databind.JsonNode;

import io.cloudevents.CloudEvent;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.spi.wire.StintEvents;
import io.stintflow.wire.Json;

/**
 * SDD 3a.11, 4.5: cancels the instance named by an {@link StintEvents#TYPE_INSTANCE_CANCEL} event — the third
 * reaction of the single domain consumer, beside {@link StartReaction} and {@link ResumeReaction}. The domain channel
 * is shared, so this reaction acts only on events whose {@code source} is on the list it was built with; the list has
 * no default, and an empty one refuses everything. A refused event is audited ({@code cancel}, {@code refused}) and
 * counts as handled, so it is acknowledged, not redelivered. The event never goes through a {@code listen}'s
 * correlation.
 */
public final class CancelReaction implements DomainEventReaction {

    private final WorkflowEngine engine;
    private final Set<String> allowedSources;

    /** @param allowedSources the {@code source}s trusted to cancel instances; required (empty refuses all) */
    public CancelReaction(WorkflowEngine engine, Set<String> allowedSources) {
        if (allowedSources == null) {
            throw new IllegalArgumentException("The sources allowed to cancel are required (an empty set refuses all)");
        }
        this.engine = engine;
        this.allowedSources = Set.copyOf(allowedSources);
    }

    @Override
    public CompletionStage<Reacted> react(CloudEvent event) {
        if (!StintEvents.TYPE_INSTANCE_CANCEL.equals(event.getType())) {
            return CompletableFuture.completedFuture(Reacted.IGNORED);
        }
        String subject = event.getSubject();
        String source = event.getSource() == null ? null : event.getSource().toString();
        if (source == null || !allowedSources.contains(source)) {
            engine.auditCancelRefused(subject, source);
            return CompletableFuture.completedFuture(Reacted.MATCHED);   // handled: refused, not redelivered
        }
        if (subject == null || subject.isBlank()) {
            return CompletableFuture.completedFuture(Reacted.MATCHED);   // nothing to cancel
        }
        return engine.cancel(subject, reasonOf(event), "event:" + source).thenApply(outcome -> Reacted.MATCHED);
    }

    private static String reasonOf(CloudEvent event) {
        if (event.getData() == null) {
            return "";
        }
        try {
            JsonNode data = Json.MAPPER.readTree(event.getData().toBytes());
            return data != null && data.hasNonNull("reason") ? data.get("reason").asText() : "";
        } catch (java.io.IOException | RuntimeException e) {
            return "";
        }
    }
}
