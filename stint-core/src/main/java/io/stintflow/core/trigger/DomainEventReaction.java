package io.stintflow.core.trigger;

import java.util.concurrent.CompletionStage;

import io.cloudevents.CloudEvent;

/**
 * One thing the engine does with an inbound domain event (SDD 2.1, RF4, sec. 8c): start bound
 * definitions ({@link StartReaction}) now, resume instances waiting in {@code listen} in SDD 2.3.
 * <p>
 * Must be idempotent — the event may be redelivered after a partial failure — and complete only once
 * its effects are durably recorded.
 */
@FunctionalInterface
public interface DomainEventReaction {

    CompletionStage<Reacted> react(CloudEvent event);

    enum Reacted {
        /** The event concerned this reaction and its effects are recorded. */
        MATCHED,
        /** The event did not concern this reaction; nothing was done. */
        IGNORED
    }
}
