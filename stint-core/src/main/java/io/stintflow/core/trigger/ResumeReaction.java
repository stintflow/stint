package io.stintflow.core.trigger;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

import io.cloudevents.CloudEvent;
import io.stintflow.core.WorkflowEngine;

/**
 * Resumes instances waiting in a {@code listen} (SDD 2.3, RF4) — the second reaction of the single domain
 * consumer, beside {@link StartReaction}. See {@link WorkflowEngine#onDomainEvent}: an event with a wait
 * resumes it; one that arrived too early is kept for {@code earlyEventWindow} (sec. 8c). Completes only once
 * the resume, the partial or the inbox entry is recorded, so the source acks after the effect.
 */
public final class ResumeReaction implements DomainEventReaction {

    /** Sec. 8c: {@code stint.listen.early-event-window}'s default. */
    public static final Duration DEFAULT_EARLY_EVENT_WINDOW = Duration.ofMinutes(5);
    /** Sec. 8c: the window is short by design. */
    public static final Duration MAX_EARLY_EVENT_WINDOW = Duration.ofMinutes(15);

    private final WorkflowEngine engine;
    private final Duration earlyEventWindow;

    public ResumeReaction(WorkflowEngine engine) {
        this(engine, DEFAULT_EARLY_EVENT_WINDOW);
    }

    public ResumeReaction(WorkflowEngine engine, Duration earlyEventWindow) {
        if (earlyEventWindow.isNegative() || earlyEventWindow.compareTo(MAX_EARLY_EVENT_WINDOW) > 0) {
            throw new IllegalArgumentException("The early-event window must be between 0 and "
                    + MAX_EARLY_EVENT_WINDOW + ", got " + earlyEventWindow);
        }
        this.engine = engine;
        this.earlyEventWindow = earlyEventWindow;
    }

    @Override
    public CompletionStage<Reacted> react(CloudEvent event) {
        return engine.onDomainEvent(event, earlyEventWindow)
                .thenApply(concerned -> concerned ? Reacted.MATCHED : Reacted.IGNORED);
    }
}
