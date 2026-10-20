package io.stintflow.core;

/** SDD 3a.11: what {@link WorkflowEngine#cancel} did. */
public enum CancelOutcome {
    /** The instance was suspended or pending and is now {@code ABORTED}, its waits consumed. */
    CANCELLED,
    /** The instance had already completed, failed or been cancelled; nothing was changed. */
    ALREADY_TERMINAL,
    /** No instance has that id. */
    NOT_FOUND
}
