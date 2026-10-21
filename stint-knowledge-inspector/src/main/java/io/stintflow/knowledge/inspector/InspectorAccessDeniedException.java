package io.stintflow.knowledge.inspector;

/** SDD 3b.6, RF7: the caller may not look at that base — or there is no such base; the message does not say which. */
public final class InspectorAccessDeniedException extends RuntimeException {

    public InspectorAccessDeniedException() {
        super("Not allowed");
    }
}
