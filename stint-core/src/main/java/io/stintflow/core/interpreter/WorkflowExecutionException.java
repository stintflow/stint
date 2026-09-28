package io.stintflow.core.interpreter;

/** A structural problem with a workflow definition, discovered while interpreting it (e.g. an unmatched {@code switch}). */
public final class WorkflowExecutionException extends RuntimeException {

    public WorkflowExecutionException(String message) {
        super(message);
    }
}
