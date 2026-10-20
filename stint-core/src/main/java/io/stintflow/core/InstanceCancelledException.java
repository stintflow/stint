package io.stintflow.core;

/** SDD 3a.11: {@link WorkflowEngine#awaitCompletion} of an instance that was cancelled ({@code ABORTED}). */
public final class InstanceCancelledException extends RuntimeException {

    private final String instanceId;

    public InstanceCancelledException(String instanceId) {
        super("Instance " + instanceId + " was cancelled");
        this.instanceId = instanceId;
    }

    public String instanceId() {
        return instanceId;
    }
}
