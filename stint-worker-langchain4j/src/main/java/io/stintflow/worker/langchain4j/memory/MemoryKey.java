package io.stintflow.worker.langchain4j.memory;

/**
 * Whose conversation a memory is (SDD 3a.3, 5.3): a workflow instance, optionally narrowed to one task.
 *
 * @param instanceId the workflow instance — every task of it sees the same conversation by default
 * @param scope      {@code ""} for the whole instance, or the {@code taskId} of a task with a conversation of its own
 */
public record MemoryKey(String instanceId, String scope) {

    public static final String INSTANCE_SCOPE = "";

    public MemoryKey {
        if (instanceId == null || instanceId.isBlank()) {
            throw new IllegalArgumentException("instanceId is required");
        }
        scope = scope == null ? INSTANCE_SCOPE : scope;
    }

    public static MemoryKey instance(String instanceId) {
        return new MemoryKey(instanceId, INSTANCE_SCOPE);
    }

    public static MemoryKey task(String instanceId, String taskId) {
        return new MemoryKey(instanceId, taskId);
    }
}
