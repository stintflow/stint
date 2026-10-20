package io.stintflow.worker.langchain4j;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.worker.TaskContext;
import io.stintflow.worker.langchain4j.memory.MemoryKey;

/**
 * What an {@link AiCall} gets besides the AI Service: the task, and the conversation the service's memory is bound to
 * ({@code null} without memory).
 */
public record AiTaskContext(TaskContext task, MemoryKey memory) {

    /** The task's input, already rehydrated past any claim-check pointer. */
    public JsonNode input() {
        return task.input();
    }
}
