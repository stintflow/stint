package io.stintflow.worker;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Everything a task handler needs, decoded from the invoke CloudEvent and with any claim-check
 * pointer already rehydrated. The handler is identical whether it runs on Lambda, Knative or a pool.
 */
public record TaskContext(
        String taskId,
        String instanceId,
        String correlationId,
        int attempt,
        JsonNode input) {
}
