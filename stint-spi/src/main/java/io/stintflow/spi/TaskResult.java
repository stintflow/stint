package io.stintflow.spi;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * The outcome of a {@link TaskInvocation}, emitted by the worker as a {@code io.stintflow.task.result.v1}
 * CloudEvent and consumed by the orchestrator to resume the suspended instance.
 *
 * @param correlationId matches the originating {@link TaskInvocation#correlationId()}
 * @param status        COMPLETED or FAILED
 * @param output        the task output when {@code status == COMPLETED} (may be a claim-check pointer)
 * @param error         failure detail when {@code status == FAILED}, otherwise {@code null}
 */
public record TaskResult(
        String correlationId,
        Status status,
        JsonNode output,
        ErrorInfo error) {

    public enum Status {COMPLETED, FAILED}

    public static TaskResult completed(String correlationId, JsonNode output) {
        return new TaskResult(correlationId, Status.COMPLETED, output, null);
    }

    public static TaskResult failed(String correlationId, ErrorInfo error) {
        return new TaskResult(correlationId, Status.FAILED, null, error);
    }
}
