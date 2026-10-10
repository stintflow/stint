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
 * @param lineage       chain, cause (the invoke's id) and the worker's trace context (SDD 2.5)
 */
public record TaskResult(
        String correlationId,
        Status status,
        JsonNode output,
        ErrorInfo error,
        Lineage lineage) {

    public enum Status {COMPLETED, FAILED}

    public TaskResult {
        lineage = lineage == null ? Lineage.NONE : lineage;
    }

    /** A result with no lineage (the pre-SDD-2.5 shape). */
    public TaskResult(String correlationId, Status status, JsonNode output, ErrorInfo error) {
        this(correlationId, status, output, error, Lineage.NONE);
    }

    public TaskResult withLineage(Lineage lineage) {
        return new TaskResult(correlationId, status, output, error, lineage);
    }

    public static TaskResult completed(String correlationId, JsonNode output) {
        return new TaskResult(correlationId, Status.COMPLETED, output, null);
    }

    public static TaskResult failed(String correlationId, ErrorInfo error) {
        return new TaskResult(correlationId, Status.FAILED, null, error);
    }
}
