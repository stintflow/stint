package io.stintflow.spi;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * A unit of work the orchestrator hands off to a (possibly remote, possibly ephemeral) worker.
 * <p>
 * Serialized onto the wire as a {@code io.stintflow.task.invoke.v1} CloudEvent. The orchestrator suspends
 * after dispatching one of these and only resumes when a matching {@link TaskResult} comes back,
 * correlated by {@link #correlationId()}.
 *
 * @param workflowInstanceId the running instance this task belongs to
 * @param taskId             the position/node in the workflow definition
 * @param correlationId      correlation of this <em>attempt</em>: unique per dispatch, used to match the
 *                           result back to the suspended instance (the business chain is {@link Lineage#chainId()})
 * @param definition         which workflow definition this came from
 * @param routingKey         transport routing hint (becomes the CloudEvent {@code subject})
 * @param input              the task input payload (may be a claim-check pointer if offloaded)
 * @param attempt            1-based attempt counter, for idempotency and retries
 * @param lineage            chain, cause and trace context (SDD 2.5); {@link Lineage#NONE} when absent
 */
public record TaskInvocation(
        String workflowInstanceId,
        String taskId,
        String correlationId,
        WorkflowRef definition,
        String routingKey,
        JsonNode input,
        int attempt,
        Lineage lineage) {

    public TaskInvocation {
        lineage = lineage == null ? Lineage.NONE : lineage;
    }

    /** An invocation with no lineage (the pre-SDD-2.5 shape). */
    public TaskInvocation(String workflowInstanceId, String taskId, String correlationId, WorkflowRef definition,
                          String routingKey, JsonNode input, int attempt) {
        this(workflowInstanceId, taskId, correlationId, definition, routingKey, input, attempt, Lineage.NONE);
    }
}
