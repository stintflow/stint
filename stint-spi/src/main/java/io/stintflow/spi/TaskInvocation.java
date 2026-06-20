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
 * @param correlationId      unique id used to match the result back to the suspended instance
 * @param definition         which workflow definition this came from
 * @param routingKey         transport routing hint (becomes the CloudEvent {@code subject})
 * @param input              the task input payload (may be a claim-check pointer if offloaded)
 * @param attempt            1-based attempt counter, for idempotency and retries
 */
public record TaskInvocation(
        String workflowInstanceId,
        String taskId,
        String correlationId,
        WorkflowRef definition,
        String routingKey,
        JsonNode input,
        int attempt) {
}
