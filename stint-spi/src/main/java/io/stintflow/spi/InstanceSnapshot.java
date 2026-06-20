package io.stintflow.spi;

import java.time.Instant;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * A durable checkpoint of a workflow instance, persisted by a {@link StateStore} while the instance
 * is suspended waiting for a remote task result.
 *
 * @param instanceId             unique running-instance id
 * @param definition             the workflow definition ref
 * @param position               opaque cursor into the definition (e.g. the current task id)
 * @param waitingForCorrelationId the correlation id this instance is currently blocked on, or {@code null}
 * @param context                the accumulated workflow data/context as JSON
 * @param status                 lifecycle status
 * @param updatedAt              last mutation timestamp
 */
public record InstanceSnapshot(
        String instanceId,
        WorkflowRef definition,
        String position,
        String waitingForCorrelationId,
        JsonNode context,
        InstanceStatus status,
        Instant updatedAt) {

    public enum InstanceStatus {RUNNING, WAITING, COMPLETED, FAILED, ABORTED}

    public InstanceSnapshot withWaiting(String position, String correlationId, JsonNode context) {
        return new InstanceSnapshot(instanceId, definition, position, correlationId, context,
                InstanceStatus.WAITING, Instant.now());
    }

    public InstanceSnapshot withStatus(InstanceStatus status, JsonNode context) {
        return new InstanceSnapshot(instanceId, definition, position, null, context, status, Instant.now());
    }
}
