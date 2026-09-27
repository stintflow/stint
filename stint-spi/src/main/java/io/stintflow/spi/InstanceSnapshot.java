package io.stintflow.spi;

import java.time.Instant;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * A durable checkpoint of a workflow instance, persisted by a {@link StateStore} while the instance
 * is suspended waiting for a remote task result.
 *
 * @param instanceId  unique running-instance id
 * @param definition  the workflow definition ref
 * @param position    opaque cursor into the definition (e.g. the current task id)
 * @param waitingKey  the typed wait key (SDD 1.2, RF2, e.g. {@code task:<correlationId>}) this
 *                    instance is currently blocked on, or {@code null}
 * @param context     the accumulated workflow data/context as JSON
 * @param status      lifecycle status
 * @param version     optimistic-concurrency version; {@code 0} means "not yet persisted" — the
 *                    condition {@link StateStore#save} uses to distinguish create from update
 * @param updatedAt   last mutation timestamp
 */
public record InstanceSnapshot(
        String instanceId,
        WorkflowRef definition,
        String position,
        String waitingKey,
        JsonNode context,
        InstanceStatus status,
        long version,
        Instant updatedAt) {

    public enum InstanceStatus {RUNNING, WAITING, COMPLETED, FAILED, ABORTED}
}
