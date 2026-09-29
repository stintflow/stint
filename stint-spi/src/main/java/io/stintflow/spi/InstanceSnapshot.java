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
 * @param retryState  retry bookkeeping (SDD 1.3, RF10) while inside a {@code TryNode}, or {@code null}
 * @param updatedAt   last mutation timestamp
 * @param input       the workflow's raw input (SDD 2.1, RF6 — DSL 1.0 {@code $workflow.input}; for an
 *                    event-started instance, the array of triggering events), or {@code null} for
 *                    snapshots written before SDD 2.1
 * @param startedAt   when the instance was created (DSL 1.0 {@code $workflow.startedAt}), or {@code null}
 *                    for snapshots written before SDD 2.1
 */
public record InstanceSnapshot(
        String instanceId,
        WorkflowRef definition,
        String position,
        String waitingKey,
        JsonNode context,
        InstanceStatus status,
        long version,
        RetryState retryState,
        Instant updatedAt,
        JsonNode input,
        Instant startedAt) {

    public enum InstanceStatus {RUNNING, WAITING, COMPLETED, FAILED, ABORTED}
}
