package io.stintflow.worker;

import java.time.Instant;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.spi.Lineage;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.WorkflowRef;

/**
 * Everything a task handler needs, decoded from the invoke CloudEvent and with any claim-check
 * pointer already rehydrated. The handler is identical whether it runs on Lambda, Knative or a pool.
 * <p>
 * SDD 1.5, RF3: {@code definition} was already on the wire (the CloudEvent {@code definition}
 * extension, {@code namespace:name:version}) — {@link TaskInvocation}'s {@code definition()} always
 * carried it, {@link WorkerRuntime} just used to drop it when building this record. No wire change
 * was needed; this is purely on the deserializing side.
 *
 * @param taskId        the position/node in the workflow definition (see
 *                      {@link io.stintflow.spi.TaskInvocation#taskId()}) — <b>not</b> a task-type id
 * @param instanceId    identifies the running workflow <em>instance</em> (the execution) — stable
 *                      across every task dispatched within it
 * @param correlationId correlation of this <em>attempt</em>: identifies this specific dispatch — a retried task gets a new
 *                      correlationId each attempt (SDD 1.3), while instanceId stays the same
 * @param attempt       1-based attempt counter, for idempotency and retries
 * @param input         the task input payload, already rehydrated past any claim-check pointer
 * @param definition    which workflow definition (namespace, name, exact version) this task came
 *                      from — the exact version the running instance started with (SDD 1.1), not
 *                      necessarily the latest registered one
 * @param chainId       root of the business chain this execution belongs to (SDD 2.5) — the id to log,
 *                      or to put on anything the handler publishes itself; {@code null} from a pre-2.5 engine
 * @param lineage       this execution's lineage: the chain, its cause (the invoke's id) and the worker
 *                      span's trace context, for creating child spans or forwarding (SDD 2.5)
 * @param deadline      when this attempt's timeout fires (SDD 3a.3c): the engine discards a result arriving later, so
 *                      a handler may stop, and should record nothing, past it; {@code null} when unknown
 * @param sensitivity   the instance's level when the task was dispatched (SDD 3a.5): what the worker's policy decides
 *                      the provider on; {@code null} from an engine without a policy — the policy reads it as restricted
 * @param report        where the handler reports the provider that produced its output (SDD 3a.5, RF8); never null
 */
public record TaskContext(
        String taskId,
        String instanceId,
        String correlationId,
        int attempt,
        JsonNode input,
        WorkflowRef definition,
        String chainId,
        Lineage lineage,
        Instant deadline,
        Sensitivity sensitivity,
        TaskReport report) {

    public TaskContext {
        report = report == null ? new TaskReport() : report;
    }

    /** The pre-SDD-3a.5 shape, without a level or a report. */
    public TaskContext(String taskId, String instanceId, String correlationId, int attempt, JsonNode input,
                       WorkflowRef definition, String chainId, Lineage lineage, Instant deadline) {
        this(taskId, instanceId, correlationId, attempt, input, definition, chainId, lineage, deadline, null, null);
    }

    /** The pre-SDD-3a.3c shape, without a deadline. */
    public TaskContext(String taskId, String instanceId, String correlationId, int attempt, JsonNode input,
                       WorkflowRef definition, String chainId, Lineage lineage) {
        this(taskId, instanceId, correlationId, attempt, input, definition, chainId, lineage, null, null, null);
    }

    /** The pre-SDD-2.5 shape, without chain or lineage. */
    public TaskContext(String taskId, String instanceId, String correlationId, int attempt, JsonNode input,
                       WorkflowRef definition) {
        this(taskId, instanceId, correlationId, attempt, input, definition, null, Lineage.NONE, null, null, null);
    }
}
