package io.stintflow.worker;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.spi.Lineage;
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
 */
public record TaskContext(
        String taskId,
        String instanceId,
        String correlationId,
        int attempt,
        JsonNode input,
        WorkflowRef definition,
        String chainId,
        Lineage lineage) {

    /** The pre-SDD-2.5 shape, without chain or lineage. */
    public TaskContext(String taskId, String instanceId, String correlationId, int attempt, JsonNode input,
                       WorkflowRef definition) {
        this(taskId, instanceId, correlationId, attempt, input, definition, null, Lineage.NONE);
    }
}
