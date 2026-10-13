package io.stintflow.spi.wire;

import java.util.List;

/**
 * The wire contract — CloudEvents type names and extension attributes shared by the orchestrator
 * and every worker, in every language and on every transport. This is the real interop surface.
 */
public final class StintEvents {

    private StintEvents() {
    }

    /** Engine -> worker: a task to execute. */
    public static final String TYPE_TASK_INVOKE = "io.stintflow.task.invoke.v1";
    /** Worker -> engine: the result of a task. */
    public static final String TYPE_TASK_RESULT = "io.stintflow.task.result.v1";
    /**
     * Timer -> engine: a scheduled timeout/delay fired. Reserved: timer fires travel in the timer
     * connector's own format ({@code {timerId, workflowInstanceId}}), not as CloudEvents (SDD 2.5, decision 2).
     */
    public static final String TYPE_TIMER_FIRE = "io.stintflow.timer.fire.v1";

    // CloudEvents extension attributes (lowercase, per the CloudEvents spec naming rules).
    /**
     * Correlation of an <em>attempt</em>: identifies one dispatch (one attempt of one task) and matches its
     * result back. Not a business correlation — the business chain is {@link #EXT_CHAIN_ID} (SDD 2.5).
     */
    public static final String EXT_CORRELATION_ID = "correlationid";
    public static final String EXT_WORKFLOW_INSTANCE_ID = "workflowinstanceid";
    public static final String EXT_TASK_ID = "taskid";
    public static final String EXT_ATTEMPT = "attempt";
    public static final String EXT_DEFINITION = "definition";
    /** {@link #TYPE_TIMER_FIRE} extension: the timerId, which is also the {@code Wait} key it guards (SDD 1.3). */
    public static final String EXT_TIMER_ID = "timerid";

    /** SDD 2.5: root of the business chain — identical on every event along the path, across workflows. */
    public static final String EXT_CHAIN_ID = "chainid";
    /**
     * SDD 2.5: id of the event that directly caused this one. For events created by an activation resumed by
     * a timer, it is the {@code timerId} — a timer fire, which is not a CloudEvent on the wire.
     */
    public static final String EXT_CAUSATION_ID = "causationid";
    /** CloudEvents Distributed Tracing extension (W3C Trace Context). */
    public static final String EXT_TRACEPARENT = "traceparent";
    public static final String EXT_TRACESTATE = "tracestate";
    /**
     * SDD 3a.3c, on a task invoke only: when the attempt's timeout fires, as an RFC 3339 instant in UTC. Optional: an
     * invoke without it (or with an unreadable one) has no known deadline.
     */
    public static final String EXT_DEADLINE = "stintdeadline";
    /**
     * SDD 3a.5, RF2: the level of the data ({@code internal}, {@code confidential}, {@code restricted}) — on a task
     * invoke, on the events an instance emits, and read on the events that start or resume one. Optional; a value
     * that isn't a level reads as {@code restricted}.
     */
    public static final String EXT_SENSITIVITY = "stintsensitivity";
    /** SDD 3a.5, RF8: on a task result, the AI provider that produced it (for the audit trail), if the worker says. */
    public static final String EXT_PROVIDER = "stintprovider";

    public static final String CONTENT_TYPE_JSON = "application/json";

    /**
     * SDD 2.2, RNF1/sec. 8e (SDD 2.4 adds {@code io.stintflow.schedule.}): type prefixes of the internal protocol. A domain fact ({@code emit}) may
     * never use them — the domain channel must not carry, or be mistaken for, engine traffic.
     */
    public static final List<String> RESERVED_TYPE_PREFIXES =
            List.of("io.stintflow.task.", "io.stintflow.timer.", "io.stintflow.schedule.");

    /**
     * SDD 2.4, sec. 8e (Stint extension): the synthetic event a schedule occurrence starts an instance with.
     * Internal to the start — never published on a bus.
     */
    public static final String TYPE_SCHEDULE_FIRED = "io.stintflow.schedule.fired.v1";

    /** Whether {@code type} belongs to the internal protocol ({@link #RESERVED_TYPE_PREFIXES}). */
    public static boolean isReservedType(String type) {
        return type != null && RESERVED_TYPE_PREFIXES.stream().anyMatch(type::startsWith);
    }
}
