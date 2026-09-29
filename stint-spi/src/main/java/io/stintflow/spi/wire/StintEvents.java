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
    /** Timer -> engine: a scheduled timeout/delay fired. */
    public static final String TYPE_TIMER_FIRE = "io.stintflow.timer.fire.v1";

    // CloudEvents extension attributes (lowercase, per the CloudEvents spec naming rules).
    public static final String EXT_CORRELATION_ID = "correlationid";
    public static final String EXT_WORKFLOW_INSTANCE_ID = "workflowinstanceid";
    public static final String EXT_TASK_ID = "taskid";
    public static final String EXT_ATTEMPT = "attempt";
    public static final String EXT_DEFINITION = "definition";
    /** {@link #TYPE_TIMER_FIRE} extension: the timerId, which is also the {@code Wait} key it guards (SDD 1.3). */
    public static final String EXT_TIMER_ID = "timerid";

    public static final String CONTENT_TYPE_JSON = "application/json";

    /**
     * SDD 2.2, RNF1/sec. 8e: type prefixes of the internal protocol. A domain fact ({@code emit}) may
     * never use them — the domain channel must not carry, or be mistaken for, engine traffic.
     */
    public static final List<String> RESERVED_TYPE_PREFIXES =
            List.of("io.stintflow.task.", "io.stintflow.timer.");

    /** Whether {@code type} belongs to the internal protocol ({@link #RESERVED_TYPE_PREFIXES}). */
    public static boolean isReservedType(String type) {
        return type != null && RESERVED_TYPE_PREFIXES.stream().anyMatch(type::startsWith);
    }
}
