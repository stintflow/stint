package io.stintflow.spi.wire;

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

    public static final String CONTENT_TYPE_JSON = "application/json";
}
