package io.stintflow.spi.wire;

import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.TimerFire;
import io.cloudevents.CloudEvent;

/**
 * Converts Stint domain objects to/from CloudEvents. Implemented once (Jackson-backed) and reused by
 * every transport connector, so the engine and workers always speak the exact same envelope.
 */
public interface CloudEventCodec {

    CloudEvent toEvent(TaskInvocation invocation);

    CloudEvent toEvent(TaskResult result, String workflowInstanceId);

    /** SDD 1.3: encodes a {@code io.stintflow.timer.fire.v1} event. */
    CloudEvent toEvent(TimerFire fire);

    TaskInvocation toInvocation(CloudEvent event);

    TaskResult toResult(CloudEvent event);

    TimerFire toTimerFire(CloudEvent event);
}
