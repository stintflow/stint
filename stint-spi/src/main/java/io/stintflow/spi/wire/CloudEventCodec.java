package io.stintflow.spi.wire;

import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.cloudevents.CloudEvent;

/**
 * Converts Stint domain objects to/from CloudEvents. Implemented once (Jackson-backed) and reused by
 * every transport connector, so the engine and workers always speak the exact same envelope.
 * <p>
 * Timer fires are not CloudEvents: each timer connector carries them in its own format (SDD 2.5,
 * decision 2), so there is no timer codec here.
 */
public interface CloudEventCodec {

    CloudEvent toEvent(TaskInvocation invocation);

    CloudEvent toEvent(TaskResult result, String workflowInstanceId);

    TaskInvocation toInvocation(CloudEvent event);

    TaskResult toResult(CloudEvent event);
}
