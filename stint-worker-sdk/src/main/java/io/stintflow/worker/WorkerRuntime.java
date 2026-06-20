package io.stintflow.worker;

import java.util.concurrent.CompletionStage;

import io.stintflow.core.ClaimCheck;
import io.stintflow.core.DefaultCloudEventCodec;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.wire.CloudEventCodec;
import io.cloudevents.CloudEvent;

/**
 * Platform-neutral worker entrypoint: decode invoke CloudEvent → rehydrate claim-check → run the
 * registered {@link TaskHandler} → encode result CloudEvent. Every binding (Lambda, Knative, pool)
 * is a thin shell that feeds events into {@link #handle(CloudEvent)} and ships the returned event.
 */
public final class WorkerRuntime {

    private final CloudEventCodec codec = new DefaultCloudEventCodec();
    private final TaskHandlerRegistry handlers;
    private final BlobStore blob;

    public WorkerRuntime(TaskHandlerRegistry handlers, BlobStore blob) {
        this.handlers = handlers;
        this.blob = blob;
    }

    public CompletionStage<CloudEvent> handle(CloudEvent invokeEvent) {
        TaskInvocation inv = codec.toInvocation(invokeEvent);
        TaskHandler handler = handlers.find(inv.routingKey())
                .orElseThrow(() -> new IllegalStateException("No handler for routing key: " + inv.routingKey()));

        return ClaimCheck.rehydrate(inv.input(), blob).thenCompose(input -> {
            TaskContext ctx = new TaskContext(inv.taskId(), inv.workflowInstanceId(),
                    inv.correlationId(), inv.attempt(), input);
            return handler.execute(ctx).handle((output, ex) -> {
                TaskResult result = (ex == null)
                        ? TaskResult.completed(inv.correlationId(), output)
                        : TaskResult.failed(inv.correlationId(), ErrorInfo.of(ex));
                return codec.toEvent(result, inv.workflowInstanceId());
            });
        });
    }
}
