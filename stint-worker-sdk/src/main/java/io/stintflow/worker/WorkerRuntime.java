package io.stintflow.worker;

import java.util.concurrent.CompletableFuture;
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
 * <p>
 * SDD 1.3, RF9: {@link #safeInvoke} never lets an exception escape — a missing route, a claim-check
 * failure, or the handler throwing synchronously or asynchronously all become a {@link TaskResult#failed}
 * instead of an exceptionally-completed stage nobody converts back into a result.
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
        return safeInvoke(inv).thenApply(result -> codec.toEvent(result, inv.workflowInstanceId()));
    }

    private CompletionStage<TaskResult> safeInvoke(TaskInvocation inv) {
        CompletableFuture<TaskResult> result = new CompletableFuture<>();
        try {
            TaskHandler handler = handlers.find(inv.routingKey())
                    .orElseThrow(() -> new IllegalStateException("No handler for routing key: " + inv.routingKey()));
            ClaimCheck.rehydrate(inv.input(), blob).thenCompose(input -> {
                TaskContext ctx = new TaskContext(inv.taskId(), inv.workflowInstanceId(),
                        inv.correlationId(), inv.attempt(), input);
                return handler.execute(ctx);
            }).whenComplete((output, ex) -> {
                if (ex != null) {
                    result.complete(TaskResult.failed(inv.correlationId(), toErrorInfo(ex)));
                } else {
                    result.complete(TaskResult.completed(inv.correlationId(), output));
                }
            });
        } catch (Exception e) {
            // a synchronous throw before/outside the stage above (routing lookup, or the handler
            // throwing before ever returning a stage) — still must become a TaskResult, never escape.
            result.complete(TaskResult.failed(inv.correlationId(), toErrorInfo(e)));
        }
        return result;
    }

    private static ErrorInfo toErrorInfo(Throwable t) {
        Throwable cause = t instanceof java.util.concurrent.CompletionException && t.getCause() != null ? t.getCause() : t;
        return cause instanceof TaskFailure failure ? failure.toErrorInfo() : ErrorInfo.of(cause);
    }
}
