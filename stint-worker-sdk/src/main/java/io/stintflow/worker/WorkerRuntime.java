package io.stintflow.worker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.stintflow.wire.ClaimCheck;
import io.stintflow.wire.DefaultCloudEventCodec;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.ExecutionSpan;
import io.stintflow.spi.ExecutionTracer;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.TraceAttributes;
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
    private final ExecutionTracer tracer;

    public WorkerRuntime(TaskHandlerRegistry handlers, BlobStore blob) {
        this(handlers, blob, ExecutionTracer.NOOP);
    }

    /** SDD 2.5: with a tracer, each task execution is a short span, child of the engine activation that dispatched it. */
    public WorkerRuntime(TaskHandlerRegistry handlers, BlobStore blob, ExecutionTracer tracer) {
        this.handlers = handlers;
        this.blob = blob;
        this.tracer = tracer;
    }

    /**
     * SDD 2.5: the result carries the invocation's chain, the invoke's id as its cause, and the trace context
     * of this execution's span — which ends before the result is returned.
     */
    public CompletionStage<CloudEvent> handle(CloudEvent invokeEvent) {
        TaskInvocation inv = codec.toInvocation(invokeEvent);
        Map<String, String> ids = identifiers(inv);
        ExecutionSpan span = tracer.start("stint.task", ids,
                inv.lineage().hasTrace() ? inv.lineage() : null, List.of());
        Lineage lineage = new Lineage(inv.lineage().chainId(), inv.correlationId(), null, null).withTrace(span.context());
        CompletionStage<TaskResult> result;
        try (LogContext log = LogContext.open(ids, lineage)) {
            result = safeInvoke(inv, lineage);
        }
        return result.thenApply(r -> {
            if (r.status() == TaskResult.Status.FAILED && r.error() != null) {
                span.recordFailure(new IllegalStateException(String.valueOf(r.error().type())));
            }
            span.close();
            return codec.toEvent(r.withLineage(lineage), inv.workflowInstanceId());
        });
    }

    /** SDD 2.5, decision 7: identifiers only. */
    private static Map<String, String> identifiers(TaskInvocation inv) {
        Map<String, String> ids = new HashMap<>();
        ids.put(TraceAttributes.INSTANCE_ID, inv.workflowInstanceId());
        if (inv.lineage().chainId() != null) {
            ids.put(TraceAttributes.CHAIN_ID, inv.lineage().chainId());
        }
        ids.put(TraceAttributes.CAUSATION_ID, inv.correlationId());
        ids.put(TraceAttributes.WORKFLOW, inv.definition().canonical());
        ids.put(TraceAttributes.TASK_ID, inv.taskId());
        ids.put(TraceAttributes.ATTEMPT, Integer.toString(inv.attempt()));
        ids.put(TraceAttributes.CORRELATION_ID, inv.correlationId());
        return ids;
    }

    private CompletionStage<TaskResult> safeInvoke(TaskInvocation inv, Lineage lineage) {
        CompletableFuture<TaskResult> result = new CompletableFuture<>();
        try {
            TaskHandler handler = handlers.find(inv.routingKey())
                    .orElseThrow(() -> new IllegalStateException("No handler for routing key: " + inv.routingKey()));
            ClaimCheck.rehydrate(inv.input(), blob).thenCompose(input -> {
                TaskContext ctx = new TaskContext(inv.taskId(), inv.workflowInstanceId(),
                        inv.correlationId(), inv.attempt(), input, inv.definition(), lineage.chainId(), lineage);
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
