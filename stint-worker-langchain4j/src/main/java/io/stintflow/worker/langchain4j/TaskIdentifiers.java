package io.stintflow.worker.langchain4j;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.MDC;

import io.stintflow.spi.TraceAttributes;
import io.stintflow.worker.TaskContext;

/**
 * A task's identifiers (SDD 2.5, decision 7) — for spans and for the logging MDC. The worker SDK puts them in the MDC
 * only for the synchronous part of {@code WorkerRuntime.handle}; a handler that runs after an asynchronous claim-check
 * rehydration is on another thread, so the module puts them back for the duration of the execution.
 */
final class TaskIdentifiers implements AutoCloseable {

    private final Map<String, String> previous = new HashMap<>();

    private TaskIdentifiers(Map<String, String> fields) {
        fields.forEach((key, value) -> {
            previous.put(key, MDC.get(key));
            MDC.put(key, value);
        });
    }

    static Map<String, String> of(TaskContext ctx) {
        Map<String, String> ids = new HashMap<>();
        ids.put(TraceAttributes.INSTANCE_ID, ctx.instanceId());
        if (ctx.chainId() != null) {
            ids.put(TraceAttributes.CHAIN_ID, ctx.chainId());
        }
        if (ctx.definition() != null) {
            ids.put(TraceAttributes.WORKFLOW, ctx.definition().canonical());
        }
        ids.put(TraceAttributes.TASK_ID, ctx.taskId());
        ids.put(TraceAttributes.ATTEMPT, Integer.toString(ctx.attempt()));
        ids.put(TraceAttributes.CORRELATION_ID, ctx.correlationId());
        return TraceAttributes.requireIdentifiersOnly(ids);
    }

    static TaskIdentifiers inMdc(TaskContext ctx) {
        return new TaskIdentifiers(of(ctx));
    }

    @Override
    public void close() {
        previous.forEach((key, value) -> {
            if (value == null) {
                MDC.remove(key);
            } else {
                MDC.put(key, value);
            }
        });
    }
}
