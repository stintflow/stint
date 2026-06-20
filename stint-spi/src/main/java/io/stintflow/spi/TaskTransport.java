package io.stintflow.spi;

import java.util.concurrent.CompletionStage;

/**
 * Port: how the orchestrator hands a task off to a worker and gets the result back, asynchronously.
 * <p>
 * Implementations are connectors (in-memory, SQS, EventBridge, Kafka, Knative, ...). The core never
 * imports a cloud SDK — it only talks to this interface.
 */
public interface TaskTransport {

    /**
     * Publish a task invocation for a worker to pick up. The returned stage completes once the
     * transport has <em>accepted</em> the message — NOT when the task has executed.
     */
    CompletionStage<Void> dispatch(TaskInvocation invocation);

    /**
     * Register the handler the runtime uses to resume instances when results come back. Connectors
     * that consume results (queue poller, HTTP receiver, ...) deliver every {@link TaskResult} here.
     */
    void onResult(TaskResultHandler handler);

    /** Honest declaration of this transport's guarantees and limits. */
    AdapterCapabilities capabilities();
}
