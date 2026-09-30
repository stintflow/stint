package io.stintflow.inmemory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import io.stintflow.wire.DefaultCloudEventCodec;
import io.stintflow.spi.AdapterCapabilities;
import io.stintflow.spi.AdapterCapabilities.DeliveryGuarantee;
import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.TaskResultHandler;
import io.stintflow.spi.TaskTransport;
import io.stintflow.spi.wire.CloudEventCodec;
import io.cloudevents.CloudEvent;

/**
 * In-process transport that runs the whole dispatch→worker→result loop in one JVM, asynchronously.
 * <p>
 * It still serializes through the real {@link CloudEventCodec}, so the local path exercises the same
 * wire contract and claim-check logic as the cloud connectors — only the broker is faked.
 * <p>
 * Wire it to a worker via {@link #connectWorker} (typically {@code workerRuntime::handle}).
 */
public final class InMemoryTaskTransport implements TaskTransport {

    private final CloudEventCodec codec = new DefaultCloudEventCodec();
    private final ExecutorService pool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "stint-inmemory-transport");
        t.setDaemon(true);
        return t;
    });

    private volatile TaskResultHandler resultHandler;
    private volatile Function<CloudEvent, CompletionStage<CloudEvent>> worker;

    /** Attach the worker side of the loop (decode → execute → encode). */
    public void connectWorker(Function<CloudEvent, CompletionStage<CloudEvent>> worker) {
        this.worker = worker;
    }

    @Override
    public CompletionStage<Void> dispatch(TaskInvocation invocation) {
        CloudEvent invokeEvent = codec.toEvent(invocation);
        return CompletableFuture.runAsync(() -> {
            worker.apply(invokeEvent)
                    .thenAccept(resultEvent -> resultHandler.handle(codec.toResult(resultEvent)))
                    .exceptionally(ex -> {
                        // SDD 1.3, RF9: a worker function that throws/fails must still produce a result —
                        // otherwise the instance would wait forever for something that will never arrive.
                        Throwable cause = ex instanceof java.util.concurrent.CompletionException && ex.getCause() != null
                                ? ex.getCause() : ex;
                        // SDD 2.5, RF11: a locally fabricated result keeps the invocation's chain; its cause is the invoke.
                        resultHandler.handle(TaskResult.failed(invocation.correlationId(), ErrorInfo.of(cause))
                                .withLineage(new Lineage(invocation.lineage().chainId(), invocation.correlationId(),
                                        invocation.lineage().traceparent(), invocation.lineage().tracestate())));
                        return null;
                    });
        }, pool);
    }

    @Override
    public void onResult(TaskResultHandler handler) {
        this.resultHandler = handler;
    }

    @Override
    public AdapterCapabilities capabilities() {
        // The fake broker is honest about being ideal: use it only for dev/tests.
        return new AdapterCapabilities(DeliveryGuarantee.EXACTLY_ONCE, true,
                Long.MAX_VALUE, Duration.ofHours(1), true, true);
    }
}
