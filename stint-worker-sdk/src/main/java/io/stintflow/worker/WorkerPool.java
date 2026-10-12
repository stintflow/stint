package io.stintflow.worker;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.cloudevents.CloudEvent;

/**
 * Runs up to {@code maxConcurrent} task deliveries at once (SDD 3a.3, 5.7), so a slow task no longer holds the ones
 * received with it — the serial loop behind the p95 of 31 s in the sandbox's load test (S.4). Transport-neutral: a
 * binding receives from its broker, asks for at most {@link #available()} messages, and {@link #submit}s each.
 * <p>
 * At-least-once is kept per message: a delivery is acknowledged only after its result was shipped, and a delivery
 * whose handling or shipping fails is never acknowledged, so the broker delivers it again. Acknowledgements are per
 * message, in completion order — never per batch; the order between messages is not guaranteed, as on SQS standard.
 * <p>
 * Virtual threads by default: an LLM call blocks its thread for seconds, and a virtual thread costs little while it
 * waits.
 */
public final class WorkerPool implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerPool.class);

    private final WorkerRuntime runtime;
    private final Semaphore permits;
    private final ExecutorService executor;

    public WorkerPool(WorkerRuntime runtime, int maxConcurrent) {
        this(runtime, maxConcurrent, Executors.newVirtualThreadPerTaskExecutor());
    }

    public WorkerPool(WorkerRuntime runtime, int maxConcurrent, ExecutorService executor) {
        if (maxConcurrent < 1) {
            throw new IllegalArgumentException("maxConcurrent must be at least 1");
        }
        this.runtime = runtime;
        this.permits = new Semaphore(maxConcurrent);
        this.executor = executor;
    }

    /** Deliveries that could start now: a binding asks its broker for at most this many, so none waits in memory. */
    public int available() {
        return permits.availablePermits();
    }

    /**
     * Runs one delivery: {@code handle → ship(result) → ack}. Blocks the caller until a slot is free — the binding's
     * back-pressure. The returned stage completes after the ack, or exceptionally (with no ack) if handling or
     * shipping failed.
     *
     * @param invoke the task's invoke event
     * @param ship   sends the result event (to the result queue, or back to the engine)
     * @param ack    acknowledges the delivery to the broker (deletes the message)
     */
    public CompletionStage<Void> submit(CloudEvent invoke, Function<CloudEvent, CompletionStage<Void>> ship,
            Runnable ack) throws InterruptedException {
        permits.acquire();
        CompletableFuture<Void> done = new CompletableFuture<>();
        try {
            executor.execute(() -> run(invoke, ship, ack).whenComplete((v, failure) -> {
                permits.release();
                if (failure == null) {
                    done.complete(null);
                } else {
                    Throwable cause = failure.getCause() != null ? failure.getCause() : failure;
                    LOG.warn("Delivery {} was not acknowledged and is left for redelivery: {}", invoke.getId(),
                            cause.getClass().getName());
                    done.completeExceptionally(cause);
                }
            }));
        } catch (RejectedExecutionException e) {
            permits.release();
            throw e;
        }
        return done;
    }

    private CompletionStage<Void> run(CloudEvent invoke, Function<CloudEvent, CompletionStage<Void>> ship,
            Runnable ack) {
        try {
            return runtime.handle(invoke).thenCompose(ship).thenRun(ack);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    /** Stops taking deliveries; those running finish (unacknowledged ones are redelivered by the broker). */
    @Override
    public void close() {
        executor.shutdown();
    }
}
