package io.stintflow.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.cloudevents.CloudEvent;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.DefaultCloudEventCodec;
import io.stintflow.wire.Json;

/**
 * SDD 3a.3, CA8 (RF9, 5.7): with {@code maxConcurrent} 2, a task held at a barrier doesn't keep another from finishing
 * and being acknowledged; a delivery is acknowledged only after its result was shipped, and never if shipping failed.
 * Barriers only — no sleep.
 */
class WorkerPoolTest {

    private final DefaultCloudEventCodec codec = new DefaultCloudEventCodec();
    private final CountDownLatch slowStarted = new CountDownLatch(1);
    private final CountDownLatch releaseSlow = new CountDownLatch(1);
    private final List<String> acked = new CopyOnWriteArrayList<>();
    private final List<String> shipped = new CopyOnWriteArrayList<>();

    private final TaskHandlerRegistry handlers = new TaskHandlerRegistry()
            .register("slow", ctx -> {
                slowStarted.countDown();
                await(releaseSlow);           // held at the barrier until the test releases it
                return CompletableFuture.completedFuture(Json.obj().put("done", ctx.taskId()));
            })
            .register("fast", ctx -> CompletableFuture.completedFuture(Json.obj().put("done", ctx.taskId())));

    private final WorkerRuntime runtime = new WorkerRuntime(handlers, (BlobStore) null);

    private CloudEvent invoke(String correlationId, String routingKey) {
        return codec.toEvent(new TaskInvocation("i-1", "/do/0/" + routingKey, correlationId,
                new WorkflowRef("test", "pool", "1.0.0"), routingKey, Json.obj(), 1));
    }

    private CompletionStage<Void> ship(CloudEvent result) {
        TaskResult decoded = codec.toResult(result);
        shipped.add(decoded.correlationId());
        return CompletableFuture.completedFuture(null);
    }

    @Test
    void a_slow_task_does_not_hold_the_others() throws Exception {
        try (WorkerPool pool = new WorkerPool(runtime, 2)) {
            CompletionStage<Void> slow = pool.submit(invoke("c-slow", "slow"), this::ship, () -> acked.add("c-slow"));
            assertThat(slowStarted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(pool.available()).isEqualTo(1);

            CompletionStage<Void> fast = pool.submit(invoke("c-fast", "fast"), this::ship, () -> acked.add("c-fast"));
            fast.toCompletableFuture().get(10, TimeUnit.SECONDS);

            assertThat(acked).containsExactly("c-fast");          // acknowledged while the slow one is still held
            assertThat(slow.toCompletableFuture()).isNotDone();

            releaseSlow.countDown();
            slow.toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertThat(acked).containsExactly("c-fast", "c-slow");
            assertThat(pool.available()).isEqualTo(2);
        }
    }

    @Test
    void the_ack_comes_after_the_result_was_shipped_and_never_if_shipping_failed() throws Exception {
        try (WorkerPool pool = new WorkerPool(runtime, 2)) {
            pool.submit(invoke("c-1", "fast"), result -> {
                assertThat(acked).as("not acknowledged before its result is shipped").isEmpty();
                return ship(result);
            }, () -> acked.add("c-1")).toCompletableFuture().get(10, TimeUnit.SECONDS);

            CompletionStage<Void> failed = pool.submit(invoke("c-2", "fast"),
                    result -> CompletableFuture.failedFuture(new IllegalStateException("result queue unavailable")),
                    () -> acked.add("c-2"));

            assertThatThrownBy(() -> failed.toCompletableFuture().get(10, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(IllegalStateException.class);
            assertThat(acked).containsExactly("c-1");             // c-2 is left for the broker to redeliver
            assertThat(shipped).containsExactly("c-1");
            assertThat(pool.available()).isEqualTo(2);            // the failed delivery gave its slot back
        }
    }

    @Test
    void a_failing_handler_still_ships_a_failed_result_and_is_acknowledged() throws Exception {
        TaskHandlerRegistry failing = new TaskHandlerRegistry().register("boom", ctx -> {
            throw new IllegalStateException("handler bug");
        });
        try (WorkerPool pool = new WorkerPool(new WorkerRuntime(failing, (BlobStore) null), 1)) {
            List<TaskResult> results = new CopyOnWriteArrayList<>();
            pool.submit(invoke("c-1", "boom"), result -> {
                results.add(codec.toResult(result));
                return CompletableFuture.completedFuture(null);
            }, () -> acked.add("c-1")).toCompletableFuture().get(10, TimeUnit.SECONDS);

            // SDD 1.3, RF9: the runtime turns the throw into a FAILED result, which is a result like any other
            assertThat(results).singleElement().extracting(TaskResult::status).isEqualTo(TaskResult.Status.FAILED);
            assertThat(acked).containsExactly("c-1");
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("the test never released the barrier");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
