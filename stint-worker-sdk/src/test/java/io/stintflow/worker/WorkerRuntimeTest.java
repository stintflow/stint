package io.stintflow.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.core.DefaultCloudEventCodec;
import io.stintflow.core.Json;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.spi.wire.CloudEventCodec;

/** CA7 (SDD 1.3): every failure path in {@link WorkerRuntime} becomes a {@code TaskResult.FAILED}, never an escaping exception. */
class WorkerRuntimeTest {

    private static final CloudEventCodec CODEC = new DefaultCloudEventCodec();
    private static final BlobStore NO_BLOB = new BlobStore() {
        @Override
        public CompletionStage<URI> put(byte[] data, String key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletionStage<byte[]> get(URI ref) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletionStage<Void> delete(URI ref) {
            throw new UnsupportedOperationException();
        }
    };

    @Test
    void unknown_routing_key_becomes_a_failed_result() throws Exception {
        WorkerRuntime runtime = new WorkerRuntime(new TaskHandlerRegistry(), NO_BLOB);
        TaskResult result = invoke(runtime, "no-such-route", Json.obj());
        assertThat(result.status()).isEqualTo(TaskResult.Status.FAILED);
        assertThat(result.error().detail()).contains("no-such-route");
    }

    @Test
    void handler_throwing_synchronously_becomes_a_failed_result() throws Exception {
        TaskHandlerRegistry handlers = new TaskHandlerRegistry()
                .register("boom", ctx -> {
                    throw new IllegalStateException("synchronous boom");
                });
        WorkerRuntime runtime = new WorkerRuntime(handlers, NO_BLOB);
        TaskResult result = invoke(runtime, "boom", Json.obj());
        assertThat(result.status()).isEqualTo(TaskResult.Status.FAILED);
        assertThat(result.error().detail()).contains("synchronous boom");
    }

    @Test
    void handler_failing_its_returned_stage_becomes_a_failed_result() throws Exception {
        TaskHandlerRegistry handlers = new TaskHandlerRegistry()
                .register("async-boom", ctx -> CompletableFuture.failedFuture(new RuntimeException("async boom")));
        WorkerRuntime runtime = new WorkerRuntime(handlers, NO_BLOB);
        TaskResult result = invoke(runtime, "async-boom", Json.obj());
        assertThat(result.status()).isEqualTo(TaskResult.Status.FAILED);
        assertThat(result.error().detail()).contains("async boom");
    }

    @Test
    void task_failure_exception_carries_its_own_type_status_and_retry_after() throws Exception {
        TaskHandlerRegistry handlers = new TaskHandlerRegistry()
                .register("rate-limited", ctx -> {
                    throw new TaskFailure(URI.create("stint://errors/communication"), 429, "rate limited",
                            Duration.ofSeconds(30));
                });
        WorkerRuntime runtime = new WorkerRuntime(handlers, NO_BLOB);
        TaskResult result = invoke(runtime, "rate-limited", Json.obj());
        assertThat(result.status()).isEqualTo(TaskResult.Status.FAILED);
        assertThat(result.error().status()).isEqualTo(429);
        assertThat(result.error().retryAfter()).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void handler_succeeding_still_works() throws Exception {
        TaskHandlerRegistry handlers = new TaskHandlerRegistry()
                .register("ok", ctx -> CompletableFuture.completedFuture(ctx.input()));
        WorkerRuntime runtime = new WorkerRuntime(handlers, NO_BLOB);
        ObjectNode input = Json.obj();
        input.put("hello", "world");
        TaskResult result = invoke(runtime, "ok", input);
        assertThat(result.status()).isEqualTo(TaskResult.Status.COMPLETED);
        assertThat(result.output().get("hello").asText()).isEqualTo("world");
    }

    private static TaskResult invoke(WorkerRuntime runtime, String routingKey, JsonNode input) throws Exception {
        TaskInvocation inv = new TaskInvocation("inst-1", "/do/0/task", "corr-1",
                new WorkflowRef("test", "wf", "1.0.0"), routingKey, input, 1);
        var invokeEvent = CODEC.toEvent(inv);
        var resultEvent = runtime.handle(invokeEvent).toCompletableFuture().get(5, TimeUnit.SECONDS);
        return CODEC.toResult(resultEvent);
    }
}
