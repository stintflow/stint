package io.stintflow.inmemory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.model.FlowDirective;
import io.stintflow.spi.AdapterCapabilities;
import io.stintflow.spi.AdapterCapabilities.DeliveryGuarantee;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.TaskResultHandler;
import io.stintflow.spi.TaskTransport;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;

/**
 * SDD 1.5, CA4: {@link WorkflowEngine#awaitCompletion} works across two independent engine
 * instances sharing only the {@link StateStore} — engine A only starts the instance and awaits;
 * engine B is the one that actually processes the task result and drives the state transition.
 */
class WorkflowEngineAwaitCompletionTest {

    private static WorkflowDefinition oneStepWorkflow(WorkflowRef ref) {
        return WorkflowBuilder.create()
                .callRemote("step", "route-a", DataFlow.NONE, FlowDirective.END)
                .build(ref);
    }

    @Test
    void completed_by_a_different_engine_instance_sharing_the_state_store() throws Exception {
        StateStore state = new InMemoryStateStore();
        BlobStore blob = new FilesystemBlobStore(Files.createTempDirectory("ca4-completed"));
        WorkflowRef ref = new WorkflowRef("test", "ca4-completed", "1.0.0");
        WorkflowRegistry registry = new WorkflowRegistry();
        registry.register(oneStepWorkflow(ref));

        ManualTransport transportA = new ManualTransport();
        WorkflowEngine engineA = new WorkflowEngine(registry, transportA, state, new InMemoryTimerService(), blob);

        String instanceId = engineA.start(ref, Json.obj());
        CompletionStage<JsonNode> awaited = engineA.awaitCompletion(instanceId, Duration.ofSeconds(5));
        TaskInvocation invocation = transportA.awaitDispatch(5, TimeUnit.SECONDS);

        ManualTransport transportB = new ManualTransport();
        WorkflowEngine engineB = new WorkflowEngine(registry, transportB, state, new InMemoryTimerService(), blob);
        transportB.deliverResult(TaskResult.completed(invocation.correlationId(), Json.obj()))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertThat(awaited.toCompletableFuture().get(5, TimeUnit.SECONDS)).isNotNull();
        assertThat(engineB).isNotNull(); // keeps engineB reachable/alive for the whole assertion above
    }

    @Test
    void failed_by_a_different_engine_instance_sharing_the_state_store() throws Exception {
        StateStore state = new InMemoryStateStore();
        BlobStore blob = new FilesystemBlobStore(Files.createTempDirectory("ca4-failed"));
        WorkflowRef ref = new WorkflowRef("test", "ca4-failed", "1.0.0");
        WorkflowRegistry registry = new WorkflowRegistry();
        registry.register(oneStepWorkflow(ref));

        ManualTransport transportA = new ManualTransport();
        WorkflowEngine engineA = new WorkflowEngine(registry, transportA, state, new InMemoryTimerService(), blob);

        String instanceId = engineA.start(ref, Json.obj());
        CompletionStage<JsonNode> awaited = engineA.awaitCompletion(instanceId, Duration.ofSeconds(5));
        TaskInvocation invocation = transportA.awaitDispatch(5, TimeUnit.SECONDS);

        ManualTransport transportB = new ManualTransport();
        WorkflowEngine engineB = new WorkflowEngine(registry, transportB, state, new InMemoryTimerService(), blob);
        transportB.deliverResult(TaskResult.failed(invocation.correlationId(), ErrorInfo.of(new RuntimeException("boom"))))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertThatThrownBy(() -> awaited.toCompletableFuture().get(5, TimeUnit.SECONDS));
        assertThat(engineB).isNotNull();
    }

    @Test
    void times_out_when_nobody_ever_delivers_a_result() throws Exception {
        StateStore state = new InMemoryStateStore();
        BlobStore blob = new FilesystemBlobStore(Files.createTempDirectory("ca4-timeout"));
        WorkflowRef ref = new WorkflowRef("test", "ca4-timeout", "1.0.0");
        WorkflowRegistry registry = new WorkflowRegistry();
        registry.register(oneStepWorkflow(ref));

        ManualTransport transport = new ManualTransport();
        WorkflowEngine engine = new WorkflowEngine(registry, transport, state, new InMemoryTimerService(), blob);

        String instanceId = engine.start(ref, Json.obj());
        transport.awaitDispatch(5, TimeUnit.SECONDS);

        assertThatThrownBy(() -> engine.awaitCompletion(instanceId, Duration.ofMillis(100))
                .toCompletableFuture().get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(TimeoutException.class);
    }

    /** Same shape as {@code TryNodeRetryTest}'s helper: dispatch is captured, results delivered manually. */
    private static final class ManualTransport implements TaskTransport {
        private final LinkedBlockingQueue<TaskInvocation> dispatched = new LinkedBlockingQueue<>();
        private volatile TaskResultHandler handler;

        @Override
        public CompletionStage<Void> dispatch(TaskInvocation invocation) {
            dispatched.add(invocation);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void onResult(TaskResultHandler handler) {
            this.handler = handler;
        }

        @Override
        public AdapterCapabilities capabilities() {
            return new AdapterCapabilities(DeliveryGuarantee.AT_LEAST_ONCE, true, Long.MAX_VALUE, null, true, true);
        }

        TaskInvocation awaitDispatch(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
            TaskInvocation inv = dispatched.poll(timeout, unit);
            if (inv == null) {
                throw new TimeoutException("No dispatch observed within " + timeout + " " + unit);
            }
            return inv;
        }

        CompletionStage<Void> deliverResult(TaskResult result) {
            return handler.handle(result);
        }
    }
}
