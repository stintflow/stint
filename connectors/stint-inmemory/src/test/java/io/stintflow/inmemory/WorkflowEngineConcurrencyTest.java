package io.stintflow.inmemory;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.wire.Json;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.DataFlow;
import io.stintflow.spi.AdapterCapabilities;
import io.stintflow.spi.AdapterCapabilities.DeliveryGuarantee;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.TaskResultHandler;
import io.stintflow.spi.TaskTransport;
import io.stintflow.spi.TimerService;
import io.stintflow.spi.WorkflowRef;

/** CA1 and CA4 (SDD 1.2), exercised through the real {@link WorkflowEngine} + {@link InMemoryStateStore}. */
class WorkflowEngineConcurrencyTest {

    @Test
    void ca1_duplicate_delivery_advances_the_instance_exactly_once() throws Exception {
        WorkflowRef ref = new WorkflowRef("test", "two-step", "1.0.0");
        DataFlow mergeFlow = new DataFlow(null, null, Expr.jq("$context + ."));
        WorkflowDefinition def = WorkflowBuilder.create()
                .callRemote("first", "route-first", mergeFlow)
                .callRemote("second", "route-second", mergeFlow)
                .build(ref);

        WorkflowRegistry registry = new WorkflowRegistry();
        registry.register(def);
        StateStore state = new InMemoryStateStore();
        BlobStore blob = new FilesystemBlobStore(Files.createTempDirectory("stint-ca1-blobs"));
        TimerService timer = new InMemoryTimerService();
        ManualTransport transport = new ManualTransport();

        WorkflowEngine engine = new WorkflowEngine(registry, transport, state, timer, blob);
        String instanceId = engine.start(ref, Json.obj());

        TaskInvocation firstInvocation = transport.awaitDispatch(5, TimeUnit.SECONDS);
        assertThat(firstInvocation.taskId()).isEqualTo("/do/0/first");

        ObjectNode firstOutput = Json.obj();
        firstOutput.put("firstDone", true);
        TaskResult firstResult = TaskResult.completed(firstInvocation.correlationId(), firstOutput);

        // Deliver the SAME result twice, sequentially, each awaited to completion (at-least-once redelivery).
        transport.deliverResult(firstResult).toCompletableFuture().get(5, TimeUnit.SECONDS);
        transport.deliverResult(firstResult).toCompletableFuture().get(5, TimeUnit.SECONDS);

        // "second" must have been dispatched exactly once, not twice.
        assertThat(transport.dispatchCount()).isEqualTo(2);
        TaskInvocation secondInvocation = transport.awaitDispatch(5, TimeUnit.SECONDS);
        assertThat(secondInvocation.taskId()).isEqualTo("/do/1/second");

        ObjectNode secondOutput = Json.obj();
        secondOutput.put("secondDone", true);
        transport.deliverResult(TaskResult.completed(secondInvocation.correlationId(), secondOutput))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        InstanceSnapshot finalSnap = state.load(instanceId).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
        assertThat(finalSnap.status()).isEqualTo(InstanceStatus.COMPLETED);
        assertThat(finalSnap.version()).isEqualTo(3); // suspend(1) -> advance-to-second(2) -> complete(3): never 4
        assertThat(finalSnap.context().get("firstDone").asBoolean()).isTrue();
        assertThat(finalSnap.context().get("secondDone").asBoolean()).isTrue();
    }

    @Test
    void ca4_no_orphan_wait_after_completion() throws Exception {
        WorkflowRef ref = new WorkflowRef("test", "one-step-complete", "1.0.0");
        WorkflowDefinition def = WorkflowBuilder.create()
                .callRemote("echo", "route-echo", DataFlow.NONE)
                .build(ref);

        WorkflowRegistry registry = new WorkflowRegistry();
        registry.register(def);
        StateStore state = new InMemoryStateStore();
        BlobStore blob = new FilesystemBlobStore(Files.createTempDirectory("stint-ca4-blobs"));
        TimerService timer = new InMemoryTimerService();
        ManualTransport transport = new ManualTransport();

        WorkflowEngine engine = new WorkflowEngine(registry, transport, state, timer, blob);
        String instanceId = engine.start(ref, Json.obj());

        TaskInvocation invocation = transport.awaitDispatch(5, TimeUnit.SECONDS);
        String waitKey = "task:" + invocation.correlationId();
        String timerKey = "timer:" + invocation.correlationId();
        assertThat(state.findWait(waitKey).toCompletableFuture().get(5, TimeUnit.SECONDS)).isPresent();
        assertThat(state.findWait(timerKey).toCompletableFuture().get(5, TimeUnit.SECONDS)).isPresent();

        transport.deliverResult(TaskResult.completed(invocation.correlationId(), Json.obj()))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertThat(state.findWait(waitKey).toCompletableFuture().get(5, TimeUnit.SECONDS)).isEmpty();
        assertThat(state.findWait(timerKey).toCompletableFuture().get(5, TimeUnit.SECONDS)).isEmpty();
        InstanceSnapshot finalSnap = state.load(instanceId).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
        assertThat(finalSnap.status()).isEqualTo(InstanceStatus.COMPLETED);
    }

    @Test
    void ca4_no_orphan_wait_after_failure() throws Exception {
        WorkflowRef ref = new WorkflowRef("test", "one-step-fail", "1.0.0");
        WorkflowDefinition def = WorkflowBuilder.create()
                .callRemote("echo", "route-echo", DataFlow.NONE)
                .build(ref);

        WorkflowRegistry registry = new WorkflowRegistry();
        registry.register(def);
        StateStore state = new InMemoryStateStore();
        BlobStore blob = new FilesystemBlobStore(Files.createTempDirectory("stint-ca4-fail-blobs"));
        TimerService timer = new InMemoryTimerService();
        ManualTransport transport = new ManualTransport();

        WorkflowEngine engine = new WorkflowEngine(registry, transport, state, timer, blob);
        String instanceId = engine.start(ref, Json.obj());

        TaskInvocation invocation = transport.awaitDispatch(5, TimeUnit.SECONDS);
        String waitKey = "task:" + invocation.correlationId();
        String timerKey = "timer:" + invocation.correlationId();

        transport.deliverResult(TaskResult.failed(invocation.correlationId(),
                        ErrorInfo.of(new RuntimeException("simulated worker failure"))))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertThat(state.findWait(waitKey).toCompletableFuture().get(5, TimeUnit.SECONDS)).isEmpty();
        assertThat(state.findWait(timerKey).toCompletableFuture().get(5, TimeUnit.SECONDS)).isEmpty();
        InstanceSnapshot finalSnap = state.load(instanceId).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
        assertThat(finalSnap.status()).isEqualTo(InstanceStatus.FAILED);
    }

    /** No auto-responding worker: dispatches are captured and counted; results are delivered on demand. */
    private static final class ManualTransport implements TaskTransport {
        private final LinkedBlockingQueue<TaskInvocation> dispatched = new LinkedBlockingQueue<>();
        private final AtomicInteger dispatchCount = new AtomicInteger();
        private volatile TaskResultHandler handler;

        @Override
        public CompletionStage<Void> dispatch(TaskInvocation invocation) {
            dispatchCount.incrementAndGet();
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

        int dispatchCount() {
            return dispatchCount.get();
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
