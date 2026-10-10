package io.stintflow.example;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.wire.Json;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.DataFlow;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryStateStore;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.spi.AdapterCapabilities;
import io.stintflow.spi.AdapterCapabilities.DeliveryGuarantee;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.TaskResultHandler;
import io.stintflow.spi.TaskTransport;
import io.stintflow.spi.TimerService;
import io.stintflow.spi.WorkflowRef;

/**
 * CA4: a suspended instance is resumed correctly by a brand-new {@link WorkflowEngine} instance —
 * as would happen after a redeploy — as long as it shares the same {@link StateStore}. The result is
 * delivered manually (not through the auto-responding in-memory worker) so the test controls exactly
 * when engine A is discarded and engine B takes over, instead of racing a background thread.
 */
class WorkflowEngineResumeTest {

    @Test
    void a_new_engine_over_the_same_state_store_resumes_a_suspended_instance() throws Exception {
        WorkflowRef ref = new WorkflowRef("test", "resume", "1.0.0");
        // export.as merges the raw output into $context — otherwise (RF5) $context is left untouched
        // by default, and the final workflow result (its $context) wouldn't reflect the task's output.
        DataFlow echoFlow = new DataFlow(null, null, Expr.jq("$context + ."));
        WorkflowDefinition def = WorkflowBuilder.create()
                .callRemote("echo", "echo-route", echoFlow)
                .build(ref);

        WorkflowRegistry registry = new WorkflowRegistry();
        registry.register(def);

        StateStore sharedState = new InMemoryStateStore();
        BlobStore blob = new FilesystemBlobStore(Files.createTempDirectory("stint-ca4-blobs"));
        TimerService timer = new InMemoryTimerService();
        ManualTransport transport = new ManualTransport();

        WorkflowEngine engineA = new WorkflowEngine(registry, transport, sharedState, timer, blob);
        ObjectNode input = Json.obj();
        input.put("hello", "world");
        String instanceId = engineA.start(ref, input);

        TaskInvocation invocation = transport.awaitDispatch(5, TimeUnit.SECONDS);
        assertThat(invocation.workflowInstanceId()).isEqualTo(instanceId);

        InstanceSnapshot waiting = awaitSnapshot(sharedState, instanceId, InstanceStatus.WAITING);
        assertThat(waiting.position()).isEqualTo("/do/0/echo");

        // engineA is discarded here, without ever seeing a result — simulates a redeploy/new process.
        // engineB re-registers onResult on the shared transport, taking over as the active handler.
        WorkflowEngine engineB = new WorkflowEngine(registry, transport, sharedState, timer, blob);

        ObjectNode output = Json.obj();
        output.put("echoed", true);
        transport.deliverResult(TaskResult.completed(invocation.correlationId(), output))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        InstanceSnapshot finalSnapshot = awaitSnapshot(sharedState, instanceId, InstanceStatus.COMPLETED);
        assertThat(finalSnapshot.context().get("echoed").asBoolean()).isTrue();
        assertThat(engineB).isNotNull(); // keeps engineB reachable/registered for the whole test
    }

    private static InstanceSnapshot awaitSnapshot(StateStore state, String instanceId, InstanceStatus expected)
            throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            var snap = state.load(instanceId).toCompletableFuture().get(1, TimeUnit.SECONDS);
            if (snap.isPresent() && snap.get().status() == expected) {
                return snap.get();
            }
            Thread.sleep(20);
        }
        throw new TimeoutException("Instance " + instanceId + " never reached status " + expected);
    }

    /** No auto-responding worker: dispatches are captured, results are delivered only when the test asks. */
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
            return new AdapterCapabilities(DeliveryGuarantee.EXACTLY_ONCE, true, Long.MAX_VALUE, null, true, true);
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
