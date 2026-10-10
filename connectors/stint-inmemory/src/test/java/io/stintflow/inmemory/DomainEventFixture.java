package io.stintflow.inmemory;

import java.net.URI;
import java.nio.file.Files;
import java.time.InstantSource;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.trigger.DomainEventRouter;
import io.stintflow.core.trigger.StartReaction;
import io.stintflow.core.trigger.TriggerBindings;
import io.stintflow.spi.AdapterCapabilities;
import io.stintflow.spi.AdapterCapabilities.DeliveryGuarantee;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.TaskResultHandler;
import io.stintflow.spi.TaskTransport;
import io.stintflow.wire.Json;

/**
 * Shared wiring for the SDD 2.1 tests: engine + bindings + {@link DomainEventRouter} subscribed to an
 * {@link InMemoryDomainEventBus}, a capturing transport (no worker; results delivered on demand) and
 * a test-mode timer (no background sweep).
 */
final class DomainEventFixture {

    static final String ORDER_PLACED = "io.acme.order.placed.v1";
    static final URI ORDERS_SOURCE = URI.create("https://acme.example/orders");

    final WorkflowRegistry registry;
    final StateStore state;
    final CapturingTransport transport = new CapturingTransport();
    final InMemoryTimerService timer = new InMemoryTimerService(InstantSource.system());
    final WorkflowEngine engine;
    final TriggerBindings bindings;
    final InMemoryDomainEventBus bus = new InMemoryDomainEventBus();

    DomainEventFixture(WorkflowRegistry registry, StateStore state) throws Exception {
        this(registry, registry, state);
    }

    /** @param engineRegistry what the engine knows — may differ from {@code bindingRegistry} (deploy skew). */
    DomainEventFixture(WorkflowRegistry bindingRegistry, WorkflowRegistry engineRegistry, StateStore state)
            throws Exception {
        this.registry = engineRegistry;
        this.state = state;
        this.engine = new WorkflowEngine(engineRegistry, transport, state, timer,
                new FilesystemBlobStore(Files.createTempDirectory("stint-sdd21-blobs")));
        this.bindings = new TriggerBindings(bindingRegistry);
        new DomainEventRouter(List.of(new StartReaction(bindings, engine))).subscribeTo(bus);
    }

    static CloudEvent orderPlaced(String eventId, String orderId) {
        ObjectNode data = Json.obj();
        data.put("orderId", orderId);
        return CloudEventBuilder.v1()
                .withId(eventId)
                .withSource(ORDERS_SOURCE)
                .withType(ORDER_PLACED)
                .withDataContentType("application/json")
                .withData(Json.bytes(data))
                .build();
    }

    InstanceSnapshot load(String instanceId) throws Exception {
        return state.load(instanceId).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
    }

    /** No auto-responding worker: dispatches are captured and counted; results are delivered on demand. */
    static final class CapturingTransport implements TaskTransport {
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

        TaskInvocation awaitDispatch() throws InterruptedException, TimeoutException {
            TaskInvocation inv = dispatched.poll(5, TimeUnit.SECONDS);
            if (inv == null) {
                throw new TimeoutException("No dispatch observed");
            }
            return inv;
        }

        CompletionStage<Void> deliverResult(TaskResult result) {
            return handler.handle(result);
        }
    }
}
