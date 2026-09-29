package io.stintflow.inmemory;

import static io.stintflow.inmemory.DomainEventFixture.ORDER_PLACED;
import static io.stintflow.inmemory.DomainEventFixture.orderPlaced;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import io.cloudevents.CloudEvent;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.trigger.EventFilter;
import io.stintflow.core.trigger.TriggerBinding;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.SaveOutcome;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.Wait;
import io.stintflow.spi.WorkflowRef;

/** SDD 2.1: CA1, CA2 (sequential and concurrent), CA4 (failed create) and CA7 (fan-out). */
class DomainEventStartTest {

    private static final WorkflowRef BILLING = new WorkflowRef("billing", "invoice-order", "1.0.0");
    private static final WorkflowRef SHIPPING = new WorkflowRef("shipping", "ship-order", "1.0.0");

    @Test
    void ca1_cloud_event_starts_the_bound_definition() throws Exception {
        DomainEventFixture f = new DomainEventFixture(registryWith(BILLING), new InMemoryStateStore());
        f.bindings.bind(new TriggerBinding("billing-on-order", EventFilter.ofType(ORDER_PLACED), BILLING));

        CloudEvent event = orderPlaced("evt-42", "A-1");
        f.bus.publish(event).toCompletableFuture().get(5, TimeUnit.SECONDS);

        TaskInvocation invocation = f.transport.awaitDispatch();
        String instanceId = WorkflowEngine.triggeredInstanceId(BILLING, event);
        assertThat(invocation.workflowInstanceId()).isEqualTo(instanceId);
        assertThat(invocation.definition()).isEqualTo(BILLING);
        // input.from ".[0].data": the workflow input is DSL 1.0's array of triggering events
        assertThat(invocation.input().get("orderId").asText()).isEqualTo("A-1");

        InstanceSnapshot snap = f.load(instanceId);
        assertThat(snap.status()).isEqualTo(InstanceStatus.WAITING);
        assertThat(snap.input().isArray()).isTrue();
        assertThat(snap.input()).hasSize(1);
        assertThat(snap.input().get(0).get("id").asText()).isEqualTo("evt-42");
        assertThat(snap.input().get(0).get("type").asText()).isEqualTo(ORDER_PLACED);
        assertThat(snap.input().get(0).get("source").asText()).isEqualTo(DomainEventFixture.ORDERS_SOURCE.toString());
        assertThat(snap.startedAt()).isNotNull();
    }

    @Test
    void ca2_same_event_delivered_twice_creates_one_instance() throws Exception {
        DomainEventFixture f = new DomainEventFixture(registryWith(BILLING), new InMemoryStateStore());
        f.bindings.bind(new TriggerBinding("billing-on-order", EventFilter.ofType(ORDER_PLACED), BILLING));
        CloudEvent event = orderPlaced("evt-42", "A-1");

        f.bus.publish(event).toCompletableFuture().get(5, TimeUnit.SECONDS);
        f.bus.publish(event).toCompletableFuture().get(5, TimeUnit.SECONDS); // redelivery: acked, no-op

        assertThat(f.transport.dispatchCount()).isEqualTo(1);
        InstanceSnapshot snap = f.load(WorkflowEngine.triggeredInstanceId(BILLING, event));
        assertThat(snap.version()).isEqualTo(1); // created once, never re-created or advanced by the duplicate
    }

    @Test
    void ca2_two_concurrent_deliveries_create_one_instance() throws Exception {
        CreateBarrierStore state = new CreateBarrierStore(new InMemoryStateStore(), new CyclicBarrier(2));
        DomainEventFixture f = new DomainEventFixture(registryWith(BILLING), state);
        f.bindings.bind(new TriggerBinding("billing-on-order", EventFilter.ofType(ORDER_PLACED), BILLING));
        CloudEvent event = orderPlaced("evt-42", "A-1");

        // Both deliveries reach the create save together (barrier), then race for it.
        CompletableFuture<Void> first = CompletableFuture.runAsync(() -> f.bus.publish(event).toCompletableFuture().join());
        CompletableFuture<Void> second = CompletableFuture.runAsync(() -> f.bus.publish(event).toCompletableFuture().join());
        first.get(10, TimeUnit.SECONDS);
        second.get(10, TimeUnit.SECONDS); // both acked

        assertThat(state.createOutcomes).containsExactlyInAnyOrder(SaveOutcome.OK, SaveOutcome.CONFLICT);
        assertThat(f.transport.dispatchCount()).isEqualTo(1); // only the winner dispatched
        assertThat(f.load(WorkflowEngine.triggeredInstanceId(BILLING, event)).version()).isEqualTo(1);
    }

    @Test
    void failed_create_is_not_acked_and_redelivery_creates_exactly_one() throws Exception {
        FailFirstCreateStore state = new FailFirstCreateStore(new InMemoryStateStore());
        DomainEventFixture f = new DomainEventFixture(registryWith(BILLING), state);
        f.bindings.bind(new TriggerBinding("billing-on-order", EventFilter.ofType(ORDER_PLACED), BILLING));
        CloudEvent event = orderPlaced("evt-42", "A-1");
        String instanceId = WorkflowEngine.triggeredInstanceId(BILLING, event);

        assertThatThrownBy(() -> f.bus.publish(event).toCompletableFuture().get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class); // not acked -> the source redelivers
        assertThat(f.state.load(instanceId).toCompletableFuture().get(5, TimeUnit.SECONDS)).isEmpty();
        assertThat(f.transport.dispatchCount()).isZero();

        f.bus.publish(event).toCompletableFuture().get(5, TimeUnit.SECONDS); // redelivery

        assertThat(f.transport.dispatchCount()).isEqualTo(1);
        assertThat(f.load(instanceId).version()).isEqualTo(1);
    }

    @Test
    void fan_out_starts_each_bound_definition_once() throws Exception {
        DomainEventFixture f = new DomainEventFixture(registryWith(BILLING, SHIPPING), new InMemoryStateStore());
        f.bindings.bind(new TriggerBinding("billing-on-order", EventFilter.ofType(ORDER_PLACED), BILLING));
        f.bindings.bind(new TriggerBinding("shipping-on-order", EventFilter.ofType(ORDER_PLACED), SHIPPING));
        CloudEvent event = orderPlaced("evt-42", "A-1");

        f.bus.publish(event).toCompletableFuture().get(5, TimeUnit.SECONDS);
        f.bus.publish(event).toCompletableFuture().get(5, TimeUnit.SECONDS); // redelivery

        assertThat(f.transport.dispatchCount()).isEqualTo(2);
        String billingId = WorkflowEngine.triggeredInstanceId(BILLING, event);
        String shippingId = WorkflowEngine.triggeredInstanceId(SHIPPING, event);
        assertThat(billingId).isNotEqualTo(shippingId);
        assertThat(f.load(billingId).definition()).isEqualTo(BILLING);
        assertThat(f.load(shippingId).definition()).isEqualTo(SHIPPING);
    }

    private static WorkflowRegistry registryWith(WorkflowRef... refs) {
        WorkflowRegistry registry = new WorkflowRegistry();
        for (WorkflowRef ref : refs) {
            registry.register(WorkflowBuilder.create()
                    .callRemote("handle", "route-" + ref.name(), new DataFlow(Expr.jq(".[0].data"), null, null))
                    .build(ref));
        }
        return registry;
    }

    /** Holds every create save ({@code expectedVersion == 0}) at a barrier, then records its outcome. */
    private static final class CreateBarrierStore extends DelegatingStore {
        private final CyclicBarrier barrier;
        final ConcurrentLinkedQueue<SaveOutcome> createOutcomes = new ConcurrentLinkedQueue<>();

        CreateBarrierStore(StateStore delegate, CyclicBarrier barrier) {
            super(delegate);
            this.barrier = barrier;
        }

        @Override
        public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion,
                List<Wait> addWaits, List<String> consumeWaitKeys) {
            if (expectedVersion != 0) {
                return delegate.save(snapshot, expectedVersion, addWaits, consumeWaitKeys);
            }
            try {
                barrier.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                return CompletableFuture.failedFuture(e);
            }
            return delegate.save(snapshot, expectedVersion, addWaits, consumeWaitKeys)
                    .thenApply(outcome -> {
                        createOutcomes.add(outcome);
                        return outcome;
                    });
        }
    }

    /** The first create save fails like an I/O error, without persisting anything. */
    private static final class FailFirstCreateStore extends DelegatingStore {
        private final AtomicBoolean failed = new AtomicBoolean();

        FailFirstCreateStore(StateStore delegate) {
            super(delegate);
        }

        @Override
        public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion,
                List<Wait> addWaits, List<String> consumeWaitKeys) {
            if (expectedVersion == 0 && failed.compareAndSet(false, true)) {
                return CompletableFuture.failedFuture(new IllegalStateException("simulated state store outage"));
            }
            return delegate.save(snapshot, expectedVersion, addWaits, consumeWaitKeys);
        }
    }

    private abstract static class DelegatingStore implements StateStore {
        final StateStore delegate;

        DelegatingStore(StateStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletionStage<Optional<InstanceSnapshot>> load(String instanceId) {
            return delegate.load(instanceId);
        }

        @Override
        public CompletionStage<Optional<Wait>> findWait(String waitKey) {
            return delegate.findWait(waitKey);
        }

        @Override
        public CompletionStage<Void> delete(String instanceId) {
            return delegate.delete(instanceId);
        }
    }
}
