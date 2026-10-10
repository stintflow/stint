package io.stintflow.inmemory;

import static io.stintflow.inmemory.DomainEventFixture.ORDER_PLACED;
import static io.stintflow.inmemory.DomainEventFixture.orderPlaced;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.cloudevents.CloudEvent;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.trigger.DomainEventReaction.Reacted;
import io.stintflow.core.trigger.DomainEventRouter;
import io.stintflow.core.trigger.EventFilter;
import io.stintflow.core.trigger.TriggerBinding;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.WorkflowRef;

/** SDD 2.1, CA5 (sec. 8d): the acknowledgement policy per case, observed through the bus's "ack" stage. */
class DomainEventRouterTest {

    private static final WorkflowRef BILLING = new WorkflowRef("billing", "invoice-order", "1.0.0");

    @Test
    void event_without_binding_is_acked_and_starts_nothing() throws Exception {
        DomainEventFixture f = new DomainEventFixture(registryWith(BILLING, DataFlow.NONE), new InMemoryStateStore());
        f.bindings.bind(new TriggerBinding("billing-on-order", EventFilter.ofType(ORDER_PLACED), BILLING));

        CloudEvent unrelated = io.cloudevents.core.builder.CloudEventBuilder.v1(orderPlaced("evt-7", "A-7"))
                .withType("io.acme.order.cancelled.v1").build();
        f.bus.publish(unrelated).toCompletableFuture().get(5, TimeUnit.SECONDS); // completes normally = ack

        assertThat(f.transport.dispatchCount()).isZero();
        assertThat(f.state.load(WorkflowEngine.triggeredInstanceId(BILLING, unrelated))
                .toCompletableFuture().get(5, TimeUnit.SECONDS)).isEmpty();
    }

    @Test
    void bound_definition_missing_from_this_engine_is_not_acked() throws Exception {
        WorkflowRegistry bindingRegistry = registryWith(BILLING, DataFlow.NONE);
        WorkflowRegistry engineRegistry = new WorkflowRegistry(); // deploy skew: this engine lacks BILLING
        DomainEventFixture f = new DomainEventFixture(bindingRegistry, engineRegistry, new InMemoryStateStore());
        f.bindings.bind(new TriggerBinding("billing-on-order", EventFilter.ofType(ORDER_PLACED), BILLING));

        assertThatThrownBy(() -> f.bus.publish(orderPlaced("evt-8", "A-8")).toCompletableFuture().get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("Unknown workflow");
        assertThat(f.transport.dispatchCount()).isZero();
    }

    @Test
    void local_execution_failure_persists_a_failed_instance_and_is_acked() throws Exception {
        // input.from raises a jq error: the instance is created as FAILED — redelivering would fail the same way.
        DataFlow failing = new DataFlow(Expr.jq("error(\"bad order payload\")"), null, null);
        DomainEventFixture f = new DomainEventFixture(registryWith(BILLING, failing), new InMemoryStateStore());
        f.bindings.bind(new TriggerBinding("billing-on-order", EventFilter.ofType(ORDER_PLACED), BILLING));
        CloudEvent event = orderPlaced("evt-9", "A-9");

        f.bus.publish(event).toCompletableFuture().get(5, TimeUnit.SECONDS); // acked

        assertThat(f.load(WorkflowEngine.triggeredInstanceId(BILLING, event)).status()).isEqualTo(InstanceStatus.FAILED);
        assertThat(f.transport.dispatchCount()).isZero();
    }

    @Test
    void a_reaction_that_throws_fails_the_event_instead_of_escaping() {
        InMemoryDomainEventBus bus = new InMemoryDomainEventBus();
        new DomainEventRouter(List.of(
                event -> CompletableFuture.completedFuture(Reacted.MATCHED),
                event -> {
                    throw new IllegalStateException("reaction blew up");
                })).subscribeTo(bus);

        assertThatThrownBy(() -> bus.publish(orderPlaced("evt-10", "A-10")).toCompletableFuture().get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("reaction blew up");
    }

    private static WorkflowRegistry registryWith(WorkflowRef ref, DataFlow flow) {
        WorkflowRegistry registry = new WorkflowRegistry();
        registry.register(WorkflowBuilder.create().callRemote("handle", "route-handle", flow).build(ref));
        return registry;
    }
}
