package io.stintflow.core.trigger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.model.DataFlow;
import io.stintflow.spi.WorkflowRef;

/** SDD 2.1, RF3/sec. 8b: event → definition bindings, exact-match filter, fan-out and dedup. */
class TriggerBindingsTest {

    private static final WorkflowRef BILLING = new WorkflowRef("billing", "invoice-order", "1.0.0");
    private static final WorkflowRef SHIPPING = new WorkflowRef("shipping", "ship-order", "1.0.0");
    private static final String ORDER_PLACED = "io.acme.order.placed.v1";

    @Test
    void matches_on_type_and_exact_with_attributes_including_extensions() {
        EventFilter filter = new EventFilter(ORDER_PLACED,
                Map.of("source", "https://acme.example/orders", "tenant", "acme"));

        assertThat(filter.matches(event(ORDER_PLACED, "https://acme.example/orders", "acme"))).isTrue();
        assertThat(filter.matches(event(ORDER_PLACED, "https://acme.example/orders", "other"))).isFalse();
        assertThat(filter.matches(event(ORDER_PLACED, "https://elsewhere.example", "acme"))).isFalse();
        assertThat(filter.matches(event("io.acme.order.cancelled.v1", "https://acme.example/orders", "acme"))).isFalse();
    }

    @Test
    void an_attribute_the_event_does_not_carry_does_not_match() {
        EventFilter filter = new EventFilter(ORDER_PLACED, Map.of("subject", "order-1"));

        assertThat(filter.matches(event(ORDER_PLACED, "https://acme.example/orders", "acme"))).isFalse();
    }

    @Test
    void type_is_required() {
        assertThatThrownBy(() -> new EventFilter(" ", Map.of())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void one_event_fans_out_to_every_distinct_bound_definition() {
        TriggerBindings bindings = new TriggerBindings(registryWith(BILLING, SHIPPING));
        bindings.bind(new TriggerBinding("billing-on-order", EventFilter.ofType(ORDER_PLACED), BILLING));
        bindings.bind(new TriggerBinding("shipping-on-order", EventFilter.ofType(ORDER_PLACED), SHIPPING));
        // A second binding of the SAME definition that also matches: still started once.
        bindings.bind(new TriggerBinding("billing-on-acme-order",
                new EventFilter(ORDER_PLACED, Map.of("tenant", "acme")), BILLING));

        assertThat(bindings.targetsFor(event(ORDER_PLACED, "https://acme.example/orders", "acme")))
                .containsExactly(BILLING, SHIPPING);
    }

    @Test
    void no_binding_matches_yields_no_target() {
        TriggerBindings bindings = new TriggerBindings(registryWith(BILLING));
        bindings.bind(new TriggerBinding("billing-on-order", EventFilter.ofType(ORDER_PLACED), BILLING));

        assertThat(bindings.targetsFor(event("io.acme.unrelated.v1", "https://acme.example", "acme"))).isEmpty();
    }

    @Test
    void unbind_stops_matching() {
        TriggerBindings bindings = new TriggerBindings(registryWith(BILLING));
        bindings.bind(new TriggerBinding("billing-on-order", EventFilter.ofType(ORDER_PLACED), BILLING));
        bindings.unbind("billing-on-order");

        assertThat(bindings.targetsFor(event(ORDER_PLACED, "https://acme.example/orders", "acme"))).isEmpty();
    }

    @Test
    void binding_to_an_unregistered_definition_or_a_duplicate_id_is_rejected() {
        TriggerBindings bindings = new TriggerBindings(registryWith(BILLING));

        assertThatThrownBy(() -> bindings.bind(new TriggerBinding("x", EventFilter.ofType(ORDER_PLACED), SHIPPING)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not registered");

        bindings.bind(new TriggerBinding("dup", EventFilter.ofType(ORDER_PLACED), BILLING));
        assertThatThrownBy(() -> bindings.bind(new TriggerBinding("dup", EventFilter.ofType(ORDER_PLACED), BILLING)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("already bound");
    }

    private static WorkflowRegistry registryWith(WorkflowRef... refs) {
        WorkflowRegistry registry = new WorkflowRegistry();
        for (WorkflowRef ref : refs) {
            registry.register(WorkflowBuilder.create().callRemote("step", "route", DataFlow.NONE).build(ref));
        }
        return registry;
    }

    private static CloudEvent event(String type, String source, String tenant) {
        return CloudEventBuilder.v1()
                .withId("evt-1")
                .withSource(URI.create(source))
                .withType(type)
                .withExtension("tenant", tenant)
                .build();
    }
}
