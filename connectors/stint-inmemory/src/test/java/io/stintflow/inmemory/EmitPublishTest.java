package io.stintflow.inmemory;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.cloudevents.CloudEvent;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.DataFlow;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.ClaimCheck;
import io.stintflow.wire.Json;

/** SDD 2.2: CA1 (emit publishes the declared fact), CA4 in-memory (large data → dataref) and CA6 (outbox drained). */
class EmitPublishTest {

    private static final WorkflowRef REF = new WorkflowRef("billing", "invoice-order", "1.0.0");
    private static final String INVOICED = "io.acme.order.invoiced.v1";

    @Test
    void ca1_emit_publishes_the_fact_with_the_declared_attributes_and_data() throws Exception {
        WorkflowDefinition def = WorkflowBuilder.create()
                .emit("announce", INVOICED, Expr.jq("{source: \"https://acme.example/billing\", type: \""
                        + INVOICED + "\", subject: .orderId, datacontenttype: \"application/json\", tenant: \"acme\","
                        + " priority: 2, data: {orderId: .orderId, total: .total}}"), DataFlow.NONE)
                .build(REF);
        EmitFixture f = new EmitFixture(def);

        String instanceId = f.engine.start(REF, order("A-1", 42));

        assertThat(f.publisher.published()).hasSize(1);
        CloudEvent fact = f.publisher.published().get(0);
        assertThat(fact.getId()).isNotBlank();
        assertThat(fact.getSource()).isEqualTo(URI.create("https://acme.example/billing"));
        assertThat(fact.getType()).isEqualTo(INVOICED);
        assertThat(fact.getSubject()).isEqualTo("A-1");
        assertThat(fact.getDataContentType()).isEqualTo("application/json");
        assertThat(fact.getExtension("tenant")).isEqualTo("acme");
        assertThat(fact.getExtension("priority")).isEqualTo(2);
        assertThat(fact.getTime()).isNotNull();
        JsonNode data = Json.read(fact.getData().toBytes());
        assertThat(data.get("orderId").asText()).isEqualTo("A-1");
        assertThat(data.get("total").asInt()).isEqualTo(42);

        assertThat(f.load(instanceId).status()).isEqualTo(InstanceStatus.COMPLETED);
        assertThat(f.outbox()).isEmpty(); // CA6
    }

    @Test
    void data_above_the_publishers_limit_goes_to_the_facts_store_as_a_dataref_readable_after_completion()
            throws Exception {
        WorkflowDefinition def = WorkflowBuilder.create()
                .emit("announce", INVOICED, Expr.jq("{source: \"https://acme.example/billing\", type: \""
                        + INVOICED + "\", data: {orderId: .orderId, lines: [range(0; 200) | {n: ., sku: \"SKU-LONG-NAME\"}]}}"),
                        DataFlow.NONE)
                .build(REF);
        EmitFixture f = new EmitFixture(new InMemoryStateStore(), new InMemoryEventPublisher(1_024), def);

        String instanceId = f.engine.start(REF, order("A-2", 7));
        assertThat(f.load(instanceId).status()).isEqualTo(InstanceStatus.COMPLETED);

        CloudEvent fact = f.publisher.published().get(0);
        assertThat(fact.getData()).isNull();
        assertThat(fact.getExtension(ClaimCheck.POINTER_FIELD)).isNull(); // never the internal claim-check
        URI dataref = URI.create((String) fact.getExtension("dataref"));
        // An "external consumer": reads the pointer on its own, after the instance finished.
        JsonNode data = Json.read(Files.readAllBytes(Path.of(dataref)));
        assertThat(data.get("orderId").asText()).isEqualTo("A-2");
        assertThat(data.get("lines")).hasSize(200);
        assertThat(dataref.toString()).contains("facts_billing_invoice-order_" + fact.getId());
        assertThat(f.outbox()).isEmpty(); // CA6
    }

    static ObjectNode order(String orderId, int total) {
        ObjectNode input = Json.obj();
        input.put("orderId", orderId);
        input.put("total", total);
        return input;
    }
}
