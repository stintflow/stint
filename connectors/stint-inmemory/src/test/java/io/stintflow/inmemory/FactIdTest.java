package io.stintflow.inmemory;

import static io.stintflow.inmemory.EmitPublishTest.order;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.cloudevents.CloudEvent;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.model.FlowDirective;
import io.stintflow.core.model.SwitchNode;
import io.stintflow.spi.WorkflowRef;

/**
 * SDD 2.2, CA7 (sec. 8b): the author's {@code id} is used as declared; without one, the engine's id is
 * {@code UUIDv3(instance | baseVersion | node pointer | occurrence)} — distinct for each pass through
 * the same node in a local loop, and reproducible (the same inputs always yield the same id).
 */
class FactIdTest {

    private static final WorkflowRef REF = new WorkflowRef("billing", "ids", "1.0.0");
    private static final String TYPE = "io.acme.tick.v1";

    @Test
    void the_authors_id_is_used_as_declared() throws Exception {
        EmitFixture f = new EmitFixture(WorkflowBuilder.create()
                .emit("announce", TYPE, Expr.jq("{id: (\"order-\" + .orderId), source: \"https://acme.example\","
                        + " type: \"" + TYPE + "\"}"), DataFlow.NONE)
                .build(REF));

        f.engine.start(REF, order("A-7", 1));

        assertThat(f.publisher.published()).singleElement()
                .satisfies(e -> assertThat(e.getId()).isEqualTo("order-A-7"));
    }

    @Test
    void without_an_id_each_pass_of_a_loop_gets_a_distinct_reproducible_id() throws Exception {
        // count=0 -> tick(emit) -> inc -> loop back to tick while count < 2 -> end: 2 emits of /do/1/tick.
        EmitFixture f = new EmitFixture(WorkflowBuilder.create()
                .set("init", Expr.jq("{count: 0}"), DataFlow.NONE)
                .emit("tick", TYPE, Expr.jq("{source: \"https://acme.example\", type: \"" + TYPE + "\", data: .}"),
                        DataFlow.NONE)
                .set("inc", Expr.jq("{count: (.count + 1)}"), DataFlow.NONE)
                .switchOn("again", DataFlow.NONE, List.of(
                        new SwitchNode.Case("more", Expr.jq(".count < 2"), FlowDirective.goTo("tick")),
                        new SwitchNode.Case("done", null, FlowDirective.END)))
                .build(REF));

        String instanceId = f.engine.start(REF, order("A-1", 1));

        List<String> ids = f.publisher.published().stream().map(CloudEvent::getId).toList();
        // Started at version 0 (a new instance); the same node, occurrences 0 and 1.
        assertThat(ids).containsExactly(expectedId(instanceId, 0, "/do/1/tick", 0),
                expectedId(instanceId, 0, "/do/1/tick", 1));
        assertThat(f.outbox()).isEmpty(); // CA6
    }

    private static String expectedId(String instanceId, long baseVersion, String pointer, int occurrence) {
        String name = instanceId + '|' + baseVersion + '|' + pointer + '|' + occurrence;
        return UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8)).toString();
    }
}
