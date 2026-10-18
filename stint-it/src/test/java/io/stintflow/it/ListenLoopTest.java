package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.model.FlowDirective;
import io.stintflow.core.model.ListenNode;
import io.stintflow.core.model.SwitchNode;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryStateStore;
import io.stintflow.inmemory.InMemoryTaskTransport;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;

/**
 * SDD 3a.10, CA1/CA2: a coordinator that loops over one {@code listen}, waiting again — in the same
 * activation that consumed an event — for the key of the event it just consumed. Before the fix every
 * such save came back {@code CONFLICT} and the instance failed after its retries.
 */
class ListenLoopTest {

    private static final WorkflowRef REF = new WorkflowRef("delivery", "collect-events", "1.0.0");
    private static final String TYPE = "io.acme.sdd.done.v1";
    private static final int N = 5;

    @Test
    void a_coordinator_waits_for_n_events_in_arrival_order_on_one_listen() throws Exception {
        ListenNode.Filter byInstance = new ListenNode.Filter(TYPE, Map.of(), new TreeMap<>(Map.of("instance",
                new ListenNode.Correlation(Expr.jq(".data.instanceId"), Expr.jq("$workflow.id")))));
        WorkflowRegistry registry = new WorkflowRegistry(Duration.ofMinutes(15));
        registry.register(WorkflowBuilder.create()
                .listen("await", ListenNode.Strategy.ONE, List.of(byInstance), null, null,
                        new DataFlow(null, null, Expr.jq("{notes: (($context.notes // []) + [.[0].note])}")))
                .switchOn("more", DataFlow.NONE, List.of(
                        new SwitchNode.Case("again", Expr.jq("($context.notes | length) < " + N),
                                FlowDirective.goTo("await")),
                        new SwitchNode.Case("done", null, FlowDirective.goTo("finish"))))
                .set("finish", null, new DataFlow(null, Expr.jq("$context.notes"), null), FlowDirective.END)
                .build(REF));
        InMemoryStateStore state = new InMemoryStateStore();
        WorkflowEngine engine = new WorkflowEngine(registry, new InMemoryTaskTransport(), state,
                new InMemoryTimerService(), new FilesystemBlobStore(Files.createTempDirectory("stint-3a10-blobs")));

        String id = engine.start(REF, Json.obj());
        // Arrival order is not event-id order; each delivery completes before the next one is sent.
        for (String eventId : List.of("e3", "e1", "e5", "e2", "e4")) {
            assertThat(engine.onDomainEvent(event(eventId, id), Duration.ofMinutes(1)).toCompletableFuture()
                    .get(10, TimeUnit.SECONDS)).isTrue();
        }

        JsonNode notes = engine.awaitCompletion(id, Duration.ofSeconds(10)).toCompletableFuture()
                .get(15, TimeUnit.SECONDS);
        assertThat(notes.get("notes")).extracting(JsonNode::asText).containsExactly("e3", "e1", "e5", "e2", "e4");
    }

    private static CloudEvent event(String eventId, String instanceId) {
        ObjectNode data = Json.obj();
        data.put("instanceId", instanceId);
        data.put("note", eventId);
        return CloudEventBuilder.v1().withId(eventId).withSource(URI.create("https://acme.example/sdd"))
                .withType(TYPE).withDataContentType("application/json").withData(Json.bytes(data)).build();
    }
}
