package io.stintflow.inmemory;

import static io.stintflow.inmemory.DomainEventFixture.ORDER_PLACED;
import static io.stintflow.inmemory.DomainEventFixture.ORDERS_SOURCE;
import static io.stintflow.inmemory.DomainEventFixture.orderPlaced;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.time.InstantSource;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.cloudevents.CloudEvent;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.trigger.EventFilter;
import io.stintflow.core.trigger.TriggerBinding;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;

/**
 * SDD 2.1, CA6 (RF6): {@code $workflow} is the DSL 1.0 workflow descriptor — {@code id}, the raw
 * {@code input} (for an event-started instance, the array of triggering events) and {@code startedAt}
 * — and it survives a resume on a <em>different</em> engine, because it lives in the snapshot.
 */
class WorkflowDescriptorTest {

    private static final WorkflowRef REF = new WorkflowRef("billing", "invoice-order", "1.0.0");

    @Test
    void trigger_event_is_available_in_workflow_input_after_a_resume_on_another_engine() throws Exception {
        WorkflowRegistry registry = new WorkflowRegistry();
        registry.register(WorkflowBuilder.create()
                .callRemote("first", "route-first", DataFlow.NONE)
                .callRemote("second", "route-second", new DataFlow(Expr.jq(
                        "{eventId: $workflow.input[0].id, eventSource: $workflow.input[0].source,"
                                + " instance: $workflow.id, startedAt: $workflow.startedAt.iso8601,"
                                + " definition: $workflow.name}"), null, null))
                .build(REF));
        InMemoryStateStore sharedState = new InMemoryStateStore();

        // Engine A: starts the instance from the event and dispatches "first".
        DomainEventFixture engineA = new DomainEventFixture(registry, sharedState);
        engineA.bindings.bind(new TriggerBinding("billing-on-order", EventFilter.ofType(ORDER_PLACED), REF));
        CloudEvent event = orderPlaced("evt-42", "A-1");
        engineA.bus.publish(event).toCompletableFuture().get(5, TimeUnit.SECONDS);
        TaskInvocation first = engineA.transport.awaitDispatch();

        // Engine B: a separate engine (own transport/timer) sharing only the StateStore receives the result.
        DomainEventFixture.CapturingTransport transportB = new DomainEventFixture.CapturingTransport();
        new WorkflowEngine(registry, transportB, sharedState, new InMemoryTimerService(InstantSource.system()),
                new FilesystemBlobStore(Files.createTempDirectory("stint-ca6-blobs")));
        transportB.deliverResult(TaskResult.completed(first.correlationId(), Json.obj()))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        TaskInvocation second = transportB.awaitDispatch();
        JsonNode input = second.input();
        String instanceId = WorkflowEngine.triggeredInstanceId(REF, event);
        assertThat(input.get("eventId").asText()).isEqualTo("evt-42");
        assertThat(input.get("eventSource").asText()).isEqualTo(ORDERS_SOURCE.toString());
        assertThat(input.get("instance").asText()).isEqualTo(instanceId);
        assertThat(input.get("startedAt").asText()).isEqualTo(engineA.load(instanceId).startedAt().toString());
        assertThat(input.get("definition").asText()).isEqualTo("invoice-order"); // Stint extension, kept
    }
}
