package io.stintflow.inmemory;

import static io.stintflow.inmemory.EmitPublishTest.order;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.cloudevents.CloudEvent;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.DataFlow;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.spi.wire.StintEvents;
import io.stintflow.wire.Json;

/**
 * SDD 2.2, CA3: a subscriber of the domain channel only ever sees domain facts — the engine's own
 * traffic for the same instance (two task invokes, their results, the timers guarding them) goes over
 * the task transport and the timer service, never through the {@code EventPublisher}.
 */
class ChannelSeparationTest {

    private static final WorkflowRef REF = new WorkflowRef("billing", "invoice-order", "1.0.0");
    private static final String INVOICED = "io.acme.order.invoiced.v1";

    @Test
    void ca3_domain_subscriber_never_receives_internal_events() throws Exception {
        WorkflowDefinition def = WorkflowBuilder.create()
                .callRemote("price", "route-price", DataFlow.NONE)
                .emit("announce", INVOICED, Expr.jq(
                        "{source: \"https://acme.example/billing\", type: \"" + INVOICED + "\", data: .}"), DataFlow.NONE)
                .callRemote("archive", "route-archive", DataFlow.NONE)
                .build(REF);
        EmitFixture f = new EmitFixture(def);
        List<CloudEvent> domainSubscriber = new CopyOnWriteArrayList<>();
        f.publisher.subscribe(event -> {
            domainSubscriber.add(event);
            return CompletableFuture.completedFuture(null);
        });

        String instanceId = f.engine.start(REF, order("A-1", 1));
        TaskInvocation price = f.transport.awaitDispatch();
        f.transport.deliverResult(TaskResult.completed(price.correlationId(), Json.obj()))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
        TaskInvocation archive = f.transport.awaitDispatch();
        f.transport.deliverResult(TaskResult.completed(archive.correlationId(), Json.obj()))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertThat(f.load(instanceId).status()).isEqualTo(InstanceStatus.COMPLETED);
        assertThat(f.transport.dispatchCount()).isEqualTo(2); // internal traffic did happen...
        assertThat(domainSubscriber).singleElement().satisfies(e -> {   // ...but not on the domain channel
            assertThat(e.getType()).isEqualTo(INVOICED);
            assertThat(StintEvents.isReservedType(e.getType())).isFalse();
        });
        assertThat(f.outbox()).isEmpty(); // CA6
    }
}
