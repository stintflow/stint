package io.stintflow.inmemory;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.trigger.DomainEventRouter;
import io.stintflow.core.trigger.EventFilter;
import io.stintflow.core.trigger.StartReaction;
import io.stintflow.core.trigger.TriggerBinding;
import io.stintflow.core.trigger.TriggerBindings;
import io.stintflow.spi.ExecutionTracer;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.TraceAttributes;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.spi.wire.StintEvents;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerRuntime;

/**
 * SDD 2.5: CA1 (one chain across two workflows, B's cause is A's fact), CA3 (a legacy event without
 * extensions starts a new chain), CA7 ({@code TaskContext.chainId()} and the MDC) and CA9 (a result the
 * in-memory transport fabricates keeps the invocation's chain). Real worker, real CloudEvents.
 */
class ChainPropagationTest {

    private static final WorkflowRef A = new WorkflowRef("meetings", "transcribe", "1.0.0");
    private static final WorkflowRef B = new WorkflowRef("minutes", "write-minutes", "1.0.0");
    private static final String RECORDED = "io.acme.meeting.recorded.v1";
    private static final String TRANSCRIBED = "io.acme.meeting.transcribed.v1";

    @Test
    void ca1_two_workflows_share_one_chain_and_b_is_caused_by_as_fact() throws Exception {
        Fixture f = new Fixture();
        CloudEvent trigger = CloudEventBuilder.v1().withId("rec-1").withSource(URI.create("https://acme.example/teams"))
                .withType(RECORDED).withExtension(StintEvents.EXT_CHAIN_ID, "chn-root").build();

        f.bus.publish(trigger).toCompletableFuture().get(5, TimeUnit.SECONDS);
        CloudEvent fact = f.factSeen.get(10, TimeUnit.SECONDS);
        f.engine.awaitCompletion(WorkflowEngine.triggeredInstanceId(A, trigger), Duration.ofSeconds(10))
                .toCompletableFuture().get(15, TimeUnit.SECONDS);
        f.engine.awaitCompletion(WorkflowEngine.triggeredInstanceId(B, fact), Duration.ofSeconds(10))
                .toCompletableFuture().get(15, TimeUnit.SECONDS);

        // Every event the Stint created — A's invoke/result, A's fact, B's invoke/result — is on the same chain.
        assertThat(f.invokes).hasSize(2);
        assertThat(f.results).hasSize(2);
        List<CloudEvent> all = new ArrayList<>(f.invokes);
        all.addAll(f.results);
        all.add(fact);
        assertThat(all).allSatisfy(e -> assertThat(e.getExtension(StintEvents.EXT_CHAIN_ID)).isEqualTo("chn-root"));

        CloudEvent invokeA = byWorkflow(f.invokes, A);
        CloudEvent invokeB = byWorkflow(f.invokes, B);
        // A was caused by the trigger; A's fact by the result that resumed A; B by A's fact.
        assertThat(invokeA.getExtension(StintEvents.EXT_CAUSATION_ID)).isEqualTo("rec-1");
        assertThat(fact.getExtension(StintEvents.EXT_CAUSATION_ID)).isEqualTo(invokeA.getId());
        assertThat(invokeB.getExtension(StintEvents.EXT_CAUSATION_ID)).isEqualTo(fact.getId());
        // A result's cause is the invoke it answers.
        assertThat(f.results).allSatisfy(r ->
                assertThat(r.getExtension(StintEvents.EXT_CAUSATION_ID)).isEqualTo(r.getId()));
    }

    @Test
    void legacy_event_without_extensions_starts_a_new_chain() throws Exception {
        Fixture f = new Fixture();
        CloudEvent legacy = CloudEventBuilder.v1().withId("rec-legacy").withSource(URI.create("https://acme.example/teams"))
                .withType(RECORDED).build(); // pre-2.5 format: no chainid/causationid/traceparent

        f.bus.publish(legacy).toCompletableFuture().get(5, TimeUnit.SECONDS);
        f.engine.awaitCompletion(WorkflowEngine.triggeredInstanceId(A, legacy), Duration.ofSeconds(10))
                .toCompletableFuture().get(15, TimeUnit.SECONDS);

        String expected = "chn-" + UUID.nameUUIDFromBytes(("chain|https://acme.example/teams|rec-legacy")
                .getBytes(StandardCharsets.UTF_8));
        assertThat(byWorkflow(f.invokes, A).getExtension(StintEvents.EXT_CHAIN_ID)).isEqualTo(expected);
        assertThat(f.factSeen.get(10, TimeUnit.SECONDS).getExtension(StintEvents.EXT_CHAIN_ID)).isEqualTo(expected);
    }

    @Test
    void ca7_task_context_and_mdc_carry_the_chain_during_the_handler_and_the_activation() throws Exception {
        Fixture f = new Fixture();
        CloudEvent trigger = CloudEventBuilder.v1().withId("rec-7").withSource(URI.create("https://acme.example/teams"))
                .withType(RECORDED).withExtension(StintEvents.EXT_CHAIN_ID, "chn-seven").build();

        f.bus.publish(trigger).toCompletableFuture().get(5, TimeUnit.SECONDS);
        JsonNode resultA = f.engine.awaitCompletion(WorkflowEngine.triggeredInstanceId(A, trigger), Duration.ofSeconds(10))
                .toCompletableFuture().get(15, TimeUnit.SECONDS);

        // Worker side: TaskContext.chainId() and the MDC while the handler ran.
        assertThat(resultA.get("ctxChain").asText()).isEqualTo("chn-seven");
        assertThat(resultA.get("mdcChain").asText()).isEqualTo("chn-seven");
        assertThat(resultA.get("mdcInstance").asText()).isEqualTo(WorkflowEngine.triggeredInstanceId(A, trigger));
        // Engine side: the MDC while the activation ran the local 'set' after the task.
        assertThat(resultA.get("engineMdcChain").asText()).isEqualTo("chn-seven");
        assertThat(MDC.get(TraceAttributes.CHAIN_ID)).isNull(); // restored afterwards
    }

    @Test
    void ca9_a_result_fabricated_by_the_in_memory_transport_keeps_the_invocation_chain() throws Exception {
        InMemoryTaskTransport transport = new InMemoryTaskTransport();
        transport.connectWorker(invoke -> CompletableFuture.failedFuture(new IllegalStateException("worker crashed")));
        CompletableFuture<TaskResult> received = new CompletableFuture<>();
        transport.onResult(result -> {
            received.complete(result);
            return CompletableFuture.completedFuture(null);
        });

        transport.dispatch(new TaskInvocation("inst-9", "/do/0/work", "corr-9", A, "route-a", Json.obj(), 1,
                new Lineage("chn-nine", "evt-8", null, null)));

        TaskResult result = received.get(5, TimeUnit.SECONDS);
        assertThat(result.status()).isEqualTo(TaskResult.Status.FAILED);
        assertThat(result.lineage().chainId()).isEqualTo("chn-nine");
        assertThat(result.lineage().causationId()).isEqualTo("corr-9");
    }

    private static CloudEvent byWorkflow(List<CloudEvent> invokes, WorkflowRef ref) {
        return invokes.stream().filter(e -> ref.canonical().equals(e.getExtension(StintEvents.EXT_DEFINITION)))
                .findFirst().orElseThrow();
    }

    /** A: task + emit TRANSCRIBED; B (bound to TRANSCRIBED): task. Real worker, publisher looped back to the bus. */
    private static final class Fixture {
        final List<CloudEvent> invokes = new CopyOnWriteArrayList<>();
        final List<CloudEvent> results = new CopyOnWriteArrayList<>();
        final CompletableFuture<CloudEvent> factSeen = new CompletableFuture<>();
        final InMemoryDomainEventBus bus = new InMemoryDomainEventBus();
        final WorkflowEngine engine;

        Fixture() throws Exception {
            WorkflowRegistry registry = new WorkflowRegistry();
            registry.register(WorkflowBuilder.create()
                    .callRemote("transcribe", "route-transcribe", new DataFlow(null, null, Expr.jq(".")))
                    .set("engineLog", Expr.of((ctx, data) -> {
                        ObjectNode out = ((ObjectNode) data).deepCopy();
                        out.put("engineMdcChain", String.valueOf(MDC.get(TraceAttributes.CHAIN_ID)));
                        return out;
                    }), new DataFlow(null, null, Expr.jq(".")))
                    .emit("announce", TRANSCRIBED, Expr.jq(
                            "{source: \"https://acme.example/meetings\", type: \"" + TRANSCRIBED + "\", data: {}}"),
                            DataFlow.NONE)
                    .build(A));
            registry.register(WorkflowBuilder.create()
                    .callRemote("write", "route-write", DataFlow.NONE)
                    .build(B));

            var blob = new FilesystemBlobStore(Files.createTempDirectory("stint-sdd25"));
            TaskHandlerRegistry handlers = new TaskHandlerRegistry()
                    .register("route-transcribe", ctx -> {
                        ObjectNode out = Json.obj();
                        out.put("ctxChain", String.valueOf(ctx.chainId()));
                        out.put("mdcChain", String.valueOf(MDC.get(TraceAttributes.CHAIN_ID)));
                        out.put("mdcInstance", String.valueOf(MDC.get(TraceAttributes.INSTANCE_ID)));
                        return CompletableFuture.completedFuture(out);
                    })
                    .register("route-write", ctx -> CompletableFuture.completedFuture(Json.obj()));
            WorkerRuntime worker = new WorkerRuntime(handlers, blob);
            InMemoryTaskTransport transport = new InMemoryTaskTransport();
            transport.connectWorker(invoke -> {
                invokes.add(invoke);
                return worker.handle(invoke).thenApply(result -> {
                    results.add(result);
                    return result;
                });
            });

            InMemoryEventPublisher publisher = new InMemoryEventPublisher();
            publisher.connectTo(bus);
            publisher.subscribe(event -> {
                factSeen.complete(event);
                return CompletableFuture.completedFuture(null);
            });
            engine = new WorkflowEngine(registry, transport, new InMemoryStateStore(), new InMemoryTimerService(), blob,
                    new TreeInterpreter(new JqExpressionEvaluator()), InstantSource.system(), publisher, null,
                    ExecutionTracer.NOOP);
            TriggerBindings bindings = new TriggerBindings(registry);
            bindings.bind(new TriggerBinding("a-on-recorded", EventFilter.ofType(RECORDED), A));
            bindings.bind(new TriggerBinding("b-on-transcribed", EventFilter.ofType(TRANSCRIBED), B));
            new DomainEventRouter(List.of(new StartReaction(bindings, engine))).subscribeTo(bus);
        }
    }
}
