package io.stintflow.otel;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.model.FlowDirective;
import io.stintflow.core.model.ListenNode;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryStateStore;
import io.stintflow.inmemory.InMemoryTaskTransport;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.spi.AdapterCapabilities;
import io.stintflow.spi.AdapterCapabilities.DeliveryGuarantee;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResultHandler;
import io.stintflow.spi.TaskTransport;
import io.stintflow.spi.TraceAttributes;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.spi.wire.StintEvents;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerRuntime;

/**
 * SDD 2.5 with a real OpenTelemetry SDK (in memory):
 * <ul>
 *   <li>CA2 — one trace covers engine and worker: activation → task (child) → resume activation (child of
 *       the task), all short spans, one {@code traceId};</li>
 *   <li>CA4 — after a wait (timer) the resume is a new trace with a <em>link</em> to the span that suspended,
 *       never a child; a start by another workflow's event is likewise a new trace linked to that event;</li>
 *   <li>SDD 2.3, RF6 — a resume by a {@code listen}ed event is a new trace linked to the event and to the
 *       span that suspended;</li>
 *   <li>CA8 — every recorded attribute is an identifier from {@link TraceAttributes#ALLOWED}.</li>
 * </ul>
 */
class OpenTelemetryTraceTest {

    private static final WorkflowRef REF = new WorkflowRef("billing", "invoice", "1.0.0");

    @Test
    void ca2_one_trace_covers_the_engine_and_the_worker() throws Exception {
        Spans spans = new Spans();
        OpenTelemetryExecutionTracer tracer = spans.tracer();
        WorkflowRegistry registry = registryWith(WorkflowBuilder.create()
                .callRemote("price", "route-price", DataFlow.NONE).build(REF));
        var blob = new FilesystemBlobStore(Files.createTempDirectory("stint-otel-ca2"));
        InMemoryTaskTransport transport = new InMemoryTaskTransport();
        transport.connectWorker(new WorkerRuntime(new TaskHandlerRegistry()
                .register("route-price", ctx -> CompletableFuture.completedFuture(Json.obj())), blob, tracer)::handle);
        WorkflowEngine engine = new WorkflowEngine(registry, transport, new InMemoryStateStore(), new InMemoryTimerService(),
                blob, new TreeInterpreter(new JqExpressionEvaluator()), InstantSource.system(), null, null, tracer);

        engine.startAndWait(REF, Json.obj()).toCompletableFuture().get(10, TimeUnit.SECONDS);
        List<SpanData> ended = spans.await(3);

        // Identified by structure, not by end order: the dispatch is asynchronous, so the start activation's
        // span may well end after the resume's.
        SpanData task = byName(ended, "stint.task", 0);
        SpanData start = ended.stream().filter(sp -> sp.getName().equals("stint.activation")
                && !sp.getParentSpanContext().isValid()).findFirst().orElseThrow();
        SpanData resume = ended.stream().filter(sp -> sp.getName().equals("stint.activation") && sp != start)
                .findFirst().orElseThrow();
        assertThat(ended).extracting(SpanData::getTraceId).containsOnly(start.getTraceId());
        assertThat(task.getParentSpanId()).isEqualTo(start.getSpanId());   // engine -> worker: child
        assertThat(resume.getParentSpanId()).isEqualTo(task.getSpanId());  // worker -> engine: child
        assertThat(resume.getLinks()).isEmpty();
        assertIdentifiersOnly(ended);
    }

    @Test
    void ca4_resume_after_a_wait_links_instead_of_parenting() throws Exception {
        Spans spans = new Spans();
        MutableClock clock = new MutableClock();
        InMemoryTimerService timer = new InMemoryTimerService(clock);
        WorkflowRegistry registry = registryWith(WorkflowBuilder.create()
                .callRemote("price", "route-price", DataFlow.NONE, FlowDirective.CONTINUE, Duration.ofMinutes(1))
                .build(REF));
        WorkflowEngine engine = new WorkflowEngine(registry, new SilentTransport(), new InMemoryStateStore(), timer,
                new FilesystemBlobStore(Files.createTempDirectory("stint-otel-ca4")),
                new TreeInterpreter(new JqExpressionEvaluator()), clock, null, null, spans.tracer());

        engine.start(REF, Json.obj());
        SpanData suspending = spans.await(1).get(0);
        clock.advance(Duration.ofMinutes(2));
        timer.tick(); // the worker never answered: the timeout resumes the instance

        SpanData resumed = spans.await(2).get(1);
        assertThat(resumed.getParentSpanContext().isValid()).isFalse();                  // not a child...
        assertThat(resumed.getTraceId()).isNotEqualTo(suspending.getTraceId());         // ...a new trace...
        assertThat(resumed.getLinks()).extracting(l -> l.getSpanContext().getSpanId())  // ...linked to who suspended
                .containsExactly(suspending.getSpanId());
        assertThat(resumed.getAttributes().get(AttributeKey.stringKey(TraceAttributes.CAUSATION_ID))).startsWith("timer:");
        assertIdentifiersOnly(spans.await(2));
    }

    @Test
    void ca4_a_start_by_another_workflows_event_is_a_new_trace_linked_to_it() throws Exception {
        Spans spans = new Spans();
        WorkflowRegistry registry = registryWith(WorkflowBuilder.create()
                .callRemote("price", "route-price", DataFlow.NONE).build(REF));
        WorkflowEngine engine = new WorkflowEngine(registry, new SilentTransport(), new InMemoryStateStore(),
                new InMemoryTimerService(InstantSource.system()),
                new FilesystemBlobStore(Files.createTempDirectory("stint-otel-fact")),
                new TreeInterpreter(new JqExpressionEvaluator()), InstantSource.system(), null, null, spans.tracer());
        String upstreamTrace = "4bf92f3577b34da6a3ce929d0e0e4736";
        String upstreamSpan = "00f067aa0ba902b7";
        CloudEvent fact = CloudEventBuilder.v1().withId("fact-1").withSource(URI.create("https://acme.example/a"))
                .withType("io.acme.done.v1")
                .withExtension(StintEvents.EXT_TRACEPARENT, "00-" + upstreamTrace + "-" + upstreamSpan + "-01").build();

        engine.startFromEvent(REF, fact).toCompletableFuture().get(5, TimeUnit.SECONDS);

        SpanData started = spans.await(1).get(0);
        assertThat(started.getParentSpanContext().isValid()).isFalse();
        assertThat(started.getTraceId()).isNotEqualTo(upstreamTrace);
        assertThat(started.getLinks()).singleElement().satisfies(link -> {
            SpanContext linked = link.getSpanContext();
            assertThat(linked.getTraceId()).isEqualTo(upstreamTrace);
            assertThat(linked.getSpanId()).isEqualTo(upstreamSpan);
        });
    }

    @Test
    void sdd23_a_resume_by_a_listened_event_links_to_the_event_and_to_the_suspension() throws Exception {
        Spans spans = new Spans();
        String reviewed = "io.acme.review.completed.v1";
        WorkflowRegistry registry = registryWith(WorkflowBuilder.create()
                .listen("awaitReview", ListenNode.Strategy.ONE, List.of(new ListenNode.Filter(reviewed, Map.of(),
                        new TreeMap<>(Map.of("instance", new ListenNode.Correlation(Expr.jq(".data.instanceId"),
                                Expr.jq("$workflow.id")))))), null, null, DataFlow.NONE)
                .build(REF));
        WorkflowEngine engine = new WorkflowEngine(registry, new SilentTransport(), new InMemoryStateStore(),
                new InMemoryTimerService(InstantSource.system()),
                new FilesystemBlobStore(Files.createTempDirectory("stint-otel-listen")),
                new TreeInterpreter(new JqExpressionEvaluator()), InstantSource.system(), null, null, spans.tracer());
        String instanceId = engine.start(REF, Json.obj());
        SpanData suspending = spans.await(1).get(0);
        String upstreamTrace = "4bf92f3577b34da6a3ce929d0e0e4736";
        String upstreamSpan = "00f067aa0ba902b7";
        var data = Json.obj().put("instanceId", instanceId);
        CloudEvent event = CloudEventBuilder.v1().withId("rev-1").withSource(URI.create("https://acme.example/review"))
                .withType(reviewed).withDataContentType("application/json").withData(Json.bytes(data))
                .withExtension(StintEvents.EXT_TRACEPARENT, "00-" + upstreamTrace + "-" + upstreamSpan + "-01").build();

        engine.onDomainEvent(event, Duration.ofMinutes(5)).toCompletableFuture().get(5, TimeUnit.SECONDS);

        SpanData resumed = spans.await(2).get(1);
        assertThat(resumed.getParentSpanContext().isValid()).isFalse(); // a new trace, not a child of the event
        assertThat(resumed.getLinks()).extracting(l -> l.getSpanContext().getSpanId())
                .containsExactlyInAnyOrder(suspending.getSpanId(), upstreamSpan);
        assertThat(resumed.getAttributes().get(AttributeKey.stringKey(TraceAttributes.CAUSATION_ID))).isEqualTo("rev-1");
        assertIdentifiersOnly(spans.await(2));
    }

    private static void assertIdentifiersOnly(Collection<SpanData> spans) {
        assertThat(spans).allSatisfy(span -> assertThat(span.getAttributes().asMap().keySet())
                .extracting(AttributeKey::getKey)
                .allSatisfy(key -> assertThat(TraceAttributes.ALLOWED).contains(key)));
    }

    private static SpanData byName(List<SpanData> spans, String name, int index) {
        return spans.stream().filter(s -> s.getName().equals(name)).toList().get(index);
    }

    private static WorkflowRegistry registryWith(io.stintflow.core.WorkflowDefinition def) {
        WorkflowRegistry registry = new WorkflowRegistry();
        registry.register(def);
        return registry;
    }

    /** An in-memory SDK whose exporter lets the test wait for N ended spans — no sleep. */
    static final class Spans implements SpanExporter {
        private final List<SpanData> ended = new CopyOnWriteArrayList<>();
        private final AtomicReference<CompletableFuture<Void>> goal = new AtomicReference<>(new CompletableFuture<>());
        private volatile int goalCount = Integer.MAX_VALUE;
        private final SdkTracerProvider provider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(this)).build();

        OpenTelemetryExecutionTracer tracer() {
            return new OpenTelemetryExecutionTracer(provider.get("stint-test"));
        }

        synchronized List<SpanData> await(int count) throws Exception {
            CompletableFuture<Void> future = new CompletableFuture<>();
            goal.set(future);
            goalCount = count;
            if (ended.size() >= count) {
                future.complete(null);
            }
            future.get(10, TimeUnit.SECONDS);
            return List.copyOf(ended);
        }

        @Override
        public CompletableResultCode export(Collection<SpanData> spans) {
            ended.addAll(spans);
            if (ended.size() >= goalCount) {
                goal.get().complete(null);
            }
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode flush() {
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode shutdown() {
            return CompletableResultCode.ofSuccess();
        }
    }

    /** Accepts every dispatch and never answers — the worker "goes silent". */
    private static final class SilentTransport implements TaskTransport {
        @Override
        public CompletionStage<Void> dispatch(TaskInvocation invocation) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void onResult(TaskResultHandler handler) {
        }

        @Override
        public AdapterCapabilities capabilities() {
            return new AdapterCapabilities(DeliveryGuarantee.AT_LEAST_ONCE, true, Long.MAX_VALUE, null, true, true);
        }
    }

    private static final class MutableClock implements InstantSource {
        private final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-01-01T00:00:00Z"));

        @Override
        public Instant instant() {
            return now.get();
        }

        void advance(Duration duration) {
            now.updateAndGet(i -> i.plus(duration));
        }
    }
}
