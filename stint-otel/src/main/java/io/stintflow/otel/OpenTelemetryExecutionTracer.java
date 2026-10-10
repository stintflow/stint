package io.stintflow.otel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.stintflow.spi.ExecutionSpan;
import io.stintflow.spi.ExecutionTracer;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.TraceAttributes;

/**
 * {@link ExecutionTracer} on the OpenTelemetry API (SDD 2.5, 8c). Trace context crosses the Stint wire as
 * W3C strings ({@link Lineage#traceparent()}/{@link Lineage#tracestate()}); this class converts them with
 * the {@link W3CTraceContextPropagator}. The engine and the worker SDK never see an OpenTelemetry type.
 * <p>
 * Parent or link (sec. 8b): a {@code parent} makes a child span (immediate handoff); no parent but some
 * {@code links} makes a new trace linked to them (resume after a wait, start by another workflow's fact);
 * neither uses the ambient context (e.g. the HTTP request that called {@code start}).
 * <p>
 * Attributes are identifiers only (decision 7): anything outside {@link TraceAttributes#ALLOWED} is rejected.
 * In a Quarkus app, build it from the {@code OpenTelemetry} bean {@code quarkus-opentelemetry} provides.
 */
public final class OpenTelemetryExecutionTracer implements ExecutionTracer {

    private static final String TRACEPARENT = "traceparent";
    private static final String TRACESTATE = "tracestate";

    private static final TextMapGetter<Lineage> FROM_LINEAGE = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Lineage carrier) {
            return List.of(TRACEPARENT, TRACESTATE);
        }

        @Override
        public String get(Lineage carrier, String key) {
            if (carrier == null) {
                return null;
            }
            return switch (key) {
                case TRACEPARENT -> carrier.traceparent();
                case TRACESTATE -> carrier.tracestate();
                default -> null;
            };
        }
    };

    private final Tracer tracer;

    public OpenTelemetryExecutionTracer(OpenTelemetry openTelemetry) {
        this(openTelemetry.getTracer("io.stintflow"));
    }

    public OpenTelemetryExecutionTracer(Tracer tracer) {
        this.tracer = tracer;
    }

    @Override
    public ExecutionSpan start(String name, Map<String, String> attributes, Lineage parent, List<Lineage> links) {
        TraceAttributes.requireIdentifiersOnly(attributes);
        SpanBuilder builder = tracer.spanBuilder(name);
        if (parent != null && parent.hasTrace()) {
            builder.setParent(extract(parent));
        } else if (!links.isEmpty()) {
            builder.setNoParent(); // a wait or another workflow in between: a new trace, only linked
        }
        for (Lineage link : links) {
            if (link != null && link.hasTrace()) {
                SpanContext linked = Span.fromContext(extract(link)).getSpanContext();
                if (linked.isValid()) {
                    builder.addLink(linked);
                }
            }
        }
        attributes.forEach(builder::setAttribute);
        return new OtelSpan(builder.startSpan());
    }

    private static Context extract(Lineage lineage) {
        return W3CTraceContextPropagator.getInstance().extract(Context.root(), lineage, FROM_LINEAGE);
    }

    private static final class OtelSpan implements ExecutionSpan {
        private final Span span;

        OtelSpan(Span span) {
            this.span = span;
        }

        @Override
        public Lineage context() {
            Map<String, String> carrier = new HashMap<>();
            W3CTraceContextPropagator.getInstance().inject(Context.root().with(span), carrier, Map::put);
            return Lineage.trace(carrier.get(TRACEPARENT), carrier.get(TRACESTATE));
        }

        @Override
        public void recordFailure(Throwable failure) {
            span.setStatus(StatusCode.ERROR, failure.getClass().getSimpleName());
        }

        @Override
        public void close() {
            span.end();
        }
    }
}
