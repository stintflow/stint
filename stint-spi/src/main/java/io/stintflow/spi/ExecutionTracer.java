package io.stintflow.spi;

import java.util.List;
import java.util.Map;

/**
 * Port: distributed tracing, as seen by the engine and the worker SDK (SDD 2.5, 8b/8c). Trace context
 * travels as plain W3C strings inside {@link Lineage}; the core never touches OpenTelemetry — the
 * {@code stint-otel} module implements this port.
 * <p>
 * Parent or link (sec. 8b):
 * <ul>
 *   <li>{@code parent} set — an immediate handoff (engine → worker → engine): the span is its child;</li>
 *   <li>{@code parent} null and {@code links} non-empty — a resume after a wait (timer, {@code listen}) or a
 *       start by another workflow's fact: a new trace, linked to those contexts, never a child;</li>
 *   <li>both empty — a fresh start: implementations may use their own ambient context.</li>
 * </ul>
 * {@code attributes} are identifiers only ({@link TraceAttributes#ALLOWED}); anything else is rejected.
 */
public interface ExecutionTracer {

    ExecutionSpan start(String name, Map<String, String> attributes, Lineage parent, List<Lineage> links);

    /**
     * Creates no spans. Its span's {@link ExecutionSpan#context()} is the parent it was given, so a traced
     * worker downstream still joins whoever called — and nothing is invented.
     */
    ExecutionTracer NOOP = (name, attributes, parent, links) -> {
        TraceAttributes.requireIdentifiersOnly(attributes);
        Lineage passThrough = parent == null ? Lineage.NONE : Lineage.trace(parent.traceparent(), parent.tracestate());
        return new ExecutionSpan() {
            @Override
            public Lineage context() {
                return passThrough;
            }

            @Override
            public void recordFailure(Throwable failure) {
            }

            @Override
            public void close() {
            }
        };
    };
}
