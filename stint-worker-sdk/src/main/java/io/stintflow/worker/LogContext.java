package io.stintflow.worker;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.MDC;

import io.stintflow.spi.Lineage;
import io.stintflow.spi.TraceAttributes;

/**
 * Puts an activation's identifiers in the logging MDC for the synchronous part of its work (SDD 2.5, 8d)
 * and restores what was there before. Keys are {@link TraceAttributes}' identifiers plus {@code traceId}/
 * {@code spanId} (the names Quarkus' OpenTelemetry log integration uses) — identifiers only, never data.
 * MDC is per thread: asynchronous continuations don't inherit it, so log messages also carry the ids.
 */
final class LogContext implements AutoCloseable {

    private final Map<String, String> previous = new HashMap<>();

    private LogContext(Map<String, String> fields) {
        fields.forEach((key, value) -> {
            previous.put(key, MDC.get(key));
            if (value == null) {
                MDC.remove(key);
            } else {
                MDC.put(key, value);
            }
        });
    }

    static LogContext open(Map<String, String> identifiers, Lineage trace) {
        Map<String, String> fields = new HashMap<>(TraceAttributes.requireIdentifiersOnly(identifiers));
        fields.put("traceId", traceId(trace));
        fields.put("spanId", spanId(trace));
        return new LogContext(fields);
    }

    @Override
    public void close() {
        previous.forEach((key, value) -> {
            if (value == null) {
                MDC.remove(key);
            } else {
                MDC.put(key, value);
            }
        });
    }

    /** W3C {@code traceparent} is {@code version-traceId-spanId-flags}. */
    static String traceId(Lineage trace) {
        String[] parts = parts(trace);
        return parts == null ? null : parts[1];
    }

    static String spanId(Lineage trace) {
        String[] parts = parts(trace);
        return parts == null ? null : parts[2];
    }

    private static String[] parts(Lineage trace) {
        if (trace == null || !trace.hasTrace()) {
            return null;
        }
        String[] parts = trace.traceparent().split("-");
        return parts.length == 4 ? parts : null;
    }
}
