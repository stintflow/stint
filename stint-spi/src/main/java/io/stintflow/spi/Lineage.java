package io.stintflow.spi;

/**
 * Where an event comes from (SDD 2.5): the business chain it belongs to, what directly caused it, and
 * the distributed-trace context it was produced in. Carried on the wire as the CloudEvents extensions
 * {@code chainid}, {@code causationid}, {@code traceparent} and {@code tracestate} — every field is
 * optional, so an event without them (the pre-2.5 format) reads as {@link #NONE}.
 *
 * @param chainId     root of the chain — identical on every event of every workflow along the path
 * @param causationId id of the event that directly caused this one (for a resume by timer: the
 *                    {@code timerId}, which is a timer fire and not an event on the wire)
 * @param traceparent W3C Trace Context {@code traceparent} of the span that produced the event
 * @param tracestate  W3C Trace Context {@code tracestate}, if any
 */
public record Lineage(String chainId, String causationId, String traceparent, String tracestate) {

    public static final Lineage NONE = new Lineage(null, null, null, null);

    /** Only the trace part — what a span hands to the next hop or keeps for a later span link. */
    public static Lineage trace(String traceparent, String tracestate) {
        return new Lineage(null, null, traceparent, tracestate);
    }

    public Lineage withTrace(Lineage trace) {
        return trace == null ? this : new Lineage(chainId, causationId, trace.traceparent(), trace.tracestate());
    }

    public boolean hasTrace() {
        return traceparent != null && !traceparent.isBlank();
    }
}
