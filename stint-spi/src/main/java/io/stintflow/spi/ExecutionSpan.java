package io.stintflow.spi;

/**
 * One short unit of traced work — an engine activation or a worker's task execution (SDD 2.5, 8b). It
 * always ends before the instance suspends: no span stays open across a wait.
 */
public interface ExecutionSpan extends AutoCloseable {

    /** This span's trace context ({@code traceparent}/{@code tracestate} only), to put on outgoing events. */
    Lineage context();

    void recordFailure(Throwable failure);

    @Override
    void close();
}
