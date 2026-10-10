package io.stintflow.spi;

import java.util.concurrent.CompletionStage;

import io.cloudevents.CloudEvent;

/**
 * Port: the domain channel (SDD 2.2) — publishes a workflow's facts ({@code emit}) as CloudEvents to
 * consumers outside the engine, on a destination separate from the internal {@link TaskTransport}
 * (invoke/result/timer never go through here).
 * <p>
 * Called only by the engine's outbox relay, after the fact was durably recorded with the state that
 * produced it; the same fact may be published more than once (always with the same {@code id}), so
 * consumers deduplicate by ({@code source}, {@code id}).
 */
public interface EventPublisher {

    /** Completes normally only once the broker accepted the event. */
    CompletionStage<Void> publish(CloudEvent event);

    /** {@link AdapterCapabilities#maxPayloadBytes()} bounds the event before its data goes to {@code dataref}. */
    AdapterCapabilities capabilities();
}
