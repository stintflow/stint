package io.stintflow.spi;

/**
 * Port: inbound domain events (SDD 2.1) — CloudEvents published by other workflows or systems,
 * delivered from an inbound queue, an EventBridge rule, a Knative source, ...
 * <p>
 * One subscriber per bus: the engine registers a single {@link DomainEventHandler} that both starts
 * bound definitions (SDD 2.1) and, later, resumes instances waiting in {@code listen} (SDD 2.3).
 * <p>
 * Delivery contract: a connector acknowledges (deletes/commits) a message <strong>only</strong> when
 * the stage returned by the handler completes normally. An exceptional completion leaves the message
 * for redelivery and, once the source's own limit is reached, for its dead-letter queue. Handlers are
 * idempotent, so at-least-once redelivery is safe.
 */
public interface DomainEventSource {

    void onEvent(DomainEventHandler handler);
}
