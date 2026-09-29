package io.stintflow.spi;

import java.util.concurrent.CompletionStage;

import io.cloudevents.CloudEvent;

/**
 * Consumes one inbound domain event (SDD 2.1). Completes normally only after every effect of the
 * event has been durably recorded — see {@link DomainEventSource} for the acknowledgement contract.
 */
@FunctionalInterface
public interface DomainEventHandler {

    CompletionStage<Void> handle(CloudEvent event);
}
