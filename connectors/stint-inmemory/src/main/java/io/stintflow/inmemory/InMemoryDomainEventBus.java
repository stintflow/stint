package io.stintflow.inmemory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.cloudevents.CloudEvent;
import io.stintflow.spi.DomainEventHandler;
import io.stintflow.spi.DomainEventSource;

/**
 * In-process {@link DomainEventSource} (SDD 2.1, RF5) for dev/tests. {@link #publish} hands the event
 * to the registered consumer and returns its stage: a normal completion is the "ack", an exceptional
 * one is what a real broker would redeliver — so a test simulates at-least-once delivery simply by
 * publishing the same event again (sequentially or concurrently).
 */
public final class InMemoryDomainEventBus implements DomainEventSource {

    private volatile DomainEventHandler handler;

    @Override
    public void onEvent(DomainEventHandler handler) {
        this.handler = handler;
    }

    public CompletionStage<Void> publish(CloudEvent event) {
        DomainEventHandler h = handler;
        if (h == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("No domain event consumer registered"));
        }
        try {
            return h.handle(event);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}
