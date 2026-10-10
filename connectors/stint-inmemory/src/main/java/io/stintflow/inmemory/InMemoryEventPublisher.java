package io.stintflow.inmemory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import io.cloudevents.CloudEvent;
import io.stintflow.spi.AdapterCapabilities;
import io.stintflow.spi.AdapterCapabilities.DeliveryGuarantee;
import io.stintflow.spi.EventPublisher;

/**
 * In-process domain channel (SDD 2.2, RF8) for dev/tests: records every accepted fact and hands it to
 * its subscribers — e.g. {@link #connectTo(InMemoryDomainEventBus)} closes the loop with SDD 2.1, so a
 * fact one workflow emits can start another. A separate object from any {@code TaskTransport}: nothing
 * internal ever reaches it.
 * <p>
 * Test hooks: {@link #failNext(int)} makes the next publishes fail (the broker "refusing" them), and
 * {@code maxPayloadBytes} is configurable to exercise the public claim-check.
 */
public final class InMemoryEventPublisher implements EventPublisher {

    private final long maxPayloadBytes;
    private final List<CloudEvent> published = new CopyOnWriteArrayList<>();
    private final List<Function<CloudEvent, CompletionStage<Void>>> subscribers = new CopyOnWriteArrayList<>();
    private final AtomicInteger failuresToInject = new AtomicInteger();
    private final AtomicInteger attempts = new AtomicInteger();

    public InMemoryEventPublisher() {
        this(262_144); // same bound as EventBridge/SNS, so local behaviour matches the cloud connectors
    }

    public InMemoryEventPublisher(long maxPayloadBytes) {
        this.maxPayloadBytes = maxPayloadBytes;
    }

    @Override
    public CompletionStage<Void> publish(CloudEvent event) {
        attempts.incrementAndGet();
        if (failuresToInject.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
            return CompletableFuture.failedFuture(new IllegalStateException("simulated broker failure"));
        }
        published.add(event);
        CompletionStage<Void> delivered = CompletableFuture.completedFuture(null);
        for (Function<CloudEvent, CompletionStage<Void>> subscriber : subscribers) {
            delivered = delivered.thenCompose(v -> subscriber.apply(event));
        }
        return delivered;
    }

    @Override
    public AdapterCapabilities capabilities() {
        return new AdapterCapabilities(DeliveryGuarantee.AT_LEAST_ONCE, false, maxPayloadBytes, null, false, false);
    }

    /** Every fact published from now on is also delivered to {@code subscriber}. */
    public void subscribe(Function<CloudEvent, CompletionStage<Void>> subscriber) {
        subscribers.add(subscriber);
    }

    /** Feeds published facts into an SDD 2.1 bus, so they can start other workflows. */
    public void connectTo(InMemoryDomainEventBus bus) {
        subscribe(bus::publish);
    }

    /** The next {@code count} publishes fail, as if the broker refused them. */
    public void failNext(int count) {
        failuresToInject.set(count);
    }

    /** Accepted facts, in publish order (duplicates included — at-least-once). */
    public List<CloudEvent> published() {
        return List.copyOf(published);
    }

    /** Every publish call, accepted or failed. */
    public int attempts() {
        return attempts.get();
    }
}
