package io.stintflow.core;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.cloudevents.CloudEvent;
import io.stintflow.spi.EventPublisher;
import io.stintflow.spi.OutboxEntry;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.wire.StintEvents;
import io.stintflow.wire.CeWire;

/**
 * Moves facts from the outbox to the {@link EventPublisher} (SDD 2.2, RF4, sec. 8a).
 * <ul>
 *   <li>{@link #publishNow}: right after the save that recorded them, in emission order. A failure is
 *       logged, never propagated — the fact is already durable and the sweep will retry it.</li>
 *   <li>{@link #sweep}: publishes whatever is still pending and older than the grace period (so it
 *       doesn't race {@link #publishNow}), oldest first. In production {@link #start} runs it
 *       periodically; tests call it directly after advancing a controllable clock.</li>
 * </ul>
 * An entry leaves the outbox only after the broker accepted it. Several engines may sweep at once, so
 * a fact can be published more than once — always with the same {@code id}; consumers deduplicate by
 * ({@code source}, {@code id}). No ordering guarantee is offered to consumers.
 */
public final class OutboxRelay {

    private static final Logger LOG = System.getLogger(OutboxRelay.class.getName());

    public static final Duration DEFAULT_GRACE = Duration.ofSeconds(30);
    public static final int DEFAULT_BATCH = 100;
    /** Past this age a still-pending fact is logged at ERROR on every sweep — a signal for alarms. */
    static final Duration STUCK_AFTER = Duration.ofMinutes(15);

    private final StateStore state;
    private final EventPublisher publisher;
    private final InstantSource clock;
    private final Duration grace;
    private final int batch;

    private ScheduledExecutorService scheduler;

    public OutboxRelay(StateStore state, EventPublisher publisher, InstantSource clock) {
        this(state, publisher, clock, DEFAULT_GRACE, DEFAULT_BATCH);
    }

    public OutboxRelay(StateStore state, EventPublisher publisher, InstantSource clock, Duration grace, int batch) {
        this.state = state;
        this.publisher = publisher;
        this.clock = clock;
        this.grace = grace;
        this.batch = batch;
    }

    /** Publishes {@code entries} in {@code seq} order; completes normally even if some fail. */
    public CompletionStage<Void> publishNow(List<OutboxEntry> entries) {
        CompletionStage<Void> chain = CompletableFuture.completedFuture(null);
        for (OutboxEntry entry : entries.stream().sorted(Comparator.comparingInt(OutboxEntry::seq)).toList()) {
            chain = chain.thenCompose(v -> publishOne(entry).thenApply(published -> null));
        }
        return chain;
    }

    /** One sweep: publishes pending facts recorded at least {@code grace} ago. Completes with how many were published. */
    public CompletionStage<Integer> sweep() {
        Instant now = clock.instant();
        return state.pendingOutbox(now.minus(grace), batch).thenCompose(pending -> {
            CompletionStage<Integer> chain = CompletableFuture.completedFuture(0);
            for (OutboxEntry entry : pending) {
                chain = chain.thenCompose(count -> publishOne(entry).thenApply(published -> {
                    if (!published && Duration.between(entry.createdAt(), now).compareTo(STUCK_AFTER) > 0) {
                        LOG.log(Level.ERROR, "Fact {0} of instance {1} still unpublished after {2}",
                                entry.eventId(), entry.instanceId(), Duration.between(entry.createdAt(), now));
                    }
                    return published ? count + 1 : count;
                }));
            }
            return chain;
        });
    }

    /** Runs {@link #sweep} every {@code interval} on a daemon thread (production). Idempotent. */
    public synchronized void start(Duration interval) {
        if (scheduler != null) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "stint-outbox-relay");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                sweep().toCompletableFuture().join();
            } catch (RuntimeException e) {
                LOG.log(Level.WARNING, "Outbox sweep failed; will retry", e);
            }
        }, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    public synchronized void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }

    /** Publish, then remove; {@code false} (and the entry kept) on any failure. */
    private CompletionStage<Boolean> publishOne(OutboxEntry entry) {
        CloudEvent event;
        try {
            event = CeWire.fromJson(entry.event().getBytes(StandardCharsets.UTF_8));
        } catch (RuntimeException e) {
            LOG.log(Level.ERROR, "Outbox entry " + entry.eventId() + " is not a valid CloudEvent", e);
            return CompletableFuture.completedFuture(false);
        }
        if (StintEvents.isReservedType(event.getType())) {
            // Unreachable by construction (sec. 8e); defence in depth — never let engine traffic out here.
            LOG.log(Level.ERROR, "Refusing to publish fact {0} with reserved type {1}", entry.eventId(), event.getType());
            return CompletableFuture.completedFuture(false);
        }
        CompletionStage<Void> published;
        try {
            published = publisher.publish(event);
        } catch (RuntimeException e) {
            published = CompletableFuture.failedFuture(e);
        }
        return published
                .thenCompose(v -> state.removeOutbox(entry.eventId()))
                .handle((v, ex) -> {
                    if (ex != null) {
                        LOG.log(Level.WARNING, "Publishing fact " + entry.eventId() + " failed; it stays in the outbox", ex);
                        return false;
                    }
                    return true;
                });
    }
}
