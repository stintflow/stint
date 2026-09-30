package io.stintflow.core.trigger;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.cloudevents.CloudEvent;
import io.stintflow.core.trigger.DomainEventReaction.Reacted;
import io.stintflow.spi.DomainEventHandler;
import io.stintflow.spi.DomainEventSource;

/**
 * The single consumer of inbound domain events (SDD 2.1, RF4, sec. 8c): one subscriber per bus, and
 * every {@link DomainEventReaction} ({@link StartReaction}, SDD 2.3 {@link ResumeReaction}) runs on each event.
 * <p>
 * Acknowledgement policy (sec. 8d), expressed through the returned stage (see {@link DomainEventSource}):
 * <ul>
 *   <li>all reactions recorded their effects → completes normally → the connector acks;</li>
 *   <li>no reaction matched (no binding, no wait) → logged and completes normally → acked and dropped:
 *       on a shared bus, events for other consumers are normal traffic, not dead letters;</li>
 *   <li>any reaction failed (unknown target definition, transient store/transport error) → completes
 *       exceptionally → not acked → redelivered, then dead-lettered by the source.</li>
 * </ul>
 * A body that isn't a valid CloudEvent never reaches here: the connector fails to decode it and
 * doesn't ack it.
 */
public final class DomainEventRouter implements DomainEventHandler {

    private static final Logger LOG = System.getLogger(DomainEventRouter.class.getName());

    private final List<DomainEventReaction> reactions;

    public DomainEventRouter(List<DomainEventReaction> reactions) {
        this.reactions = List.copyOf(reactions);
    }

    /** Wires this router as {@code source}'s single consumer. */
    public DomainEventRouter subscribeTo(DomainEventSource source) {
        source.onEvent(this);
        return this;
    }

    @Override
    public CompletionStage<Void> handle(CloudEvent event) {
        List<CompletableFuture<Reacted>> outcomes = reactions.stream()
                .map(reaction -> invoke(reaction, event))
                .toList();
        return CompletableFuture.allOf(outcomes.toArray(CompletableFuture[]::new))
                .thenRun(() -> {
                    if (outcomes.stream().noneMatch(o -> o.join() == Reacted.MATCHED)) {
                        LOG.log(Level.INFO, "Domain event {0} ({1}) from {2} matched no binding or wait; dropped",
                                event.getId(), event.getType(), event.getSource());
                    }
                })
                .whenComplete((v, ex) -> {
                    if (ex != null) {
                        LOG.log(Level.WARNING, "Domain event " + event.getId() + " (" + event.getType()
                                + ") not processed; leaving it for redelivery", ex);
                    }
                });
    }

    /** A reaction that throws synchronously must fail the event, not escape the handler. */
    private static CompletableFuture<Reacted> invoke(DomainEventReaction reaction, CloudEvent event) {
        try {
            return reaction.react(event).toCompletableFuture();
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}
