package io.stintflow.spi;

import java.time.Instant;

/**
 * A fact waiting to be published (SDD 2.2, RF3): written in the same {@link StateStore#save} as the
 * snapshot that produced it, removed only once an {@link EventPublisher} accepted it.
 *
 * @param eventId    the CloudEvent {@code id} — also the outbox key
 * @param instanceId the instance that emitted it
 * @param seq        emission order within its activation
 * @param event      the complete CloudEvent, structured JSON mode
 * @param createdAt  when it was recorded; the sweep publishes oldest first
 */
public record OutboxEntry(String eventId, String instanceId, int seq, String event, Instant createdAt) {
}
