package io.stintflow.spi;

import java.time.Instant;

/**
 * A domain event that arrived before the {@code listen} waiting for it was registered (SDD 2.3, RF3,
 * sec. 8c), kept for a short window so the instance can still consume it when it suspends.
 *
 * @param waitKey   the {@code event:<type>:<shape>:<value>} key the event would have resumed
 * @param eventId   the CloudEvent {@code id}
 * @param event     the complete CloudEvent, structured JSON mode
 * @param expiresAt past this instant the entry is ignored (readers filter; any native TTL only cleans up)
 */
public record InboxEntry(String waitKey, String eventId, String event, Instant expiresAt) {

    /** Identifies an entry to remove. */
    public record Key(String waitKey, String eventId) {
    }

    public Key key() {
        return new Key(waitKey, eventId);
    }
}
