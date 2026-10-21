package io.stintflow.knowledge;

import java.util.Map;

/**
 * What an erasure left behind (SDD 3b.5, 4.1). {@code remaining} counts, by kind ({@code PASSAGE}, {@code ENTITY},
 * {@code RELATION}, {@code LINK}, {@code DOCUMENT}, {@code BLOB}), what the request should have removed and is still there; it
 * is empty when nothing is. {@code tombstone} says the erasure is recorded, so a redelivered write does not bring it back.
 * Ids and counts only.
 */
public record ErasureCheck(String requestId, Map<String, Integer> remaining, boolean tombstone) {

    public ErasureCheck {
        remaining = remaining == null ? Map.of() : Map.copyOf(remaining);
    }

    /** Nothing left and the tombstone is in place. */
    public boolean verified() {
        return remaining.isEmpty() && tombstone;
    }
}
