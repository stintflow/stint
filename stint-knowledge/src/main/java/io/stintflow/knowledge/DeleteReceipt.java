package io.stintflow.knowledge;

import java.util.Map;

/**
 * The result of an erasure (SDD 3b.1, 4.7).
 *
 * @param requestId    the request
 * @param counts       what was erased, by kind, e.g. {@code ERASE.PASSAGE}; empty when there was nothing
 * @param lastSequence the last sequence number of the change log after it
 */
public record DeleteReceipt(String requestId, Map<String, Integer> counts, long lastSequence) {

    public DeleteReceipt {
        counts = counts == null ? Map.of() : Map.copyOf(counts);
    }
}
