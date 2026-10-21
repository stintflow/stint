package io.stintflow.knowledge;

/**
 * The result of a reindexation (SDD 3b.5, 4.1).
 *
 * @param status       what happened
 * @param fromModel    the model before, or {@code null} for a base that had none
 * @param toModel      the model after
 * @param passages     the passages whose vector was recomputed
 * @param skipped      the restricted passages whose blob had expired: no text to embed, so they left the index
 * @param lastSequence the last sequence number of the change log after it
 */
public record ReindexReceipt(Status status, String fromModel, String toModel, int passages, int skipped, long lastSequence) {

    public enum Status {
        /** The vectors were recomputed. */
        APPLIED,
        /** The base already held the target model: nothing was done. */
        UNCHANGED
    }
}
