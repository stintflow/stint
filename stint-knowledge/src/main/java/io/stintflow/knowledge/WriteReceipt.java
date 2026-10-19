package io.stintflow.knowledge;

import java.util.Map;

/**
 * The result of a write (SDD 3b.1, 4.2).
 *
 * @param status       what happened
 * @param counts       the changes made, by {@code <op>.<kind>}, e.g. {@code ADD.PASSAGE}; a refused or unchanged write has none
 * @param lastSequence the last sequence number of the scope's change log after the write
 * @param dropped      references to erased entities that were left out of the write
 */
public record WriteReceipt(Status status, Map<String, Integer> counts, long lastSequence, int dropped) {

    public enum Status {
        /** The batch changed the base. */
        APPLIED,
        /** The base already had exactly this state of the document: nothing was written, nothing was logged. */
        UNCHANGED,
        /** A later revision of the document was applied already: nothing was written. */
        STALE,
        /** The document was erased (SDD 3b.5): it is not written again. */
        REJECTED_ERASED
    }

    public WriteReceipt {
        counts = counts == null ? Map.of() : Map.copyOf(counts);
    }
}
