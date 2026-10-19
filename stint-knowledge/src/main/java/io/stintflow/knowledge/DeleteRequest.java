package io.stintflow.knowledge;

import java.util.Objects;

/**
 * Asks the store to erase (SDD 3b.1, 4.7). The flow around it — who may ask, how the person is identified — is SDD 3b.5's.
 * The store erases from the vector index, the graph and the blobs of restricted content in one atomic step, records an
 * {@code ERASE} entry per thing erased (ids only) and keeps a tombstone, so a redelivered write does not bring it back.
 */
public sealed interface DeleteRequest {

    /** Identifies the request in the change log; a repeat of it erases nothing more. */
    String requestId();

    /** A code, not a text: why (e.g. {@code erasure:document}); it goes to the log. */
    String reason();

    /** Everything a document gave: its passages, its links and its evidence; nodes and edges left with none go too. */
    record ByDocument(String requestId, String reason, String documentId) implements DeleteRequest {
        public ByDocument {
            Objects.requireNonNull(requestId, "requestId");
            Objects.requireNonNull(reason, "reason");
            Objects.requireNonNull(documentId, "documentId");
        }
    }

    /**
     * An entity (a person's pseudonym), whatever documents assert it: the node, every edge and link that touches it and,
     * with {@link PassagePolicy#LINKED}, the passages that mention it.
     */
    record ByPerson(String requestId, String reason, String entityId, PassagePolicy passages)
            implements DeleteRequest {
        public ByPerson {
            Objects.requireNonNull(requestId, "requestId");
            Objects.requireNonNull(reason, "reason");
            Objects.requireNonNull(entityId, "entityId");
            passages = passages == null ? PassagePolicy.LINKED : passages;
        }
    }

    enum PassagePolicy {
        /** The passages that mention the entity are erased too (the safe side; the default). */
        LINKED,
        /** The passages stay (their text is already anonymous); only the node, its edges and its links go. */
        NONE
    }
}
