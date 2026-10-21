package io.stintflow.knowledge;

import java.util.List;
import java.util.Objects;

import io.stintflow.spi.Sensitivity;

/**
 * One piece of context a retrieval returns (SDD 3b.1, 4.2).
 *
 * @param content     the text: a passage, an entity (type and label) or a relation (an edge written out)
 * @param origin      where it comes from
 * @param score       0 to 1; cosine of the passage with the question, or a fixed fraction of it for what the graph adds
 * @param entities    the entities a passage mentions, those at or below the caller's ceiling only
 * @param sensitivity the item's level
 */
public record ContextItem(String content, Origin origin, double score, List<EntityRef> entities,
        Sensitivity sensitivity) {

    public ContextItem {
        Objects.requireNonNull(content, "content");
        Objects.requireNonNull(origin, "origin");
        entities = entities == null ? List.of() : List.copyOf(entities);
        Objects.requireNonNull(sensitivity, "sensitivity");
    }

    public enum Kind { PASSAGE, ENTITY, RELATION }

    /**
     * @param kind       what the item is
     * @param id         its id (a passage id, an entity id, or an edge key)
     * @param documentId the document it comes from, or {@code null} for what the graph adds
     * @param scope      the base
     * @param instanceId the instance that wrote it, or {@code null}
     * @param chainId    its business chain, or {@code null}
     * @param evidence   the documents that still hold what the graph added (an entity or a relation), sorted; empty for a passage
     *                   (SDD 3b.4, 4.1)
     */
    public record Origin(Kind kind, String id, String documentId, KnowledgeScope scope, String instanceId,
            String chainId, List<String> evidence) {

        public Origin {
            evidence = evidence == null ? List.of() : List.copyOf(evidence);
        }

        public Origin(Kind kind, String id, String documentId, KnowledgeScope scope, String instanceId, String chainId) {
            this(kind, id, documentId, scope, instanceId, chainId, List.of());
        }
    }

    public record EntityRef(String id, String type, String label) {
    }
}
