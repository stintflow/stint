package io.stintflow.knowledge;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.stintflow.spi.Sensitivity;

/**
 * A node as {@link KnowledgeNodes} returns it (SDD 3c.2, 4.1): its label and properties are at or below the caller's ceiling.
 *
 * @param documents the documents that still hold it, sorted
 * @param neighbors the nodes it is related to (each without neighbours of its own), by relation and direction, within the limit
 */
public record NodeContent(String id, String type, String label, Map<String, String> properties, Sensitivity level,
        List<String> documents, List<Neighbor> neighbors) {

    public NodeContent {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(label, "label");
        Objects.requireNonNull(level, "level");
        properties = properties == null ? Map.of() : Map.copyOf(properties);
        documents = documents == null ? List.of() : List.copyOf(documents);
        neighbors = neighbors == null ? List.of() : List.copyOf(neighbors);
    }

    public enum Direction { OUT, IN }

    /** @param relation the relation type; {@code direction} is OUT when this node is the relation's source */
    public record Neighbor(String relation, Direction direction, NodeContent node) {
    }
}
