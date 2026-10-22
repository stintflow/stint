package io.stintflow.worker.knowledge;

import java.util.Set;

/**
 * SDD 3c.2, 4.2: what a node of one type shows of itself to another domain — the published language, not the internal schema.
 *
 * @param type       the node type
 * @param label      whether the label is shown
 * @param properties the properties shown, by name; none by default
 * @param relations  the relation types whose other end is shown with the node (at depth 1); none by default
 */
public record PublishedType(String type, boolean label, Set<String> properties, Set<String> relations) {

    public PublishedType {
        properties = properties == null ? Set.of() : Set.copyOf(properties);
        relations = relations == null ? Set.of() : Set.copyOf(relations);
    }

    public static PublishedType of(String type, boolean label, Set<String> properties, Set<String> relations) {
        return new PublishedType(type, label, properties, relations);
    }
}
