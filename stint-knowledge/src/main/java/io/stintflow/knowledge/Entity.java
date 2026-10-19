package io.stintflow.knowledge;

import java.util.Map;
import java.util.Objects;

import io.stintflow.spi.Sensitivity;

/**
 * A node of the graph (SDD 3b.1, 4.2). The id and type come from the writer: resolving a mention to an existing id is the
 * extraction's job (SDD 3b.2), not the store's.
 *
 * @param entityId    opaque and stable ({@link EntityIds#of} makes one from a type and a canonical name)
 * @param type        the node type, e.g. {@code System}
 * @param label       a short name; for a restricted entity it rests in a blob like any restricted content
 * @param properties  more text values; {@code aliases} (joined by {@code |}) are matched by {@link EntityQuery}
 * @param sensitivity its level; absent reads as {@code RESTRICTED}
 */
public record Entity(String entityId, String type, String label, Map<String, String> properties,
        Sensitivity sensitivity) {

    public Entity {
        Objects.requireNonNull(entityId, "entityId");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(label, "label");
        properties = properties == null ? Map.of() : Map.copyOf(properties);
        sensitivity = sensitivity == null ? Sensitivity.RESTRICTED : sensitivity;
    }

    public Entity(String entityId, String type, String label, Sensitivity sensitivity) {
        this(entityId, type, label, Map.of(), sensitivity);
    }
}
