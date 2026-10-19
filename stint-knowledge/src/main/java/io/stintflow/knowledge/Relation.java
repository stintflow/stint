package io.stintflow.knowledge;

import java.util.Objects;

import io.stintflow.spi.Sensitivity;

/**
 * An edge of the graph (SDD 3b.1, 4.2): identified by {@code (type, fromId, toId)}. Its level is at least that of both ends.
 */
public record Relation(String type, String fromId, String toId, Sensitivity sensitivity) {

    public Relation {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(fromId, "fromId");
        Objects.requireNonNull(toId, "toId");
        sensitivity = sensitivity == null ? Sensitivity.RESTRICTED : sensitivity;
    }

    /** The identity of the edge. */
    public String key() {
        return type + "|" + fromId + "|" + toId;
    }
}
