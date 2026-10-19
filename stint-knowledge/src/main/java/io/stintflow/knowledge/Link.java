package io.stintflow.knowledge;

import java.util.Objects;

/** A passage mentions an entity (SDD 3b.1, 4.2). */
public record Link(String passageId, String entityId) {

    public Link {
        Objects.requireNonNull(passageId, "passageId");
        Objects.requireNonNull(entityId, "entityId");
    }
}
