package io.stintflow.knowledge;

import java.util.List;

import io.stintflow.spi.Sensitivity;

/** An entity found by name (SDD 3b.1, 4.2). */
public record EntityMatch(String entityId, String type, String label, List<String> aliases, Sensitivity sensitivity) {

    public EntityMatch {
        aliases = aliases == null ? List.of() : List.copyOf(aliases);
    }
}
