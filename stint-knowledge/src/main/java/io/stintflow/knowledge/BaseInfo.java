package io.stintflow.knowledge;

import java.util.Set;

/**
 * What a base says about itself (SDD 3b.5, 4.1): the embedding model its vectors came from — one per base, since vectors of two
 * models are not comparable — and the graph schema versions its change log has seen. Counts and names only.
 *
 * @param model          the embedding model, or {@code null} for a base that has never been written
 * @param dimension      the vector size, or 0 when {@code model} is {@code null}
 * @param schemaVersions every schema version a change in the log carries
 */
public record BaseInfo(String model, int dimension, int documents, int passages, int entities, int relations,
        Set<String> schemaVersions) {

    public BaseInfo {
        schemaVersions = schemaVersions == null ? Set.of() : Set.copyOf(schemaVersions);
    }
}
