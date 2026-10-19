package io.stintflow.knowledge;

import java.util.Objects;

import io.stintflow.spi.Sensitivity;

/**
 * Looks entities up by name (SDD 3b.1, 4.2): what the extraction asks before it creates a node. A name matches the label
 * or an alias after {@link EntityIds#normalize}.
 *
 * @param type           only this type, or {@code null} for any
 * @param normalizedName {@link EntityIds#normalize} of the name looked for
 * @param ceiling        required, as in {@link RetrievalQuery}
 * @param limit          1 to 50
 */
public record EntityQuery(String type, String normalizedName, Sensitivity ceiling, int limit) {

    public EntityQuery {
        Objects.requireNonNull(normalizedName, "normalizedName");
        Objects.requireNonNull(ceiling, "ceiling is required: there is no default level");
        if (limit < 1 || limit > 50) {
            throw new KnowledgeValidationException("limit is 1 to 50");
        }
    }
}
