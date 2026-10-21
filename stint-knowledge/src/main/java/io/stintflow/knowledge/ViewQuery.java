package io.stintflow.knowledge;

import java.util.Objects;

import io.stintflow.spi.Sensitivity;

/**
 * What a {@link KnowledgeInspector#view} asks (SDD 3b.6, 4.1).
 *
 * @param ceiling     required, as in {@link RetrievalQuery}: nothing above it comes back, nor a hint that it is there
 * @param limit       nodes per page, 1 to 5000
 * @param afterNodeId the last node id of the previous page, or {@code null} for the first
 * @param type        only nodes of this type, or {@code null} for any
 */
public record ViewQuery(Sensitivity ceiling, int limit, String afterNodeId, String type) {

    public ViewQuery {
        Objects.requireNonNull(ceiling, "ceiling is required: there is no default level");
        if (limit < 1 || limit > 5000) {
            throw new KnowledgeValidationException("limit is 1 to 5000");
        }
    }
}
