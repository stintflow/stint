package io.stintflow.knowledge;

import java.util.Objects;
import java.util.Set;

import io.stintflow.spi.Sensitivity;

/**
 * What a worker asks of {@link KnowledgeNodes} (SDD 3c.2, 4.1).
 *
 * @param ids           one to {@value #MAX_IDS} node ids
 * @param ceiling       <b>required</b>: the highest level the caller may receive, and never above {@code confidential}
 * @param neighborLimit 0 for the nodes alone; 1 to {@value #MAX_NEIGHBORS} to add that many neighbours of each (by relation)
 */
public record NodesQuery(Set<String> ids, Sensitivity ceiling, int neighborLimit) {

    public static final int MAX_IDS = 50;
    public static final int MAX_NEIGHBORS = 20;

    public NodesQuery {
        Objects.requireNonNull(ids, "ids");
        Objects.requireNonNull(ceiling, "ceiling is required: there is no default level");
        ids = Set.copyOf(ids);
        if (ids.isEmpty() || ids.size() > MAX_IDS) {
            throw new KnowledgeValidationException("ids is 1 to " + MAX_IDS);
        }
        if (ceiling == Sensitivity.RESTRICTED) {
            throw new KnowledgeValidationException("This port does not return restricted content");
        }
        if (neighborLimit < 0 || neighborLimit > MAX_NEIGHBORS) {
            throw new KnowledgeValidationException("neighborLimit is 0 to " + MAX_NEIGHBORS);
        }
    }
}
