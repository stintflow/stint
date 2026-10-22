package io.stintflow.worker.knowledge;

import java.util.Objects;
import java.util.Set;

import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.spi.Sensitivity;

/**
 * SDD 3c.2, 4.2: what one requesting definition (namespace and name, never the version) may resolve in the bases of this origin:
 * which bases, which node types, up to which level — and never above {@code confidential}.
 */
public record Grant(String requesterNamespace, String requesterName, Set<KnowledgeScope> bases, Set<String> types,
        Sensitivity ceiling) {

    public Grant {
        Objects.requireNonNull(requesterNamespace, "requesterNamespace");
        Objects.requireNonNull(requesterName, "requesterName");
        Objects.requireNonNull(ceiling, "ceiling");
        if (ceiling == Sensitivity.RESTRICTED) {
            throw new IllegalArgumentException("A grant never reaches restricted content");
        }
        bases = Set.copyOf(bases);
        types = Set.copyOf(types);
    }

    boolean covers(String namespace, String name, KnowledgeScope base) {
        return requesterNamespace.equals(namespace) && requesterName.equals(name) && bases.contains(base);
    }
}
