package io.stintflow.knowledge.inspector;

import java.util.Map;
import java.util.Set;

import io.stintflow.knowledge.KnowledgeScope;

/**
 * Port (SDD 3b.6, RF7): whether a caller may look at a base. Authenticating the caller is the host's — a gateway, Quarkus
 * security — and this asks only the question. There is no default that allows: {@link #none()} refuses everyone, as an empty
 * list of scopes does for the workers (SDD 3b.3).
 */
@FunctionalInterface
public interface InspectorAccess {

    boolean mayInspect(String caller, KnowledgeScope scope);

    /** Nobody. */
    static InspectorAccess none() {
        return (caller, scope) -> false;
    }

    /** Each caller may look at the bases listed for them, and no other. */
    static InspectorAccess allowing(Map<String, Set<KnowledgeScope>> byCaller) {
        Map<String, Set<KnowledgeScope>> copy = Map.copyOf(byCaller);
        return (caller, scope) -> caller != null && copy.getOrDefault(caller, Set.of()).contains(scope);
    }
}
