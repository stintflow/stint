package io.stintflow.worker.knowledge;

import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.worker.TaskFailure;

/**
 * SDD 3b.3, RF7 and 4.5: the bases a worker serves. The scope of a task is in its input — never inferred — and a scope not in
 * the list is refused ({@code authorization} 403) before anything is read or written. An empty list refuses everything: there
 * is no default, as for the allowed sources of a cancellation (SDD 3a.11). A base belongs to one domain; joining two bases is
 * the business of a later phase.
 */
public final class ScopeAllowList {

    private final Set<KnowledgeScope> allowed;

    public ScopeAllowList(Set<KnowledgeScope> allowed) {
        this.allowed = Set.copyOf(allowed);
    }

    /**
     * The scope the input names, if this worker serves it.
     *
     * @throws TaskFailure {@code validation} if the input names none, {@code authorization} if it is not in the list
     */
    KnowledgeScope require(JsonNode input) {
        JsonNode scope = input.path("scope");
        String namespace = scope.path("namespace").asText("");
        String base = scope.path("base").asText("");
        if (namespace.isBlank() || base.isBlank()) {
            throw Failures.validation("scope.namespace and scope.base are required");
        }
        KnowledgeScope requested = KnowledgeScope.named(namespace, base);
        if (!allowed.contains(requested)) {
            throw Failures.refused("This worker does not serve that knowledge base");
        }
        return requested;
    }
}
