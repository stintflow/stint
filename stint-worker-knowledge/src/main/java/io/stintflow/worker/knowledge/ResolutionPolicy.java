package io.stintflow.worker.knowledge;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.stintflow.knowledge.KnowledgeScope;

/**
 * SDD 3c.2, 4.2: who may resolve what in the bases of this origin, and what a node shows. No grants resolve nothing: the policy is
 * denial by default, like {@link ScopeAllowList}.
 */
public final class ResolutionPolicy {

    private final List<Grant> grants;
    private final Map<KnowledgeScope, PublishedView> views = new HashMap<>();

    public ResolutionPolicy(List<Grant> grants, List<PublishedView> views) {
        this.grants = List.copyOf(grants);
        for (PublishedView view : views) {
            if (this.views.put(view.base(), view) != null) {
                throw new IllegalArgumentException("The base " + view.base().key() + " has two published views");
            }
        }
    }

    /** The grant of the requester over {@code base}, if the base is published and a grant covers it. */
    Optional<Grant> grantFor(String namespace, String name, KnowledgeScope base) {
        if (!views.containsKey(base)) {
            return Optional.empty();
        }
        return grants.stream().filter(g -> g.covers(namespace, name, base)).findFirst();
    }

    Optional<PublishedView> viewOf(KnowledgeScope base) {
        return Optional.ofNullable(views.get(base));
    }
}
