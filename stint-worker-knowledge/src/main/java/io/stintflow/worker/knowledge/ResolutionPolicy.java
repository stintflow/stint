package io.stintflow.worker.knowledge;

import java.util.HashMap;
import java.util.LinkedHashMap;
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
    private String contractVersion;
    private String contractHash;
    private final Map<KnowledgeScope, Map<String, PublishedView>> views = new HashMap<>();

    public ResolutionPolicy(List<Grant> grants, List<PublishedView> views) {
        this.grants = List.copyOf(grants);
        for (PublishedView view : views) {
            if (this.views.computeIfAbsent(view.base(), b -> new LinkedHashMap<>()).put(view.version(), view) != null) {
                throw new IllegalArgumentException("The base " + view.base().key() + " publishes the view " + view.version() + " twice");
            }
        }
        for (Grant grant : this.grants) {
            if (grant.view() != null && grant.bases().stream().anyMatch(b -> !this.views.getOrDefault(b, Map.of()).containsKey(grant.view()))) {
                throw new IllegalArgumentException("The grant of " + grant.requesterNamespace() + ":" + grant.requesterName()
                        + " names the view " + grant.view() + ", which a base it covers does not publish");
            }
        }
    }

    /** SDD 3c.5: says this policy came from the contract of that version and hash, which the audit of each resolution then cites. */
    ResolutionPolicy contract(String version, String hash) {
        this.contractVersion = version;
        this.contractHash = hash;
        return this;
    }

    Optional<String> contractVersion() {
        return Optional.ofNullable(contractVersion);
    }

    Optional<String> contractHash() {
        return Optional.ofNullable(contractHash);
    }

    /** The grant of the requester over {@code base}, if the base is published and a grant covers it. */
    Optional<Grant> grantFor(String namespace, String name, KnowledgeScope base) {
        if (!views.containsKey(base)) {
            return Optional.empty();
        }
        return grants.stream().filter(g -> g.covers(namespace, name, base)).findFirst();
    }

    /** The view {@code grant} sees in {@code base}: the one it names, else the first the base published. */
    Optional<PublishedView> viewOf(KnowledgeScope base, Grant grant) {
        Map<String, PublishedView> published = views.getOrDefault(base, Map.of());
        if (grant.view() != null) {
            return Optional.ofNullable(published.get(grant.view()));
        }
        return published.values().stream().findFirst();
    }
}
