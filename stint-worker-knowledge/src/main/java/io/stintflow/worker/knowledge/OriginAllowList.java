package io.stintflow.worker.knowledge;

import java.util.Set;

import io.stintflow.knowledge.KnowledgeScope;

/**
 * SDD 3c.1, RF6 and 4.4: the domains this worker accepts pointers from. A consumer builds nodes in its own base from what other
 * domains announce, so it names whom it listens to; a domain not in the list is refused ({@code authorization} 403) before the
 * announcement is read. An empty list refuses everything — there is no default, as for {@link ScopeAllowList}. It is not the
 * authentication of the channel between domains (SDD 3c.2): it limits which domains' announcements may become nodes here.
 */
public final class OriginAllowList {

    private final Set<KnowledgeScope> allowed;

    public OriginAllowList(Set<KnowledgeScope> allowed) {
        this.allowed = Set.copyOf(allowed);
    }

    public boolean allows(KnowledgeScope origin) {
        return allowed.contains(origin);
    }
}
