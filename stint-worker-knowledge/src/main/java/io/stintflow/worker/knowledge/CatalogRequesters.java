package io.stintflow.worker.knowledge;

import java.util.Set;

import io.stintflow.spi.WorkflowRef;

/**
 * SDD 3c.3, RF6 and 4.2: the definitions that may ask the catalog what a name is, as {@code namespace:name} (no version). The
 * requester is the definition of the invitation, never a field of the input; a definition not listed gets the same answer as a
 * catalog that does not know the name. An empty list answers {@code NONE} to everyone — there is no default.
 */
public final class CatalogRequesters {

    private final Set<String> allowed;

    public CatalogRequesters(Set<String> allowed) {
        this.allowed = Set.copyOf(allowed);
    }

    boolean allows(WorkflowRef definition) {
        return definition != null && allowed.contains(definition.namespace() + ":" + definition.name());
    }
}
