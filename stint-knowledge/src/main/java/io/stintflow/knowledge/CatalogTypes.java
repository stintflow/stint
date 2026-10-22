package io.stintflow.knowledge;

import java.util.Set;

/**
 * The node types the entity catalog holds (SDD 3c.3, 4.1): the systems, products and teams of the organization. A person is not
 * one of them: a person is personal data and has its own path ({@code resolve-person}). The loader of the catalog, the lookup and
 * the areas that link to it all read the set here, so none of them has to know another's class.
 */
public final class CatalogTypes {

    public static final Set<String> NAMES = Set.of("System", "Product", "Team");

    private CatalogTypes() {
    }
}
