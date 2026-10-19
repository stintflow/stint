package io.stintflow.knowledge;

import java.util.Objects;

import io.stintflow.spi.WorkflowRef;

/**
 * Which knowledge base (SDD 3b.1, 4.3): a namespace and a base name. Derived from a {@link WorkflowRef} <em>without its
 * version</em> — the base is what outlives new versions of a definition; the version enters the change log as
 * provenance. {@link #named} makes a base several definitions share, or one that survives a renamed definition.
 */
public record KnowledgeScope(String namespace, String base) {

    public KnowledgeScope {
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(base, "base");
        if (namespace.isBlank() || base.isBlank()) {
            throw new IllegalArgumentException("A knowledge scope needs a namespace and a base");
        }
    }

    /** The base of a definition: its namespace and name; every version of it shares the base. */
    public static KnowledgeScope of(WorkflowRef definition) {
        return new KnowledgeScope(definition.namespace(), definition.name());
    }

    /** A base with a name of its own, shared by whichever definitions name it. */
    public static KnowledgeScope named(String namespace, String base) {
        return new KnowledgeScope(namespace, base);
    }

    /** {@code namespace/base}: the key under which a store keeps the scope. */
    public String key() {
        return namespace + "/" + base;
    }
}
