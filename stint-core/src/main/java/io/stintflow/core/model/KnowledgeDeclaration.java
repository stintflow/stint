package io.stintflow.core.model;

import io.stintflow.spi.WorkflowRef;

/**
 * SDD 3b.2, 4.1: what a definition declares about its knowledge base — the Stint extension
 * {@code document.metadata.x-stint-knowledge}, not part of the CNCF DSL. The engine only carries it: the schema and the
 * glossary are resolved by the worker that extracts ({@code stint-knowledge}), since the engine side does not depend on it.
 *
 * @param base     the name of the base, or {@code null}: then the base is the definition's own name
 * @param schema   {@code <id>@<major>}, e.g. {@code meetings@1}: the highest minor of that major the worker knows
 * @param glossary {@code <id>@<major>}, or {@code null}
 */
public record KnowledgeDeclaration(String base, String schema, String glossary) {

    /** The key under {@code document.metadata}. */
    public static final String METADATA_KEY = "x-stint-knowledge";

    /** The name of the base for {@code definition}: the declared one, else the definition's name (never its version). */
    public String baseOf(WorkflowRef definition) {
        return base != null ? base : definition.name();
    }
}
