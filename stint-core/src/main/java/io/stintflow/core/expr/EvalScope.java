package io.stintflow.core.expr;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.spi.WorkflowRef;

/**
 * What an {@link Expr} sees besides the current data ({@code .}): the workflow's {@code $context}
 * document (SDD 1.1, RF5 — mutated only via {@code export.as}) and {@code $workflow} (SDD 2.1, RF6 —
 * the DSL 1.0 workflow descriptor plus the ref).
 */
public record EvalScope(JsonNode context, WorkflowDescriptor workflow) {

    /** A scope with no running instance behind it: {@code $workflow} carries only the ref. */
    public EvalScope(JsonNode context, WorkflowRef ref) {
        this(context, ref == null ? null : WorkflowDescriptor.of(ref));
    }
}
