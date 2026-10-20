package io.stintflow.core.expr;

import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.spi.WorkflowRef;

/**
 * What an {@link Expr} sees besides the current data ({@code .}): the workflow's {@code $context}
 * document (SDD 1.1, RF5 — mutated only via {@code export.as}) and {@code $workflow} (SDD 2.1, RF6 —
 * the DSL 1.0 workflow descriptor plus the ref).
 * <p>
 * SDD 3a.8: {@code variables} are extra named values an expression reads as {@code $name} — the {@code each} and
 * {@code at} of a {@code for}. Empty everywhere else.
 */
public record EvalScope(JsonNode context, WorkflowDescriptor workflow, Map<String, JsonNode> variables) {

    public EvalScope {
        variables = variables == null ? Map.of() : Map.copyOf(variables);
    }

    public EvalScope(JsonNode context, WorkflowDescriptor workflow) {
        this(context, workflow, Map.of());
    }

    /** A scope with no running instance behind it: {@code $workflow} carries only the ref. */
    public EvalScope(JsonNode context, WorkflowRef ref) {
        this(context, ref == null ? null : WorkflowDescriptor.of(ref), Map.of());
    }

    /** This scope, also exposing {@code variables} (a {@code for}'s {@code each} and {@code at}). */
    public EvalScope withVariables(Map<String, JsonNode> variables) {
        return new EvalScope(context, workflow, variables);
    }
}
