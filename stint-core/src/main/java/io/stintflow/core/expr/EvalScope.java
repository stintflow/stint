package io.stintflow.core.expr;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.spi.WorkflowRef;

/**
 * What an {@link Expr} sees besides the current data ({@code .}): the workflow's {@code $context}
 * document (SDD 1.1, RF5 — mutated only via {@code export.as}) and {@code $workflow} (its ref).
 */
public record EvalScope(JsonNode context, WorkflowRef workflow) {
}
