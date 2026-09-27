package io.stintflow.core.expr;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Port: evaluates a data-flow {@link Expr} against the current data and scope. The interpreter is
 * responsible for the "no expression configured" identity default (RF5) — this port only evaluates
 * expressions it is actually given.
 */
public interface ExpressionEvaluator {

    JsonNode eval(Expr expr, JsonNode input, EvalScope scope);
}
