package io.stintflow.core.expr;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Port: evaluates a data-flow {@link Expr} against the current data and scope. The interpreter is
 * responsible for the "no expression configured" identity default (RF5) — this port only evaluates
 * expressions it is actually given.
 */
public interface ExpressionEvaluator {

    JsonNode eval(Expr expr, JsonNode input, EvalScope scope);

    /**
     * Compiles {@code expr}'s syntax without evaluating it (SDD 1.4, sec. 8b): lets a DSL loader
     * fail at load time, citing file+path, instead of at first evaluation in production. Successful
     * validation may populate the same compiled-query cache {@link #eval} uses, so validating an
     * expression once at load time is not wasted work.
     *
     * @throws ExpressionEvaluationException if {@code expr}'s syntax is invalid
     */
    void validate(Expr expr);
}
