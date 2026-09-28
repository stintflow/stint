package io.stintflow.core.expr;

import java.util.function.BiFunction;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * A data-flow expression, as used by {@code input.from}, {@code output.as} and {@code export.as}
 * (SDD 1.1, RF5). The default implementation ({@link JqExpressionEvaluator}) evaluates {@link Jq}
 * against the current data ({@code .}) with {@code $context} and {@code $workflow} in scope.
 * <p>
 * {@link Lambda} is the "adapter opcional" from RF7: a plain Java function for tests and
 * programmatic examples that don't want to write jq, with the same (context, data) shape the jq
 * evaluator exposes.
 */
public sealed interface Expr permits Expr.Jq, Expr.Lambda {

    record Jq(String source) implements Expr {
        public Jq {
            if (source == null || source.isBlank()) {
                throw new IllegalArgumentException("jq expression source must not be blank");
            }
        }
    }

    record Lambda(BiFunction<JsonNode, JsonNode, JsonNode> fn) implements Expr {
        public Lambda {
            if (fn == null) {
                throw new IllegalArgumentException("lambda must not be null");
            }
        }
    }

    static Expr jq(String source) {
        return new Jq(source);
    }

    /** @param fn {@code (context, data) -> result}, mirroring what a jq expression sees as {@code $context}/{@code .} */
    static Expr of(BiFunction<JsonNode, JsonNode, JsonNode> fn) {
        return new Lambda(fn);
    }
}
