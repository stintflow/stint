package io.stintflow.core.interpreter;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.core.expr.EvalScope;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.expr.ExpressionEvaluator;
import io.stintflow.spi.WorkflowRef;

/**
 * Shared identity-default semantics for {@code input.from}/{@code output.as}/{@code export.as}
 * (RF5), used by both {@link TreeInterpreter} (local nodes) and {@code WorkflowEngine} (applying a
 * {@code call: remote} node's data flow once its result arrives).
 */
public final class DataFlowSupport {

    private DataFlowSupport() {
    }

    /** {@code input.from}/{@code output.as} default to the identity expression (RF5). */
    public static JsonNode applyExpr(ExpressionEvaluator evaluator, Expr expr, JsonNode data, JsonNode context, WorkflowRef ref) {
        return expr == null ? data : evaluator.eval(expr, data, new EvalScope(context, ref));
    }

    /** {@code export.as} defaults to leaving {@code $context} unchanged (RF5) — not identity-over-output. */
    public static JsonNode applyExportAs(ExpressionEvaluator evaluator, Expr exportAs, JsonNode transformedOutput, JsonNode context, WorkflowRef ref) {
        return exportAs == null ? context : evaluator.eval(exportAs, transformedOutput, new EvalScope(context, ref));
    }
}
