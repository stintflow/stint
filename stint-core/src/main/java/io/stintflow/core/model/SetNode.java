package io.stintflow.core.model;

import io.stintflow.core.expr.Expr;

/**
 * {@code set}: computes new data locally (no remote dispatch) by evaluating {@link #set()} against
 * the task's (post {@code input.from}) input; the result is this node's raw output, which then goes
 * through {@code output.as}/{@code export.as} like any other node.
 */
public record SetNode(
        String name,
        String pointer,
        DataFlow dataFlow,
        FlowDirective then,
        Expr set) implements TaskNode {
}
