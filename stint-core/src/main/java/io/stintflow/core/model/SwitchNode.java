package io.stintflow.core.model;

import java.util.List;

import io.stintflow.core.expr.Expr;

/**
 * {@code switch} (RF4): evaluates {@link Case#when()} for each case in order (against the task's
 * post {@code input.from} data) and follows the first match's {@link Case#then()}. A case with a
 * {@code null} {@code when} is the default and always matches; it should be listed last.
 * <p>
 * {@link #then()} is unused — control always transfers via the matched case's own {@code then}.
 */
public record SwitchNode(
        String name,
        String pointer,
        DataFlow dataFlow,
        FlowDirective then,
        List<Case> cases) implements TaskNode {

    public record Case(String name, Expr when, FlowDirective then) {
    }
}
