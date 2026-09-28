package io.stintflow.core.model;

import io.stintflow.core.expr.Expr;

/**
 * {@code try}/{@code catch}/{@code retry} (SDD 1.3, RF5). Scope note: {@link #body()} is restricted
 * to a single {@link CallRemoteNode} in this increment (not an arbitrary sub-tree) — it covers every
 * CA this SDD specifies; generalizing it is a documented finding (SDD 1.3, sec. 8) for whenever a
 * real use case needs to retry more than one remote call as a unit.
 */
public record TryNode(
        String name,
        String pointer,
        DataFlow dataFlow,
        FlowDirective then,
        CallRemoteNode body,
        Catch catchClause) implements TaskNode {

    /**
     * @param errorFilter    matches the error (as {@code .}) to decide whether this catch applies;
     *                       {@code null} catches any error
     * @param when           extra guard over data/context/error; {@code null} = always true
     * @param exceptWhen     extra veto; {@code null} = never vetoes
     * @param retry          retry policy; {@code null} = no retry, straight to compensation/then
     * @param compensation   optional local-only expression (SDD 1.3, sec. 9 — scope note below) run
     *                       after retries are exhausted or when there's no retry policy at all;
     *                       evaluated the same way a {@code set} node's own expression is
     * @param then           where to continue once the catch is done (not consulted if the error
     *                       propagates uncaught, or if retries are exhausted — see the engine)
     */
    public record Catch(Expr errorFilter, Expr when, Expr exceptWhen, RetryPolicy retry, Expr compensation,
            FlowDirective then) {
    }
}
