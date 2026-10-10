package io.stintflow.core.model;

import io.stintflow.core.expr.Expr;

/**
 * {@code try}/{@code catch}/{@code retry} (SDD 1.3, RF5). Scope note: the try'd {@link #task()} is
 * a single {@link CallRemoteNode} or (SDD 2.3, sec. 8d — so a {@code listen} timeout can be caught) a
 * single {@link ListenNode}, not an arbitrary sub-tree; generalizing it is a documented finding (SDD 1.3,
 * sec. 8) for whenever a real use case needs to retry more than one remote call as a unit.
 */
public record TryNode(
        String name,
        String pointer,
        DataFlow dataFlow,
        FlowDirective then,
        TaskNode task,
        Catch catchClause) implements TaskNode {

    public TryNode {
        if (!(task instanceof CallRemoteNode) && !(task instanceof ListenNode)) {
            throw new IllegalArgumentException("A try body must be a single call: remote or listen, got: " + task);
        }
    }

    /** The try'd remote call. @throws IllegalStateException if the body is a {@code listen} — see {@link #listen()} */
    public CallRemoteNode body() {
        if (task instanceof CallRemoteNode remote) {
            return remote;
        }
        throw new IllegalStateException("The body of try " + pointer + " is a listen, not a remote call");
    }

    /** The try'd {@code listen}, or {@code null} if the body is a remote call (SDD 2.3). */
    public ListenNode listen() {
        return task instanceof ListenNode listen ? listen : null;
    }

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
