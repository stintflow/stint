package io.stintflow.core.model;

import io.stintflow.core.expr.Expr;
import io.stintflow.spi.WorkflowRef;

/**
 * SDD 3a.8: a {@code for} whose body is one {@code run: workflow} — the concurrent fan-out. Each item of {@code in}
 * starts a <em>child instance</em> of {@code run}, started by the engine, with a snapshot, memory, trace, retries and
 * dead-letter handling of its own; the {@code for} suspends until the join is decided. DSL 1.0 {@code for.each/in/at}
 * and {@code run.workflow}; the concurrency, join and limit are Stint extensions ({@code metadata["x-stint-*"]}).
 *
 * @param in            evaluates to the array the loop runs over
 * @param each          the name of the loop variable ({@code $<each>} in {@code run.input})
 * @param at            the name of the index variable, or {@code null}
 * @param run           the workflow every item starts
 * @param concurrency   how many children run at once (1 = in series, DSL 1.0's behaviour)
 * @param join          how the children's outcomes become the {@code for}'s output
 * @param maxIterations the longest {@code in} accepted; a longer one fails the {@code for} before any child starts
 */
public record ForRunNode(
        String name,
        String pointer,
        DataFlow dataFlow,
        FlowDirective then,
        Expr in,
        String each,
        String at,
        Run run,
        int concurrency,
        Join join,
        int maxIterations) implements TaskNode {

    /** SDD 3a.8, 5.1: {@code x-stint-join}. */
    public enum Join {
        /** All children succeed; output = the outputs in the order of {@code in}. The first failure fails the {@code for}. */
        ALL,
        /** The first child to succeed ends it; output = {@code {index, output}}. Fails only if all failed. */
        ANY,
        /** Waits for every child, whatever its outcome; output = {@code [{index, status, output | error}]}. */
        COLLECT
    }

    /**
     * {@code run.workflow}: the definition each child runs and the expression of its input (over the item and the
     * loop variables), or {@code null} to pass the item itself.
     */
    public record Run(String namespace, String name, String version, Expr input) {
        public WorkflowRef ref() {
            return new WorkflowRef(namespace, name, version);
        }
    }

    public ForRunNode {
        if (concurrency < 1) {
            throw new IllegalArgumentException("concurrency must be at least 1");
        }
        if (maxIterations < 1) {
            throw new IllegalArgumentException("maxIterations must be at least 1");
        }
    }
}
