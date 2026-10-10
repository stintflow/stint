package io.stintflow.core.model;

import io.stintflow.core.expr.Expr;

/**
 * A task's data-flow expressions (DSL 1.0, RF5). Each is optional ({@code null}); a {@code null}
 * expression means the DSL-documented identity default — the interpreter applies that default, not
 * this record.
 *
 * @param inputFrom transforms the incoming data into this task's input ({@code $input})
 * @param outputAs  transforms the task's raw output before it becomes the next task's input
 * @param exportAs  evaluated over the transformed output; its result replaces {@code $context}
 */
public record DataFlow(Expr inputFrom, Expr outputAs, Expr exportAs) {

    public static final DataFlow NONE = new DataFlow(null, null, null);
}
