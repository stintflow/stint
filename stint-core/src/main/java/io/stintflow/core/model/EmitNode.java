package io.stintflow.core.model;

import io.stintflow.core.expr.Expr;

/**
 * {@code emit} (SDD 2.2, RF2): publishes a domain fact. DSL 1.0 {@code dsl-reference.md}, Emit:
 * "Allows workflows to publish events to event brokers or messaging systems"; {@code emit.event.with}
 * carries the CloudEvent attributes ({@code source} and {@code type} required, {@code id}, {@code time},
 * {@code subject}, {@code datacontenttype}, {@code dataschema}, {@code data} and extensions optional).
 * <p>
 * A local node: it never suspends. {@link #event()} evaluates, against the node's input, to the
 * {@code with} object; the fact is recorded in the outbox with the state the activation saves and
 * published after that save (sec. 8a). The node's raw output is its input, unchanged — the spec
 * doesn't define an emit's output; this is Stint's choice.
 *
 * @param declaredType the {@code type} when it is a literal (checked against the reserved internal
 *                     prefixes at registration, sec. 8e), or {@code null} when it's an expression
 */
public record EmitNode(
        String name,
        String pointer,
        DataFlow dataFlow,
        FlowDirective then,
        Expr event,
        String declaredType) implements TaskNode {
}
