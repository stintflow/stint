package io.stintflow.core.model;

import java.time.Duration;

import io.stintflow.core.expr.Expr;

/**
 * {@code call: remote} (Stint extension, see SDD 1.4 sec. 8): dispatches a task over the
 * {@link io.stintflow.spi.TaskTransport} and suspends the instance until its result arrives.
 *
 * @param routingKey transport routing hint (CloudEvent {@code subject}) selecting the worker
 * @param timeout    {@code timeout.after} (SDD 1.3, RF3); {@code null} = use the engine's configured
 *                   default. Validated against {@link io.stintflow.spi.TimerService#maxDelay()} when
 *                   the definition is registered (SDD 1.3, sec. 8d) — not at execution time.
 * @param withInput  SDD 2.9, RF4: {@code with.input}, the call's argument, evaluated over the task's input after
 *                   {@code input.from} (DSL 1.0: input transformation first, then the call's {@code with}). It is
 *                   what the worker receives; {@code null} = the task's (transformed) input as is.
 */
public record CallRemoteNode(
        String name,
        String pointer,
        DataFlow dataFlow,
        FlowDirective then,
        String routingKey,
        Duration timeout,
        Expr withInput) implements TaskNode {

    /** Without {@code with.input} (built in code, or a definition that declares none). */
    public CallRemoteNode(String name, String pointer, DataFlow dataFlow, FlowDirective then, String routingKey,
            Duration timeout) {
        this(name, pointer, dataFlow, then, routingKey, timeout, null);
    }
}
