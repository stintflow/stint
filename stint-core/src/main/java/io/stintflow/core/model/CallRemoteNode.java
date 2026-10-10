package io.stintflow.core.model;

import java.time.Duration;

/**
 * {@code call: remote} (Stint extension, see SDD 1.4 sec. 8): dispatches a task over the
 * {@link io.stintflow.spi.TaskTransport} and suspends the instance until its result arrives.
 *
 * @param routingKey transport routing hint (CloudEvent {@code subject}) selecting the worker
 * @param timeout    {@code timeout.after} (SDD 1.3, RF3); {@code null} = use the engine's configured
 *                   default. Validated against {@link io.stintflow.spi.TimerService#maxDelay()} when
 *                   the definition is registered (SDD 1.3, sec. 8d) — not at execution time.
 */
public record CallRemoteNode(
        String name,
        String pointer,
        DataFlow dataFlow,
        FlowDirective then,
        String routingKey,
        Duration timeout) implements TaskNode {
}
