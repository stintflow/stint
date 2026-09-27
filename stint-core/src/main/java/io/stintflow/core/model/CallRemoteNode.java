package io.stintflow.core.model;

/**
 * {@code call: remote} (Stint extension, see SDD 1.4 sec. 8): dispatches a task over the
 * {@link io.stintflow.spi.TaskTransport} and suspends the instance until its result arrives.
 *
 * @param routingKey transport routing hint (CloudEvent {@code subject}) selecting the worker
 */
public record CallRemoteNode(
        String name,
        String pointer,
        DataFlow dataFlow,
        FlowDirective then,
        String routingKey) implements TaskNode {
}
