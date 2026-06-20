package io.stintflow.core;

import java.util.function.BiFunction;
import java.util.function.Function;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * A single remote task in the workflow: it is dispatched over the {@link io.stintflow.spi.TaskTransport}
 * rather than executed in-process.
 *
 * @param id          task id (the position in the workflow)
 * @param routingKey  transport routing hint → CloudEvent {@code subject} → selects which worker runs it
 * @param inputMapper builds the task input from the current workflow context
 * @param outputMerger merges the task output back into the workflow context
 */
public record RemoteStep(
        String id,
        String routingKey,
        Function<JsonNode, JsonNode> inputMapper,
        BiFunction<JsonNode, JsonNode, JsonNode> outputMerger) {
}
