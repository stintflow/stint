package io.stintflow.worker;

import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * The uniform worker programming model: business logic for one task type. Write it once; the binding
 * (Lambda handler, Knative HTTP receiver, or a pull-based pool runner) is what differs per platform.
 */
@FunctionalInterface
public interface TaskHandler {
    CompletionStage<JsonNode> execute(TaskContext ctx);
}
