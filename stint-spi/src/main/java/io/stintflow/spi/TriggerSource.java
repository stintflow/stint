package io.stintflow.spi;

import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Port: ingress — how a workflow instance gets started from the outside world
 * (HTTP, an inbound queue, an EventBridge rule, a Knative source, ...).
 * <p>
 * Optional: instances can also be started programmatically through the engine. Connectors that
 * provide external ingress deliver start requests to the registered handler.
 */
public interface TriggerSource {

    void onTrigger(StartHandler handler);

    @FunctionalInterface
    interface StartHandler {
        /** @return the started instance id. */
        CompletionStage<String> start(WorkflowRef definition, JsonNode input);
    }
}
