package io.stintflow.core.interpreter;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.core.model.CallRemoteNode;

/**
 * Outcome of {@link TreeInterpreter#run}: either the walk hit a {@link CallRemoteNode} and must
 * suspend for dispatch, the workflow instance finished, or it failed (e.g. the local node limit).
 */
public sealed interface InterpretResult {

    record Suspend(CallRemoteNode node, String pointer, JsonNode dispatchInput, JsonNode context)
            implements InterpretResult {
    }

    record Complete(String pointer, JsonNode context) implements InterpretResult {
    }

    record Failed(String pointer, JsonNode context, String message) implements InterpretResult {
    }

    static InterpretResult suspend(CallRemoteNode node, String pointer, JsonNode dispatchInput, JsonNode context) {
        return new Suspend(node, pointer, dispatchInput, context);
    }

    static InterpretResult complete(String pointer, JsonNode context) {
        return new Complete(pointer, context);
    }

    static InterpretResult failed(String pointer, JsonNode context, String message) {
        return new Failed(pointer, context, message);
    }
}
