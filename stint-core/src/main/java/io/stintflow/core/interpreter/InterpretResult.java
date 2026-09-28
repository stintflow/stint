package io.stintflow.core.interpreter;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.core.model.CallRemoteNode;
import io.stintflow.core.model.TryNode;

/**
 * Outcome of {@link TreeInterpreter#run}: either the walk hit a {@link CallRemoteNode} (or a
 * {@link TryNode}'s body) and must suspend for dispatch, the workflow instance finished, or it
 * failed (e.g. the local node limit).
 */
public sealed interface InterpretResult {

    record Suspend(CallRemoteNode node, String pointer, JsonNode dispatchInput, JsonNode context)
            implements InterpretResult {
    }

    /** SDD 1.3: entering a {@link TryNode} — its body is what gets dispatched, but the engine needs
     *  {@code tryNode} too, to set up {@link io.stintflow.spi.RetryState} and evaluate its catch. */
    record SuspendInTry(TryNode tryNode, JsonNode dispatchInput, JsonNode context) implements InterpretResult {
    }

    record Complete(String pointer, JsonNode context) implements InterpretResult {
    }

    record Failed(String pointer, JsonNode context, String message) implements InterpretResult {
    }

    static InterpretResult suspend(CallRemoteNode node, String pointer, JsonNode dispatchInput, JsonNode context) {
        return new Suspend(node, pointer, dispatchInput, context);
    }

    static InterpretResult suspendInTry(TryNode tryNode, JsonNode dispatchInput, JsonNode context) {
        return new SuspendInTry(tryNode, dispatchInput, context);
    }

    static InterpretResult complete(String pointer, JsonNode context) {
        return new Complete(pointer, context);
    }

    static InterpretResult failed(String pointer, JsonNode context, String message) {
        return new Failed(pointer, context, message);
    }
}
