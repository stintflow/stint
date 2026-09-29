package io.stintflow.core.interpreter;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.core.model.CallRemoteNode;
import io.stintflow.core.model.TryNode;

/**
 * Outcome of {@link TreeInterpreter#run}: either the walk hit a {@link CallRemoteNode} (or a
 * {@link TryNode}'s body) and must suspend for dispatch, the workflow instance finished, or it
 * failed (e.g. the local node limit).
 * <p>
 * SDD 2.2: every outcome also carries the facts the activation {@link #emitted()} on its way there, in
 * emission order — the engine records them in the outbox with the same save as the outcome.
 */
public sealed interface InterpretResult {

    List<Emitted> emitted();

    /** The same outcome, carrying {@code emitted} instead. */
    InterpretResult withEmitted(List<Emitted> emitted);

    record Suspend(CallRemoteNode node, String pointer, JsonNode dispatchInput, JsonNode context,
                   List<Emitted> emitted) implements InterpretResult {
        @Override
        public InterpretResult withEmitted(List<Emitted> emitted) {
            return new Suspend(node, pointer, dispatchInput, context, List.copyOf(emitted));
        }
    }

    /** SDD 1.3: entering a {@link TryNode} — its body is what gets dispatched, but the engine needs
     *  {@code tryNode} too, to set up {@link io.stintflow.spi.RetryState} and evaluate its catch. */
    record SuspendInTry(TryNode tryNode, JsonNode dispatchInput, JsonNode context, List<Emitted> emitted)
            implements InterpretResult {
        @Override
        public InterpretResult withEmitted(List<Emitted> emitted) {
            return new SuspendInTry(tryNode, dispatchInput, context, List.copyOf(emitted));
        }
    }

    record Complete(String pointer, JsonNode context, List<Emitted> emitted) implements InterpretResult {
        @Override
        public InterpretResult withEmitted(List<Emitted> emitted) {
            return new Complete(pointer, context, List.copyOf(emitted));
        }
    }

    record Failed(String pointer, JsonNode context, String message, List<Emitted> emitted)
            implements InterpretResult {
        @Override
        public InterpretResult withEmitted(List<Emitted> emitted) {
            return new Failed(pointer, context, message, List.copyOf(emitted));
        }
    }

    static InterpretResult suspend(CallRemoteNode node, String pointer, JsonNode dispatchInput, JsonNode context) {
        return new Suspend(node, pointer, dispatchInput, context, List.of());
    }

    static InterpretResult suspendInTry(TryNode tryNode, JsonNode dispatchInput, JsonNode context) {
        return new SuspendInTry(tryNode, dispatchInput, context, List.of());
    }

    static InterpretResult complete(String pointer, JsonNode context) {
        return new Complete(pointer, context, List.of());
    }

    static InterpretResult failed(String pointer, JsonNode context, String message) {
        return new Failed(pointer, context, message, List.of());
    }
}
