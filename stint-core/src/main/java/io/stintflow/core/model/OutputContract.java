package io.stintflow.core.model;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * SDD 3a.1, RF4/RF5: a contract a {@link CallRemoteNode}'s successful output must meet. The engine checks it before
 * applying the output: a violation becomes a standard {@code validation} error raised at the task, catchable like
 * any failure of a try's body. The core knows nothing of what the contract is about (a decision, in SDD 3a.1) — the
 * DSL builds it.
 */
public interface OutputContract {

    /** What the error and the log call an output out of contract, e.g. {@code Decision output out of contract}. */
    String title();

    /**
     * Why {@code output} breaks the contract, or an empty list. Messages must not repeat values of the output: they
     * end up in the error's {@code detail}.
     */
    List<String> violations(JsonNode output);

    /**
     * What the engine records in the snapshot ({@code InstanceSnapshot.decisions}, by the task's pointer) about a
     * valid output, or {@code null} to record nothing. Must not carry the task's input.
     */
    JsonNode record(JsonNode output);
}
