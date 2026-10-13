package io.stintflow.core.model;

import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.spi.Sensitivity;

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

    /**
     * SDD 3a.5, RF5: the level the instance's data has after this valid output — a verified redaction lowers it — or
     * empty to keep it. The engine never lets it rise above the instance's current level this way.
     */
    default Optional<Sensitivity> sensitivityAfter(JsonNode output) {
        return Optional.empty();
    }

    /** SDD 3a.5, RF8: the audit kind of a valid output (e.g. {@code redaction}), or {@code null} for the default. */
    default String auditKind() {
        return null;
    }
}
