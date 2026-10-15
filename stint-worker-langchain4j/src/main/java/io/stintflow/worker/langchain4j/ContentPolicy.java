package io.stintflow.worker.langchain4j;

import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.worker.TaskContext;

/**
 * The extension point for the sensitivity policy of SDD 3a.5 (SDD 3a.3, 5.6): whether an execution's content — its
 * prompt and response messages — may be recorded anywhere, and where. Nothing in the module records content
 * otherwise. The default, {@link #NONE}, records nothing.
 */
@FunctionalInterface
public interface ContentPolicy {

    /** Where this execution's content may be recorded, if anywhere. */
    Optional<ContentSink> sinkFor(TaskContext task);

    ContentPolicy NONE = task -> Optional.empty();

    /** Receives the messages a successful execution added (as LangChain4j serializes them) and its output. */
    @FunctionalInterface
    interface ContentSink {
        void record(TaskContext task, JsonNode messages, JsonNode output);
    }
}
