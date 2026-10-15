package io.stintflow.worker.langchain4j;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Checks a task's output beyond its type (SDD 3a.3, 5.5). Violations name fields only, never their values: they go
 * into the error's {@code detail}, which travels on the wire and into logs.
 */
@FunctionalInterface
public interface OutputValidator {

    List<String> violations(JsonNode output);

    OutputValidator NONE = output -> List.of();
}
