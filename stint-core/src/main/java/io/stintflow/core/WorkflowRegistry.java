package io.stintflow.core;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import io.stintflow.spi.WorkflowRef;

/** In-process registry of known workflow definitions, keyed by {@code namespace:name:version}. */
public final class WorkflowRegistry {

    private final Map<String, WorkflowDefinition> byRef = new ConcurrentHashMap<>();

    public void register(WorkflowDefinition def) {
        byRef.put(def.ref().canonical(), def);
    }

    public Optional<WorkflowDefinition> find(WorkflowRef ref) {
        return Optional.ofNullable(byRef.get(ref.canonical()));
    }
}
