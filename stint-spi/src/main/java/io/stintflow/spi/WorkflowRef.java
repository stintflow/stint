package io.stintflow.spi;

/**
 * Uniquely identifies a workflow definition by {@code namespace:name:version}.
 * <p>
 * This is part of the agnostic contract — it carries no notion of where or how the workflow runs.
 */
public record WorkflowRef(String namespace, String name, String version) {

    public WorkflowRef {
        if (namespace == null || name == null || version == null) {
            throw new IllegalArgumentException("namespace, name and version are required");
        }
    }

    /** Canonical {@code namespace:name:version} form, used as routing/lookup key. */
    public String canonical() {
        return namespace + ":" + name + ":" + version;
    }

    public static WorkflowRef parse(String canonical) {
        String[] parts = canonical.split(":", 3);
        if (parts.length != 3) {
            throw new IllegalArgumentException("Expected namespace:name:version, got: " + canonical);
        }
        return new WorkflowRef(parts[0], parts[1], parts[2]);
    }
}
