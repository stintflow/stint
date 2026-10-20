package io.stintflow.worker.decision;

/**
 * Who an adapter is (SDD 3a.2, 4.7): what goes into a decision's provenance, and what the sensitivity policy of SDD
 * 3a.5 decides on.
 *
 * @param id       the adapter's id: the provenance's {@code adapter}, and its name in the configuration
 * @param provider who runs the model (e.g. {@code openai}, {@code typesafe}), or {@code null} for local rules
 * @param model    the configured model, or {@code null}; an answer may name the exact model that answered
 * @param version  the adapter's version — for an LLM adapter, its prompt's version
 * @param remote   whether the decision's input leaves the worker's process
 */
public record AdapterDescriptor(String id, String provider, String model, String version, boolean remote) {

    public AdapterDescriptor {
        if (id == null || id.isBlank() || id.contains(".")) {
            throw new IllegalArgumentException("An adapter id is required and has no dots (it is a configuration key)");
        }
        if (version == null || version.isBlank()) {
            throw new IllegalArgumentException("An adapter version is required");
        }
    }
}
