package io.stintflow.wire.decision;

/**
 * SDD 3a.1, RF1: one answer a decision may give. For {@link DecisionKind#BOOLEAN} the names are {@code true} and
 * {@code false}.
 *
 * @param name        the value the decision returns when it picks this option
 * @param description what the option means, for the adapter (e.g. part of an LLM prompt); may be {@code null}
 */
public record DecisionOption(String name, String description) {

    public DecisionOption {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("a decision option needs a name");
        }
    }
}
