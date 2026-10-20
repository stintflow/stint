package io.stintflow.wire.decision;

import java.util.Locale;
import java.util.Optional;

/** SDD 3a.1, RF1: what a decision answers — one of its options, a number, or yes/no. */
public enum DecisionKind {
    CHOICE, SCORE, BOOLEAN;

    /** The name used in the DSL and on the wire ({@code choice}, {@code score}, {@code boolean}). */
    public String wireName() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static Optional<DecisionKind> fromWireName(String name) {
        for (DecisionKind kind : values()) {
            if (kind.wireName().equals(name)) {
                return Optional.of(kind);
            }
        }
        return Optional.empty();
    }
}
