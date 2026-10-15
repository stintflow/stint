package io.stintflow.spi;

import java.util.Locale;
import java.util.Optional;

/**
 * SDD 3a.5, RF1: how sensitive the data of a workflow instance is — {@link #INTERNAL} < {@link #CONFIDENTIAL} <
 * {@link #RESTRICTED}. An instance's level only rises (by an event of a higher level), and only a verified redaction
 * lowers it. Carried on the wire as the CloudEvents extension {@code stintsensitivity}; a value that isn't a level
 * reads as {@link #RESTRICTED} — the safe side.
 */
public enum Sensitivity {
    INTERNAL,
    CONFIDENTIAL,
    RESTRICTED;

    /** The wire name: {@code internal}, {@code confidential} or {@code restricted}. */
    public String wireName() {
        return name().toLowerCase(Locale.ROOT);
    }

    /** The level named {@code value}, or empty when it names none (the caller decides what that means). */
    public static Optional<Sensitivity> parse(String value) {
        if (value == null) {
            return Optional.empty();
        }
        for (Sensitivity level : values()) {
            if (level.wireName().equals(value.trim().toLowerCase(Locale.ROOT))) {
                return Optional.of(level);
            }
        }
        return Optional.empty();
    }

    /** The level named {@code value}; anything else — absent included — is {@link #RESTRICTED}. */
    public static Sensitivity parseOrRestricted(String value) {
        return parse(value).orElse(RESTRICTED);
    }

    /** The higher of two levels; {@code null} counts as absent. */
    public static Sensitivity max(Sensitivity a, Sensitivity b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return a.compareTo(b) >= 0 ? a : b;
    }

    public boolean atLeast(Sensitivity other) {
        return compareTo(other) >= 0;
    }
}
