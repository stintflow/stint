package io.stintflow.worker.redaction;

import java.util.Map;
import java.util.Set;

/**
 * One layer of a redaction (SDD 3a.5, 4.4): it gets the text as the previous layers left it and replaces what it finds
 * with pseudonyms. A pipeline composes layers with {@link LayeredRedactor} — the reference {@link PatternLayer}, a
 * {@link TermListLayer}, and whatever else it has (a local detector, a yes/no question to a local decision model).
 */
public interface RedactionLayer {

    /** The layer's name, recorded in the result's provenance. */
    String name();

    /** Redacts {@code text}. Never returns a found value outside the redacted text. */
    Result apply(String text, Scope scope);

    /**
     * What one application found.
     *
     * @param text   the text with every finding replaced by its pseudonym
     * @param counts how many of each type — numbers only
     * @param doubt  the layer could not decide about something in the text
     */
    record Result(String text, Map<String, Integer> counts, boolean doubt) {

        public Result {
            counts = Map.copyOf(counts);
        }
    }

    /**
     * What a layer knows about the redaction it is part of.
     *
     * @param instanceId the instance, for its pseudonyms
     * @param types      the types asked for; empty = every type the layer knows
     * @param pseudonyms the labels stable within the instance
     */
    record Scope(String instanceId, Set<String> types, Pseudonyms pseudonyms) {

        public Scope {
            types = Set.copyOf(types);
        }

        public boolean wants(String type) {
            return types.isEmpty() || types.contains(type);
        }
    }
}
