package io.stintflow.worker.decision;

import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * An adapter's raw answer (SDD 3a.2, 4.2), before the threshold.
 *
 * @param value        the answer: an option's name, a boolean or a number, as the request's kind asks
 * @param confidence   in [0, 1], or {@code null} when the adapter got none — which makes the decision uncertain
 * @param distribution the probability of each candidate value (option name, {@code true}/{@code false}, score level),
 *                     or {@code null}; two candidates at the top make the decision uncertain
 * @param model        the model that answered, if the provider said; else the descriptor's
 * @param usage        what the answer cost, or {@code null}
 */
public record Decision(JsonNode value, Double confidence, Map<String, Double> distribution, String model,
        DecisionUsage usage) {

    public Decision {
        distribution = distribution == null ? null : Map.copyOf(distribution);
    }

    public static Decision of(JsonNode value, Double confidence) {
        return new Decision(value, confidence, null, null, null);
    }
}
