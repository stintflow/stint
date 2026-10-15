package io.stintflow.wire.decision;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.wire.Json;

/**
 * SDD 3a.1, RF1: what a decision worker returns as its task output —
 * {@code {"value", "confidence", "uncertain", "provenance": {"adapter", "provider", "model", "version"}}}.
 * Extra fields are allowed and ignored, so the contract can grow additively.
 *
 * @param value      for a choice, the name of one of the options; for a score, a finite number; for a boolean, a boolean
 * @param confidence in [0, 1]
 * @param uncertain  whether the adapter considers the answer below its threshold (SDD 3a.2); the engine trusts it
 * @param provenance who decided
 */
public record DecisionResult(JsonNode value, double confidence, boolean uncertain, Provenance provenance) {

    public static final String VALUE = "value";
    public static final String CONFIDENCE = "confidence";
    public static final String UNCERTAIN = "uncertain";
    public static final String PROVENANCE = "provenance";

    /**
     * @param adapter  the adapter that decided (required)
     * @param provider the model provider, or {@code null} for an adapter without one (e.g. rules)
     * @param model    the model, or {@code null}
     * @param version  the adapter's (or model's) version (required)
     */
    public record Provenance(String adapter, String provider, String model, String version) {

        public ObjectNode toJson() {
            ObjectNode node = Json.obj();
            node.put("adapter", adapter);
            node.put("provider", provider);
            node.put("model", model);
            node.put("version", version);
            return node;
        }
    }

    public ObjectNode toJson() {
        ObjectNode node = Json.obj();
        node.set(VALUE, value);
        node.put(CONFIDENCE, confidence);
        node.put(UNCERTAIN, uncertain);
        node.set(PROVENANCE, provenance.toJson());
        return node;
    }
}
