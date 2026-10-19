package io.stintflow.wire.redaction;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.wire.Json;

/**
 * SDD 3a.5, RF5: the contract of a redaction, what {@code call: redact} dispatches and checks. A redactor (the
 * reference one of {@code stint-worker-redaction}, or the sandbox's layered filter) answers:
 * <pre>
 * { "output": &lt;the input, redacted&gt;,
 *   "counts": { "email": 2, "person": 1 },          // numbers only — never a value found
 *   "doubt": false,                                  // true when any layer could not decide
 *   "provenance": { "redactor": "patterns", "version": "1", "layers": ["patterns"] } }
 * </pre>
 * The engine lowers the instance's level only for a valid result without doubt. Messages name fields only, never
 * values: the output is redacted text, but an invalid one may not be.
 */
public final class RedactionContract {

    public static final String OUTPUT = "output";
    public static final String COUNTS = "counts";
    public static final String DOUBT = "doubt";
    public static final String PROVENANCE = "provenance";
    /** The request: {@code {"input": <JSON>, "types": [..]}} ({@code types} empty = every type the redactor knows). */
    public static final String INPUT = "input";
    public static final String TYPES = "types";
    /**
     * SDD 3b.2, 4.6: optional, in the request: {@code {"pseudonyms": {"mode": "scope", "scope": "<ns>/<base>"}}} asks for pseudonyms
     * that are the same in every meeting of one knowledge base (128 bits, no instance in them) — a worker that allows it
     * answers so; any other answers with the pseudonyms of the instance. The scope is a name, never a person.
     */
    public static final String PSEUDONYMS = "pseudonyms";
    public static final String PSEUDONYMS_MODE = "mode";
    public static final String PSEUDONYMS_SCOPE = "scope";
    public static final String MODE_SCOPE = "scope";

    private RedactionContract() {
    }

    /** Why {@code output} is not a valid redaction result, or an empty list. */
    public static List<String> violations(JsonNode output) {
        List<String> problems = new ArrayList<>();
        if (output == null || !output.isObject()) {
            problems.add("the output is not an object");
            return problems;
        }
        if (!output.has(OUTPUT)) {
            problems.add("output is missing");
        }
        JsonNode counts = output.get(COUNTS);
        if (counts == null || !counts.isObject()) {
            problems.add("counts must be an object");
        } else {
            counts.properties().forEach(e -> {
                if (!e.getValue().canConvertToInt() || e.getValue().asInt() < 0) {
                    problems.add("counts." + e.getKey() + " must be a whole number, at least 0");
                }
            });
        }
        JsonNode doubt = output.get(DOUBT);
        if (doubt == null || !doubt.isBoolean()) {
            problems.add("doubt must be a boolean");
        }
        JsonNode provenance = output.get(PROVENANCE);
        if (provenance == null || !provenance.isObject()) {
            problems.add("provenance must be an object");
        } else {
            for (String required : List.of("redactor", "version")) {
                JsonNode field = provenance.get(required);
                if (field == null || !field.isTextual() || field.textValue().isBlank()) {
                    problems.add("provenance." + required + " must be a non-empty string");
                }
            }
        }
        return problems;
    }

    /** Whether a valid result has no doubt — the only case that may lower the level. */
    public static boolean certain(JsonNode output) {
        return output != null && output.path(DOUBT).isBoolean() && !output.path(DOUBT).booleanValue();
    }

    /** What the engine records and audits: counts, doubt and provenance — never the output. */
    public static ObjectNode record(JsonNode output) {
        ObjectNode record = Json.obj();
        record.set(COUNTS, output.path(COUNTS).deepCopy());
        record.put(DOUBT, output.path(DOUBT).asBoolean(true));
        JsonNode provenance = output.get(PROVENANCE);
        if (provenance != null && provenance.isObject()) {
            ObjectNode kept = record.putObject(PROVENANCE);
            for (String field : List.of("redactor", "version")) {
                if (provenance.path(field).isTextual()) {
                    kept.put(field, provenance.path(field).asText());
                }
            }
            if (provenance.path("layers").isArray()) {
                kept.set("layers", provenance.get("layers").deepCopy());
            }
        }
        return record;
    }
}
