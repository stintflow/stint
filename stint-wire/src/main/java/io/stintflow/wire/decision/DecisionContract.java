package io.stintflow.wire.decision;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.wire.Json;

/**
 * SDD 3a.1, RF1/RF4: checks a decision worker's output against the request it answered. The same check serves a
 * worker that wants to fail early and the engine, which is the authority (a worker in another language may not use
 * this class).
 * <p>
 * Messages name fields only, never values: a decision's input may carry sensitive text (RF7), and an output that
 * echoes it must not leak it into an error.
 */
public final class DecisionContract {

    private static final List<String> PROVENANCE_FIELDS = List.of("adapter", "provider", "model", "version");

    private DecisionContract() {
    }

    /** Why {@code output} is not a valid answer to {@code request}, or an empty list. */
    public static List<String> violations(DecisionRequest request, JsonNode output) {
        List<String> problems = new ArrayList<>();
        if (output == null || !output.isObject()) {
            problems.add("the output is not an object");
            return problems;
        }
        JsonNode value = output.get(DecisionResult.VALUE);
        switch (request.kind()) {
            case CHOICE -> {
                if (value == null || !value.isTextual()
                        || request.options().stream().noneMatch(o -> o.name().equals(value.textValue()))) {
                    problems.add("value is not one of the options");
                }
            }
            case BOOLEAN -> {
                if (value == null || !value.isBoolean()) {
                    problems.add("value must be a boolean");
                }
            }
            case SCORE -> {
                if (value == null || !value.isNumber() || !Double.isFinite(value.doubleValue())) {
                    problems.add("value must be a finite number");
                }
            }
        }
        JsonNode confidence = output.get(DecisionResult.CONFIDENCE);
        if (confidence == null || !confidence.isNumber()
                || !(confidence.doubleValue() >= 0 && confidence.doubleValue() <= 1)) {
            problems.add("confidence must be a number in [0,1]");
        }
        JsonNode uncertain = output.get(DecisionResult.UNCERTAIN);
        if (uncertain == null || !uncertain.isBoolean()) {
            problems.add("uncertain must be a boolean");
        }
        JsonNode provenance = output.get(DecisionResult.PROVENANCE);
        if (provenance == null || !provenance.isObject()) {
            problems.add("provenance must be an object");
        } else {
            for (String required : List.of("adapter", "version")) {
                JsonNode field = provenance.get(required);
                if (field == null || !field.isTextual() || field.textValue().isBlank()) {
                    problems.add("provenance." + required + " must be a non-empty string");
                }
            }
            for (String optional : List.of("provider", "model")) {
                JsonNode field = provenance.get(optional);
                if (field != null && !field.isNull() && !field.isTextual()) {
                    problems.add("provenance." + optional + " must be a string or null");
                }
            }
        }
        return problems;
    }

    /**
     * What the engine records about a valid decision (SDD 3a.1, RF5): the kind and the fields of the contract — never
     * the input, the options, or any extra field of the output.
     */
    public static ObjectNode record(DecisionRequest request, JsonNode output) {
        ObjectNode record = Json.obj();
        record.put("kind", request.kind().wireName());
        record.set(DecisionResult.VALUE, output.get(DecisionResult.VALUE));
        record.set(DecisionResult.CONFIDENCE, output.get(DecisionResult.CONFIDENCE));
        record.set(DecisionResult.UNCERTAIN, output.get(DecisionResult.UNCERTAIN));
        ObjectNode provenance = record.putObject(DecisionResult.PROVENANCE);
        JsonNode source = output.get(DecisionResult.PROVENANCE);
        for (String field : PROVENANCE_FIELDS) {
            JsonNode value = source.get(field);
            provenance.set(field, value == null ? Json.MAPPER.nullNode() : value);
        }
        return record;
    }
}
