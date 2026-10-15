package io.stintflow.wire.decision;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.wire.Json;

/**
 * SDD 3a.1, RF1: what a decision worker receives as its task input —
 * {@code {"kind": ..., "options": [{"name", "description"}], "input": ...}}.
 *
 * @param kind    what the decision answers
 * @param options for {@link DecisionKind#CHOICE}, at least two with distinct names; for {@link DecisionKind#BOOLEAN},
 *                none or exactly {@code true}/{@code false}; for {@link DecisionKind#SCORE}, none
 * @param input   the data the decision is about; {@code null} in a request's {@link #shape}
 */
public record DecisionRequest(DecisionKind kind, List<DecisionOption> options, JsonNode input) {

    public static final String KIND = "kind";
    public static final String OPTIONS = "options";
    public static final String INPUT = "input";

    public DecisionRequest {
        if (kind == null) {
            throw new IllegalArgumentException("a decision needs a kind");
        }
        options = options == null ? List.of() : List.copyOf(options);
        List<String> problems = optionProblems(kind, options);
        if (!problems.isEmpty()) {
            throw new IllegalArgumentException(String.join("; ", problems));
        }
    }

    /** The request without its input: what the DSL knows at load time and what a result is checked against. */
    public static DecisionRequest shape(DecisionKind kind, List<DecisionOption> options) {
        return new DecisionRequest(kind, options, null);
    }

    /** Why {@code options} does not fit {@code kind}, or an empty list. */
    public static List<String> optionProblems(DecisionKind kind, List<DecisionOption> options) {
        List<String> problems = new ArrayList<>();
        Set<String> names = new HashSet<>();
        for (DecisionOption option : options) {
            if (!names.add(option.name())) {
                problems.add("option '" + option.name() + "' is declared twice");
            }
        }
        switch (kind) {
            case CHOICE -> {
                if (options.size() < 2) {
                    problems.add("a choice needs at least two options");
                }
            }
            case BOOLEAN -> {
                if (!options.isEmpty() && !(options.size() == 2 && names.equals(Set.of("true", "false")))) {
                    problems.add("the options of a boolean decision, when declared, are exactly 'true' and 'false'");
                }
            }
            case SCORE -> {
                if (!options.isEmpty()) {
                    problems.add("a score decision has no options");
                }
            }
        }
        return problems;
    }

    /** The options as they travel in the request: {@code [{"name": ..., "description": ...}]}. */
    public ArrayNode optionsJson() {
        ArrayNode array = Json.MAPPER.createArrayNode();
        for (DecisionOption option : options) {
            ObjectNode node = array.addObject();
            node.put("name", option.name());
            if (option.description() != null) {
                node.put("description", option.description());
            }
        }
        return array;
    }

    public ObjectNode toJson() {
        ObjectNode node = Json.obj();
        node.put(KIND, kind.wireName());
        node.set(OPTIONS, optionsJson());
        node.set(INPUT, input == null ? Json.MAPPER.nullNode() : input);
        return node;
    }

    /**
     * Reads a request as a worker receives it.
     *
     * @throws IllegalArgumentException if it is not a decision request
     */
    public static DecisionRequest fromJson(JsonNode node) {
        if (node == null || !node.isObject()) {
            throw new IllegalArgumentException("a decision request is an object");
        }
        DecisionKind kind = DecisionKind.fromWireName(node.path(KIND).asText(null))
                .orElseThrow(() -> new IllegalArgumentException("unknown decision kind"));
        List<DecisionOption> options = new ArrayList<>();
        for (JsonNode option : node.path(OPTIONS)) {
            options.add(new DecisionOption(option.path("name").asText(null),
                    option.path("description").isTextual() ? option.path("description").asText() : null));
        }
        return new DecisionRequest(kind, options, node.get(INPUT));
    }
}
