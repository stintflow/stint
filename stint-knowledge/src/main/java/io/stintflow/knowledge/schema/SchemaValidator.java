package io.stintflow.knowledge.schema;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.knowledge.schema.KnowledgeSchema.NodeType;
import io.stintflow.knowledge.schema.KnowledgeSchema.Property;
import io.stintflow.knowledge.schema.KnowledgeSchema.RelationType;

/**
 * Checks what an extraction answered against a schema (SDD 3b.2, 4.3), by fields and rules alone: a violation names the path
 * and the rule, never a value of the answer — it travels in the error's {@code detail} and into logs, and the answer may
 * hold what the model made of the meeting.
 * <pre>
 * { "entities":  [ { "ref": "e1", "type": "System", "mention": "checkout service", "glossaryId": "sys-1"?,
 *                    "properties": { ... }?, "passages": [0, 2] } ],
 *   "relations": [ { "type": "affects", "from": "e1", "to": "e2" } ] }
 * </pre>
 * The key property of a type (its name, its pseudonym) is the {@code mention}. The meeting itself is not the model's to
 * create: the worker adds it as {@value #MEETING_REF}, which the relations may name.
 */
public final class SchemaValidator {

    /** The ref of the meeting node the worker creates; a relation may use it as an end. */
    public static final String MEETING_REF = "meeting";
    public static final int MAX_ENTITIES = 200;
    public static final int MAX_RELATIONS = 400;
    public static final int MAX_MENTION = 200;
    public static final int MAX_VALUE = 1_000;
    private static final Pattern REF = Pattern.compile("[A-Za-z0-9_-]{1,40}");
    private static final Set<String> ENTITY_KEYS = Set.of("ref", "type", "mention", "glossaryId", "properties", "passages");
    private static final Set<String> RELATION_KEYS = Set.of("type", "from", "to");

    private SchemaValidator() {
    }

    /** The violations of {@code result} against {@code schema}; empty when it is valid. */
    public static List<String> violations(KnowledgeSchema schema, JsonNode result, int passageCount) {
        List<String> problems = new ArrayList<>();
        if (result == null || !result.isObject()) {
            problems.add("$: the answer is not an object");
            return problems;
        }
        result.fieldNames().forEachRemaining(name -> {
            if (!name.equals("entities") && !name.equals("relations")) {
                problems.add("$: a field that is not part of the answer");
            }
        });
        JsonNode entities = result.get("entities");
        JsonNode relations = result.get("relations");
        if (entities == null || !entities.isArray()) {
            problems.add("$.entities: must be an array");
            return problems;
        }
        if (relations != null && !relations.isArray()) {
            problems.add("$.relations: must be an array");
            return problems;
        }
        if (entities.size() > MAX_ENTITIES) {
            problems.add("$.entities: more than " + MAX_ENTITIES);
        }
        if (relations != null && relations.size() > MAX_RELATIONS) {
            problems.add("$.relations: more than " + MAX_RELATIONS);
        }
        Map<String, String> typeOfRef = new HashMap<>();
        typeOfRef.put(MEETING_REF, "Meeting");
        for (int i = 0; i < entities.size() && i < MAX_ENTITIES; i++) {
            entity(schema, entities.get(i), "$.entities[" + i + "]", passageCount, typeOfRef, problems);
        }
        if (relations != null) {
            for (int i = 0; i < relations.size() && i < MAX_RELATIONS; i++) {
                relation(schema, relations.get(i), "$.relations[" + i + "]", typeOfRef, problems);
            }
        }
        return problems;
    }

    private static void entity(KnowledgeSchema schema, JsonNode e, String path, int passageCount,
            Map<String, String> typeOfRef, List<String> problems) {
        if (!e.isObject()) {
            problems.add(path + ": must be an object");
            return;
        }
        e.fieldNames().forEachRemaining(name -> {
            if (!ENTITY_KEYS.contains(name)) {
                problems.add(path + ": a field that is not part of an entity");
            }
        });
        String ref = text(e, "ref");
        if (ref == null || !REF.matcher(ref).matches()) {
            problems.add(path + ".ref: required, 1 to 40 letters, digits, '_' or '-'");
        } else if (ref.equals(MEETING_REF) || typeOfRef.containsKey(ref)) {
            problems.add(path + ".ref: already used (or reserved)");
        }
        String typeName = text(e, "type");
        NodeType type = typeName == null ? null : schema.nodes().get(typeName);
        if (type == null) {
            problems.add(path + ".type: not a node type of " + schema.fullVersion());
        } else if (type.name().equals("Meeting")) {
            problems.add(path + ".type: the meeting is created by the worker, not by the model");
        }
        if (ref != null && REF.matcher(ref).matches() && type != null) {
            typeOfRef.putIfAbsent(ref, type.name());
        }
        String mention = text(e, "mention");
        if (mention == null || mention.isBlank() || mention.length() > MAX_MENTION) {
            problems.add(path + ".mention: required, 1 to " + MAX_MENTION + " characters");
        } else if (type != null) {
            Property key = type.properties().get(type.key());
            if (key.pattern() != null && !Pattern.matches(key.pattern(), mention)) {
                problems.add(path + ".mention: does not match the form of the key of " + type.name()
                        + " (a pseudonym, never a name)");
            }
        }
        if (e.has("glossaryId") && !e.get("glossaryId").isTextual()) {
            problems.add(path + ".glossaryId: must be text");
        }
        JsonNode passages = e.get("passages");
        if (passages == null || !passages.isArray() || passages.isEmpty()) {
            problems.add(path + ".passages: required, a non-empty array of passage ordinals");
        } else {
            for (int i = 0; i < passages.size(); i++) {
                JsonNode ordinal = passages.get(i);
                if (!ordinal.isInt() || ordinal.asInt() < 0 || ordinal.asInt() >= passageCount) {
                    problems.add(path + ".passages[" + i + "]: not the ordinal of a passage of the window");
                }
            }
        }
        properties(type, e.get("properties"), path + ".properties", problems);
    }

    private static void properties(NodeType type, JsonNode properties, String path, List<String> problems) {
        if (type == null) {
            return;
        }
        if (properties != null && !properties.isObject()) {
            problems.add(path + ": must be an object");
            return;
        }
        if (type.name().equals("Person") && properties != null && !properties.isEmpty()) {
            problems.add(path + ": a Person takes no properties but its pseudonym (no name, no contact)");
            return;
        }
        if (properties != null) {
            properties.fields().forEachRemaining(entry -> {
                Property declared = type.properties().get(entry.getKey());
                if (declared == null || entry.getKey().equals(type.key())) {
                    problems.add(path + ": a property that " + (declared == null ? "is not one of " + type.name()
                            : "the model does not set (the key is the mention)"));
                    return;
                }
                value(declared, entry.getValue(), path + "." + entry.getKey(), problems);
            });
        }
        for (Property declared : type.properties().values()) {
            if (declared.required() && !declared.name().equals(type.key())
                    && (properties == null || !properties.hasNonNull(declared.name()))) {
                problems.add(path + "." + declared.name() + ": required for " + type.name());
            }
        }
    }

    private static void value(Property property, JsonNode value, String path, List<String> problems) {
        if (!value.isTextual() || value.asText().isBlank() || value.asText().length() > MAX_VALUE) {
            problems.add(path + ": must be non-blank text of at most " + MAX_VALUE + " characters");
            return;
        }
        String text = value.asText();
        switch (property.type()) {
            case ENUM -> {
                if (!property.values().contains(text)) {
                    problems.add(path + ": not one of " + new java.util.TreeSet<>(property.values()));
                }
            }
            case DATE -> {
                try {
                    LocalDate.parse(text);
                } catch (DateTimeParseException e) {
                    problems.add(path + ": must be an ISO date (yyyy-mm-dd)");
                }
            }
            default -> {
                if (property.pattern() != null && !Pattern.matches(property.pattern(), text)) {
                    problems.add(path + ": does not match the form of " + property.name());
                }
            }
        }
    }

    private static void relation(KnowledgeSchema schema, JsonNode r, String path, Map<String, String> typeOfRef,
            List<String> problems) {
        if (!r.isObject()) {
            problems.add(path + ": must be an object");
            return;
        }
        r.fieldNames().forEachRemaining(name -> {
            if (!RELATION_KEYS.contains(name)) {
                problems.add(path + ": a field that is not part of a relation");
            }
        });
        String typeName = text(r, "type");
        RelationType type = typeName == null ? null : schema.relations().get(typeName);
        if (type == null) {
            problems.add(path + ".type: not a relation of " + schema.fullVersion());
            return;
        }
        String from = end(r, "from", path, typeOfRef, problems);
        String to = end(r, "to", path, typeOfRef, problems);
        if (from != null && !type.from().contains(from)) {
            problems.add(path + ".from: a " + type.name() + " does not start at that type of node");
        }
        if (to != null && !type.to().contains(to)) {
            problems.add(path + ".to: a " + type.name() + " does not end at that type of node");
        }
        if (type.sameType() && from != null && to != null && !from.equals(to)) {
            problems.add(path + ": a " + type.name() + " joins two nodes of the same type");
        }
    }

    /** The node type a relation end refers to, or {@code null} after recording why not. */
    private static String end(JsonNode r, String field, String path, Map<String, String> typeOfRef, List<String> problems) {
        String ref = text(r, field);
        if (ref == null || !typeOfRef.containsKey(ref)) {
            problems.add(path + "." + field + ": must be the ref of an entity of the answer (or '" + MEETING_REF + "')");
            return null;
        }
        return typeOfRef.get(ref);
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value != null && value.isTextual() ? value.asText() : null;
    }
}
