package io.stintflow.knowledge.schema;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.stintflow.knowledge.KnowledgeValidationException;
import io.stintflow.knowledge.PassageIds;
import io.stintflow.spi.Sensitivity;

/**
 * The versioned schema of a knowledge graph (SDD 3b.2, 4.1): the node types, their properties, and the relations between
 * them. A type, property or relation the schema does not list does not exist. A schema is immutable per {@code (id, version)}:
 * the registry refuses another content under the same version.
 *
 * @param id       e.g. {@code meetings}
 * @param version  semver, e.g. {@code 1.0.0}; a minor only adds, a major may remove or rename
 * @param language the language of the labels and the glossary (the content stays in its own)
 */
public record KnowledgeSchema(String id, String version, String language, Map<String, NodeType> nodes,
        Map<String, RelationType> relations, String contentHash) {

    private static final Pattern SEMVER = Pattern.compile("\\d+\\.\\d+\\.\\d+");
    private static final ObjectMapper JSON = new ObjectMapper();

    public KnowledgeSchema {
        Objects.requireNonNull(id, "id");
        if (!SEMVER.matcher(version).matches()) {
            throw new KnowledgeValidationException("A schema version is semver (1.0.0)");
        }
        nodes = Map.copyOf(nodes);
        relations = Map.copyOf(relations);
        for (RelationType relation : relations.values()) {
            for (String type : concat(relation.from(), relation.to())) {
                if (!nodes.containsKey(type)) {
                    throw new KnowledgeValidationException("Relation " + relation.name() + " names a node type the schema "
                            + "does not have: " + type);
                }
            }
        }
    }

    /** The type of a node: its key property, the level it has by default, and its properties. */
    public record NodeType(String name, String key, Sensitivity defaultSensitivity, boolean glossary,
            Map<String, Property> properties) {
        public NodeType {
            properties = Map.copyOf(properties);
            if (!properties.containsKey(key)) {
                throw new KnowledgeValidationException("Node type " + name + " has no property " + key + ", its key");
            }
        }
    }

    public enum PropertyType { STRING, DATE, ENUM }

    /**
     * @param sensitive free text: where the item is restricted, it rests in the blob (SDD 3b.1, E1)
     * @param pattern   a regular expression the whole value matches, or {@code null}
     * @param values    for an enum
     */
    public record Property(String name, PropertyType type, boolean required, boolean sensitive, String pattern,
            Set<String> values) {
        public Property {
            values = values == null ? Set.of() : Set.copyOf(values);
        }
    }

    /** {@code sameType}: both ends are of one node type (a decision supersedes a decision). */
    public record RelationType(String name, Set<String> from, Set<String> to, boolean sameType) {
        public RelationType {
            from = Set.copyOf(from);
            to = Set.copyOf(to);
        }
    }

    /** {@code <id>@<version>}: what a batch carries as its {@code schemaVersion} and the change log records. */
    public String fullVersion() {
        return id + "@" + version;
    }

    public int major() {
        return Integer.parseInt(version.substring(0, version.indexOf('.')));
    }

    // --- loading ---------------------------------------------------------------------------------------------------

    /** The reference schema for meetings, {@code meetings@1.0.0}, shipped with the module. */
    public static KnowledgeSchema meetings() {
        try (InputStream in = KnowledgeSchema.class.getResourceAsStream("meetings-1.0.0.json")) {
            return fromJson(JSON.readTree(Objects.requireNonNull(in, "meetings-1.0.0.json")));
        } catch (IOException e) {
            throw new IllegalStateException("The meetings schema could not be read", e);
        }
    }

    /** The reference schema of the architecture domain, {@code architecture@1.0.0} (SDD 3c.1): domain references and systems. */
    public static KnowledgeSchema architecture() {
        try (InputStream in = KnowledgeSchema.class.getResourceAsStream("architecture-1.0.0.json")) {
            return fromJson(JSON.readTree(Objects.requireNonNull(in, "architecture-1.0.0.json")));
        } catch (IOException e) {
            throw new IllegalStateException("The architecture schema could not be read", e);
        }
    }

    /** The reference schema of the entity catalog, {@code catalog@1.0.0} (SDD 3c.3): systems, products and teams, never people. */
    public static KnowledgeSchema catalog() {
        return resource("catalog-1.0.0.json");
    }

    /** What an area writes to say a local node denotes a catalog node, {@code catalog-links@1.0.0} (SDD 3c.3): a reference and {@code denotes}. */
    public static KnowledgeSchema catalogLinks() {
        return resource("catalog-links-1.0.0.json");
    }

    private static KnowledgeSchema resource(String name) {
        try (InputStream in = KnowledgeSchema.class.getResourceAsStream(name)) {
            return fromJson(JSON.readTree(Objects.requireNonNull(in, name)));
        } catch (IOException e) {
            throw new IllegalStateException("The schema " + name + " could not be read", e);
        }
    }

    public static KnowledgeSchema fromJson(JsonNode json) {
        Map<String, NodeType> nodes = new LinkedHashMap<>();
        json.path("nodes").properties().forEach(entry -> {
            JsonNode node = entry.getValue();
            Map<String, Property> properties = new LinkedHashMap<>();
            node.path("properties").properties().forEach(p -> properties.put(p.getKey(), property(p.getKey(), p.getValue())));
            nodes.put(entry.getKey(), new NodeType(entry.getKey(), node.path("key").asText(),
                    Sensitivity.parse(node.path("defaultSensitivity").asText()).orElse(Sensitivity.RESTRICTED),
                    node.path("glossary").asBoolean(false), properties));
        });
        Map<String, RelationType> relations = new LinkedHashMap<>();
        json.path("relations").properties().forEach(entry -> relations.put(entry.getKey(), new RelationType(entry.getKey(),
                strings(entry.getValue().path("from")), strings(entry.getValue().path("to")),
                entry.getValue().path("sameType").asBoolean(false))));
        return new KnowledgeSchema(json.path("schema").asText(), json.path("version").asText(),
                json.path("language").asText("en"), nodes, relations, PassageIds.sha256Hex(json.toString()));
    }

    private static Property property(String name, JsonNode node) {
        PropertyType type = switch (node.path("type").asText("string")) {
            case "date" -> PropertyType.DATE;
            case "enum" -> PropertyType.ENUM;
            default -> PropertyType.STRING;
        };
        return new Property(name, type, node.path("required").asBoolean(false), node.path("sensitive").asBoolean(false),
                node.hasNonNull("pattern") ? node.get("pattern").asText() : null, strings(node.path("values")));
    }

    private static Set<String> strings(JsonNode array) {
        Set<String> out = new LinkedHashSet<>();
        array.forEach(item -> out.add(item.asText()));
        return out;
    }

    private static List<String> concat(Set<String> a, Set<String> b) {
        List<String> all = new ArrayList<>(a);
        all.addAll(b);
        return all;
    }
}
