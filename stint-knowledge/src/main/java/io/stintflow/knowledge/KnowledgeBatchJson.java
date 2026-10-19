package io.stintflow.knowledge;

import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.WorkflowRef;

/**
 * A {@link KnowledgeBatch} as JSON and back (SDD 3b.2, 4.3): what an extraction task answers and the step that writes it reads.
 * Only what the batch holds; nothing is added or dropped, so a batch survives the trip equal to itself.
 */
public final class KnowledgeBatchJson {

    private static final ObjectMapper JSON = new ObjectMapper();

    private KnowledgeBatchJson() {
    }

    public static ObjectNode toJson(KnowledgeBatch batch) {
        ObjectNode out = JSON.createObjectNode();
        out.put("batchId", batch.batchId());
        out.put("documentId", batch.documentId());
        out.put("contentHash", batch.contentHash());
        if (batch.revision() != null) {
            out.put("revision", batch.revision());
        }
        out.put("schemaVersion", batch.schemaVersion());
        ObjectNode source = out.putObject("source");
        if (batch.source().instanceId() != null) {
            source.put("instanceId", batch.source().instanceId());
        }
        if (batch.source().chainId() != null) {
            source.put("chainId", batch.source().chainId());
        }
        if (batch.source().definition() != null) {
            source.put("definition", batch.source().definition().canonical());
        }
        ArrayNode passages = out.putArray("passages");
        for (Passage p : batch.passages()) {
            passages.addObject().put("passageId", p.passageId()).put("ordinal", p.ordinal()).put("text", p.text())
                    .put("language", p.language()).put("sensitivity", p.sensitivity().wireName());
        }
        ArrayNode entities = out.putArray("entities");
        for (Entity e : batch.entities()) {
            ObjectNode node = entities.addObject();
            node.put("entityId", e.entityId()).put("type", e.type()).put("label", e.label())
                    .put("sensitivity", e.sensitivity().wireName());
            ObjectNode properties = node.putObject("properties");
            e.properties().forEach(properties::put);
        }
        ArrayNode relations = out.putArray("relations");
        for (Relation r : batch.relations()) {
            relations.addObject().put("type", r.type()).put("fromId", r.fromId()).put("toId", r.toId())
                    .put("sensitivity", r.sensitivity().wireName());
        }
        ArrayNode links = out.putArray("links");
        for (Link l : batch.links()) {
            links.addObject().put("passageId", l.passageId()).put("entityId", l.entityId());
        }
        return out;
    }

    public static KnowledgeBatch fromJson(JsonNode json) {
        JsonNode source = json.path("source");
        java.util.List<Passage> passages = new java.util.ArrayList<>();
        json.path("passages").forEach(p -> passages.add(new Passage(p.path("passageId").asText(),
                p.path("ordinal").asInt(), p.path("text").asText(), p.path("language").asText(),
                Sensitivity.parseOrRestricted(p.path("sensitivity").asText(null)))));
        java.util.List<Entity> entities = new java.util.ArrayList<>();
        json.path("entities").forEach(e -> {
            Map<String, String> properties = new LinkedHashMap<>();
            e.path("properties").properties().forEach(entry -> properties.put(entry.getKey(), entry.getValue().asText()));
            entities.add(new Entity(e.path("entityId").asText(), e.path("type").asText(), e.path("label").asText(),
                    properties, Sensitivity.parseOrRestricted(e.path("sensitivity").asText(null))));
        });
        java.util.List<Relation> relations = new java.util.ArrayList<>();
        json.path("relations").forEach(r -> relations.add(new Relation(r.path("type").asText(),
                r.path("fromId").asText(), r.path("toId").asText(),
                Sensitivity.parseOrRestricted(r.path("sensitivity").asText(null)))));
        java.util.List<Link> links = new java.util.ArrayList<>();
        json.path("links").forEach(l -> links.add(new Link(l.path("passageId").asText(), l.path("entityId").asText())));
        return new KnowledgeBatch(json.path("batchId").asText(), json.path("documentId").asText(),
                json.path("contentHash").asText(), json.hasNonNull("revision") ? json.get("revision").asLong() : null,
                new Source(source.hasNonNull("instanceId") ? source.get("instanceId").asText() : null,
                        source.hasNonNull("chainId") ? source.get("chainId").asText() : null,
                        source.hasNonNull("definition") ? WorkflowRef.parse(source.get("definition").asText()) : null),
                json.path("schemaVersion").asText(), passages, entities, relations, links);
    }
}
