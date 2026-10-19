package io.stintflow.knowledge.extraction;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.knowledge.Entity;
import io.stintflow.knowledge.EntityIds;
import io.stintflow.knowledge.KnowledgeBatch;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.Link;
import io.stintflow.knowledge.Passage;
import io.stintflow.knowledge.PassageIds;
import io.stintflow.knowledge.Relation;
import io.stintflow.knowledge.Source;
import io.stintflow.knowledge.schema.KnowledgeSchema;
import io.stintflow.knowledge.schema.SchemaValidator;
import io.stintflow.spi.Sensitivity;

/**
 * Builds the {@link KnowledgeBatch} of a meeting out of the answers (already validated) of each window (SDD 3b.2, 4.3): the
 * entities resolved to ids, the relations between them, the links from the passages that mention them, and the meeting
 * itself. The batch is the complete state of the document, with deterministic ids, so writing it again changes nothing.
 * <p>
 * An item's level is the higher of its type's default in the schema and the level of the instance (SDD 3b.2, 4.1); a
 * relation's, the higher of its ends'. A candidate duplicate is reported beside the batch — it is not in it.
 */
public final class BatchAssembler {

    /** An entity that may be the same as another (SDD 3b.2, 4.5). */
    public record DuplicateCandidate(String entityId, String otherId, double score, List<String> reasons,
            String criterionVersion) {
    }

    /** The result: the batch, the candidates, and how the mentions were resolved ({@code GLOSSARY.n}, …). */
    public record Assembly(KnowledgeBatch batch, List<DuplicateCandidate> duplicateCandidates,
            Map<String, Integer> resolutions) {
    }

    private final KnowledgeScope scope;
    private final KnowledgeSchema schema;
    private final String documentId;
    private final String meetingId;
    private final Sensitivity instanceLevel;
    private final EntityResolver resolver;
    private final List<Passage> passages = new ArrayList<>();
    private final Map<String, Entity> entities = new LinkedHashMap<>();
    private final Map<String, Set<String>> mentionedIn = new LinkedHashMap<>();
    private final Map<String, Relation> relations = new LinkedHashMap<>();
    private final List<DuplicateCandidate> candidates = new ArrayList<>();
    private final Map<String, Integer> resolutions = new TreeMap<>();
    private final String meetingEntityId;

    /**
     * @param meetingId     the id of the meeting (the key of the {@code Meeting} node and, as {@code meeting:<id>}, of the document)
     * @param heldOn        its date (ISO), or {@code null}
     * @param instanceLevel the level of the instance that extracts (absent reads as restricted)
     */
    public BatchAssembler(KnowledgeScope scope, KnowledgeSchema schema, String meetingId, String heldOn,
            Sensitivity instanceLevel, EntityResolver resolver) {
        this.scope = scope;
        this.schema = schema;
        this.meetingId = meetingId;
        this.documentId = "meeting:" + meetingId;
        this.instanceLevel = instanceLevel == null ? Sensitivity.RESTRICTED : instanceLevel;
        this.resolver = resolver;
        this.meetingEntityId = EntityIds.of("Meeting", meetingId);
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("meetingId", meetingId);
        if (heldOn != null) {
            properties.put("heldOn", heldOn);
        }
        entities.put(meetingEntityId, new Entity(meetingEntityId, "Meeting", meetingId, properties, levelOf("Meeting")));
    }

    public String documentId() {
        return documentId;
    }

    /** Adds the passages of one window (in order) and the validated answer about them. {@code passages[]} are window-local. */
    public void addWindow(List<Passage> window, JsonNode answer) {
        int firstGlobal = passages.size();
        for (Passage passage : window) {
            passages.add(passage);
        }
        Map<String, String> idOfRef = new LinkedHashMap<>();
        idOfRef.put(SchemaValidator.MEETING_REF, meetingEntityId);
        for (JsonNode e : answer.path("entities")) {
            String type = e.get("type").asText();
            String mention = e.get("mention").asText();
            EntityResolver.Resolution resolution = resolver.resolve(type, mention,
                    e.hasNonNull("glossaryId") ? e.get("glossaryId").asText() : null);
            resolutions.merge(resolution.kind().name(), 1, Integer::sum);
            String id = resolution.entityId();
            idOfRef.put(e.get("ref").asText(), id);
            Map<String, String> properties = new LinkedHashMap<>();
            KnowledgeSchema.NodeType nodeType = schema.nodes().get(type);
            properties.put(nodeType.key(), resolution.label());
            e.path("properties").properties().forEach(p -> properties.put(p.getKey(), p.getValue().asText()));
            Entity previous = entities.get(id);
            if (previous == null) {
                entities.put(id, new Entity(id, type, resolution.label(), properties, levelOf(type)));
                resolution.candidates().forEach(c -> candidates.add(new DuplicateCandidate(id, c.other(), c.how().score(),
                        c.how().reasons(), DuplicateCriterion.VERSION)));
            }
            Set<String> where = mentionedIn.computeIfAbsent(id, k -> new LinkedHashSet<>());
            for (JsonNode ordinal : e.path("passages")) {
                where.add(passages.get(firstGlobal + ordinal.asInt()).passageId());
            }
        }
        for (JsonNode r : answer.path("relations")) {
            String from = idOfRef.get(r.get("from").asText());
            String to = idOfRef.get(r.get("to").asText());
            Sensitivity level = Sensitivity.max(entities.get(from).sensitivity(), entities.get(to).sensitivity());
            Relation relation = new Relation(r.get("type").asText(), from, to, level);
            relations.putIfAbsent(relation.key(), relation);
        }
    }

    private Sensitivity levelOf(String type) {
        return Sensitivity.max(schema.nodes().get(type).defaultSensitivity(), instanceLevel);
    }

    /** The batch of the whole document: all the windows added so far. */
    public Assembly build(String batchId, Source source) {
        List<Link> links = new ArrayList<>();
        mentionedIn.forEach((entityId, passageIds) -> passageIds.forEach(p -> links.add(new Link(p, entityId))));
        StringBuilder content = new StringBuilder();
        passages.forEach(p -> content.append(p.text()).append('\0'));
        KnowledgeBatch batch = new KnowledgeBatch(batchId, documentId, PassageIds.sha256Hex(content.toString()), null, source,
                schema.fullVersion(), passages, new ArrayList<>(entities.values()), new ArrayList<>(relations.values()), links);
        return new Assembly(batch, List.copyOf(candidates), Map.copyOf(resolutions));
    }
}
