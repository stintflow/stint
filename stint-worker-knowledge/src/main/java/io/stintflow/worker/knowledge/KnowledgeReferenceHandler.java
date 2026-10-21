package io.stintflow.worker.knowledge;

import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.knowledge.Entity;
import io.stintflow.knowledge.EntityIds;
import io.stintflow.knowledge.GraphPointer;
import io.stintflow.knowledge.KnowledgeBatch;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.KnowledgeStore;
import io.stintflow.knowledge.KnowledgeValidationException;
import io.stintflow.knowledge.PassageIds;
import io.stintflow.knowledge.Relation;
import io.stintflow.knowledge.Source;
import io.stintflow.knowledge.WriteReceipt;
import io.stintflow.spi.AuditLog;
import io.stintflow.spi.Sensitivity;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;

/**
 * The task {@code knowledge-reference} (SDD 3c.1, 4.4): what another domain announced, as nodes of this one — a
 * {@code DomainReference} per announced node, holding the pointer and nothing the origin holds, and a {@code informs} relation from
 * each local <em>anchor</em> the definition names to each of them.
 * <p>
 * <b>Input.</b> {@code {"event": {"domain": {"namespace", "base"}, "documentId", "upTo", "items": [{"pointer", "type", "level"}]},
 * "scope": {"namespace", "base"}, "anchors"?: [{"type", "name"}]}}. The domain of the event has to be one this worker listens to
 * ({@link OriginAllowList}; none by default); every pointer has to be well formed ({@link GraphPointer}) and of that domain, or the
 * whole announcement is refused and nothing is written. The label of a reference is its type, its domain and the start of a hash
 * of the node id — never the name the origin gives the node.
 * <p>
 * <b>The batch</b> is one per document of the origin ({@code pointers:<domain>:<documentId>}, revision {@code upTo}): the same
 * announcement again changes nothing, a later one replaces it, an earlier one is {@code STALE}. Erasing that document takes the
 * references it alone held (SDD 3b.5).
 */
public final class KnowledgeReferenceHandler implements TaskHandler {

    public static final String AUDIT_KIND = "knowledge-reference";
    public static final String SCHEMA_VERSION = "architecture@1.0.0";
    public static final String REFERENCE_TYPE = "DomainReference";
    public static final String RELATION = "informs";

    static final int MAX_ITEMS = 200;
    static final int MAX_ANCHORS = 20;
    private static final Pattern TYPE = Pattern.compile("[A-Za-z][A-Za-z0-9]{0,40}");
    private static final Logger LOG = LoggerFactory.getLogger(KnowledgeReferenceHandler.class);

    private final KnowledgeStore store;
    private final ScopeAllowList scopes;
    private final OriginAllowList origins;
    private final AuditLog audit;
    private final InstantSource clock;

    public KnowledgeReferenceHandler(KnowledgeStore store, ScopeAllowList scopes, OriginAllowList origins, AuditLog audit,
            InstantSource clock) {
        this.store = Objects.requireNonNull(store, "store");
        this.scopes = Objects.requireNonNull(scopes, "scopes");
        this.origins = Objects.requireNonNull(origins, "origins");
        this.audit = audit == null ? AuditLog.NONE : audit;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext ctx) {
        try {
            return CompletableFuture.completedFuture(reference(ctx));
        } catch (TaskFailure failure) {
            return CompletableFuture.failedFuture(failure);
        } catch (RuntimeException e) {
            LOG.warn("References of task {} in instance {} failed ({})", ctx.taskId(), ctx.instanceId(), e.getClass().getName());
            return CompletableFuture.failedFuture(Failures.unavailable("The references could not be written ("
                    + e.getClass().getSimpleName() + ")"));
        }
    }

    private JsonNode reference(TaskContext ctx) {
        JsonNode in = ctx.input();
        KnowledgeScope scope = scopes.require(in);
        JsonNode event = in.path("event");
        KnowledgeScope domain = domainOf(event.path("domain"));
        if (!origins.allows(domain)) {
            record(ctx, scope, domain, "refused", Map.of());
            throw Failures.refused("This worker does not accept announcements from that domain");
        }
        String documentId = event.path("documentId").asText("");
        if (documentId.isBlank() || documentId.length() > 200) {
            throw Failures.validation("event.documentId is required");
        }
        long upTo = event.path("upTo").asLong(0);
        if (upTo < 1) {
            throw Failures.validation("event.upTo is required");
        }
        JsonNode announced = event.path("items");
        if (!announced.isArray() || announced.isEmpty() || announced.size() > MAX_ITEMS) {
            throw Failures.validation("event.items is one to " + MAX_ITEMS + " pointers");
        }

        // a node announced twice keeps its highest version
        Map<String, Reference> byNode = new TreeMap<>();
        for (int i = 0; i < announced.size(); i++) {
            JsonNode item = announced.get(i);
            GraphPointer pointer;
            try {
                pointer = GraphPointer.parse(item.path("pointer").asText(null));
            } catch (KnowledgeValidationException e) {
                throw Failures.validation("items[" + i + "]: " + e.getMessage());
            }
            if (!pointer.scope().equals(domain)) {
                throw Failures.validation("items[" + i + "]: the pointer is not of the domain of the event");
            }
            String type = item.path("type").asText("");
            if (!TYPE.matcher(type).matches()) {
                throw Failures.validation("items[" + i + "]: type is a node type");
            }
            Sensitivity level = Sensitivity.parse(item.path("level").asText("")).orElseThrow(() ->
                    Failures.validation("level is internal, confidential or restricted"));
            if (level == Sensitivity.RESTRICTED) {
                throw Failures.validation("items[" + i + "]: a restricted node is never announced");
            }
            byNode.merge(pointer.nodeKey(), new Reference(pointer, type, level), (a, b) -> a.pointer.version() >= b.pointer.version() ? a : b);
        }
        List<Entity> anchors = anchors(in.path("anchors"));
        Map<String, Integer> found = PiiGate.findings(new KnowledgeBatch("gate", "gate", "gate", null, null, SCHEMA_VERSION,
                List.of(), anchors, List.of(), List.of()));
        if (!found.isEmpty()) {
            LOG.warn("Task {} of instance {}: an anchor still holds personal data {}; refused", ctx.taskId(), ctx.instanceId(),
                    found);
            throw Failures.validation("An anchor still holds personal data (counts by type: " + found + "); nothing was written");
        }

        List<Entity> entities = new ArrayList<>(anchors);
        List<Relation> relations = new ArrayList<>();
        for (Reference ref : byNode.values()) {
            String id = EntityIds.of(REFERENCE_TYPE, ref.pointer.nodeKey());
            Map<String, String> properties = new LinkedHashMap<>();
            properties.put("pointer", ref.pointer.format());
            properties.put("remoteType", ref.type);
            properties.put("domain", domain.key());
            properties.put("observedVersion", Long.toString(ref.pointer.version()));
            entities.add(new Entity(id, REFERENCE_TYPE, ref.type + " @ " + domain.key() + " " + PassageIds.sha256Hex(ref.pointer.nodeId()).substring(0, 8),
                    properties, ref.level));
            for (Entity anchor : anchors) {
                relations.add(new Relation(RELATION, anchor.entityId(), id, ref.level));
            }
        }
        String documentKey = "pointers:" + domain.key() + ":" + documentId;
        String hash = PassageIds.sha256Hex(entities.toString() + relations);
        KnowledgeBatch batch = new KnowledgeBatch("reference:" + ctx.instanceId() + ":" + ctx.taskId(), documentKey, hash, upTo,
                new Source(ctx.instanceId(), ctx.chainId(), ctx.definition()), SCHEMA_VERSION, List.of(), entities, relations,
                List.of());
        WriteReceipt receipt;
        try {
            receipt = store.write(scope, batch).toCompletableFuture().join();
        } catch (RuntimeException e) {
            Throwable cause = e instanceof CompletionException && e.getCause() != null ? e.getCause() : e;
            if (cause instanceof KnowledgeValidationException invalid) {
                throw Failures.validation("The base refused the references: " + invalid.getMessage());
            }
            throw Failures.unavailable("The base could not be written (" + cause.getClass().getSimpleName() + ")");
        }
        record(ctx, scope, domain, "answered", Map.of("status", receipt.status().name(), "references",
                Integer.toString(byNode.size()), "anchors", Integer.toString(anchors.size())));
        ObjectNode out = Json.obj();
        out.put("status", receipt.status().name());
        ObjectNode counts = out.putObject("counts");
        receipt.counts().forEach(counts::put);
        out.put("lastSequence", receipt.lastSequence());
        out.put("references", byNode.size()).put("anchors", anchors.size());
        return out;
    }

    private static KnowledgeScope domainOf(JsonNode domain) {
        String namespace = domain.path("namespace").asText("");
        String base = domain.path("base").asText("");
        if (namespace.isBlank() || base.isBlank()) {
            throw Failures.validation("event.domain.namespace and event.domain.base are required");
        }
        return KnowledgeScope.named(namespace, base);
    }

    /** The local nodes the definition names to hold the relation: a type and a name each, internal, at most {@value #MAX_ANCHORS}. */
    private static List<Entity> anchors(JsonNode named) {
        if (!named.isMissingNode() && !named.isNull() && (!named.isArray() || named.size() > MAX_ANCHORS)) {
            throw Failures.validation("anchors is at most " + MAX_ANCHORS + " nodes");
        }
        Map<String, Entity> anchors = new TreeMap<>(Comparator.naturalOrder());
        for (JsonNode anchor : named) {
            String type = anchor.path("type").asText("");
            String name = anchor.path("name").asText("");
            if (!TYPE.matcher(type).matches() || name.isBlank() || name.length() > 200) {
                throw Failures.validation("an anchor is a node type and a name");
            }
            Entity entity = new Entity(EntityIds.of(type, name), type, name, Sensitivity.INTERNAL);
            anchors.putIfAbsent(entity.entityId(), entity);
        }
        return new ArrayList<>(anchors.values());
    }

    private void record(TaskContext ctx, KnowledgeScope scope, KnowledgeScope origin, String outcome, Map<String, String> more) {
        Map<String, String> detail = new LinkedHashMap<>(more);
        detail.put("base", scope.key());
        detail.put("origin", origin.key());
        audit.append(new AuditLog.AuditRecord(ctx.instanceId(), ctx.chainId(), clock.instant(), AUDIT_KIND, ctx.taskId(),
                ctx.sensitivity(), "knowledge-reference", outcome, detail)).toCompletableFuture().join();
    }

    private record Reference(GraphPointer pointer, String type, Sensitivity level) {
    }
}
