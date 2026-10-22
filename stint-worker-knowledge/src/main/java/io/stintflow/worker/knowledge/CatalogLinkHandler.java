package io.stintflow.worker.knowledge;

import java.time.InstantSource;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.knowledge.CatalogTypes;
import io.stintflow.knowledge.Entity;
import io.stintflow.knowledge.EntityIds;
import io.stintflow.knowledge.GraphPointer;
import io.stintflow.knowledge.KnowledgeBatch;
import io.stintflow.knowledge.KnowledgeNodes;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.KnowledgeStore;
import io.stintflow.knowledge.KnowledgeValidationException;
import io.stintflow.knowledge.NodeContent;
import io.stintflow.knowledge.NodesQuery;
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
 * The task {@code catalog-link} (SDD 3c.3, 4.4), served by an area: says that a node of the area <em>denotes</em> a node of the
 * catalog, without changing the node and without copying the catalog. For each link it writes a {@code DomainReference} (the
 * pointer, the type, the domain, the observed version — nothing the catalog says of the node) and a {@code denotes} relation from
 * the local node to it. The area knows the catalog only by the pointer: not its store, not its classes.
 * <p>
 * <b>Input.</b> {@code {"links": [{"entityId", "pointer"}], "documentId", "scope": {"namespace", "base"}}}, one to {@value
 * #MAX_LINKS}. Every pointer is well formed ({@link GraphPointer}) and of a domain {@link OriginAllowList} lists, or the whole call
 * is refused and nothing is written. A local node that does not exist, or is not a type the catalog holds, or is above
 * {@code internal}, is skipped and counted: a link never creates a node of the area.
 * <p>
 * <b>The batch</b> is one per document ({@code catalog-links:<documentId>}), revision the highest version among the pointers: the
 * same call again changes nothing, a call after the catalog moved replaces the reference with its new version, an earlier one is
 * {@code STALE}.
 */
public final class CatalogLinkHandler implements TaskHandler {

    public static final String AUDIT_KIND = "catalog-link";
    public static final String SCHEMA_VERSION = "catalog-links@1.0.0";
    public static final String REFERENCE_TYPE = "DomainReference";
    public static final String RELATION = "denotes";

    static final int MAX_LINKS = 50;
    private static final Logger LOG = LoggerFactory.getLogger(CatalogLinkHandler.class);

    private final KnowledgeStore store;
    private final KnowledgeNodes nodes;
    private final ScopeAllowList scopes;
    private final OriginAllowList origins;
    private final AuditLog audit;
    private final InstantSource clock;

    public CatalogLinkHandler(KnowledgeStore store, KnowledgeNodes nodes, ScopeAllowList scopes, OriginAllowList origins,
            AuditLog audit, InstantSource clock) {
        this.store = Objects.requireNonNull(store, "store");
        this.nodes = Objects.requireNonNull(nodes, "nodes");
        this.scopes = Objects.requireNonNull(scopes, "scopes");
        this.origins = Objects.requireNonNull(origins, "origins");
        this.audit = audit == null ? AuditLog.NONE : audit;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext ctx) {
        try {
            return CompletableFuture.completedFuture(link(ctx));
        } catch (TaskFailure failure) {
            return CompletableFuture.failedFuture(failure);
        } catch (RuntimeException e) {
            LOG.warn("Catalog link of task {} in instance {} failed ({})", ctx.taskId(), ctx.instanceId(), e.getClass().getName());
            return CompletableFuture.failedFuture(Failures.unavailable("The links could not be written ("
                    + e.getClass().getSimpleName() + ")"));
        }
    }

    private JsonNode link(TaskContext ctx) {
        JsonNode in = ctx.input();
        KnowledgeScope scope = scopes.require(in);
        String documentId = in.path("documentId").asText("");
        if (documentId.isBlank() || documentId.length() > 200) {
            throw Failures.validation("documentId is required");
        }
        JsonNode links = in.path("links");
        if (!links.isArray() || links.isEmpty() || links.size() > MAX_LINKS) {
            throw Failures.validation("links is one to " + MAX_LINKS + " links");
        }
        // an entity linked twice keeps its highest version
        Map<String, GraphPointer> byEntity = new TreeMap<>();
        for (int i = 0; i < links.size(); i++) {
            String entityId = links.get(i).path("entityId").asText("");
            if (entityId.isBlank() || entityId.length() > 200) {
                throw Failures.validation("links[" + i + "]: entityId is required");
            }
            GraphPointer pointer;
            try {
                pointer = GraphPointer.parse(links.get(i).path("pointer").asText(null));
            } catch (KnowledgeValidationException e) {
                throw Failures.validation("links[" + i + "]: " + e.getMessage());
            }
            if (!origins.allows(pointer.scope())) {
                record(ctx, scope, "refused", Map.of());
                throw Failures.refused("This worker does not link to the catalog of that domain");
            }
            byEntity.merge(entityId, pointer, (a, b) -> a.version() >= b.version() ? a : b);
        }

        Map<String, NodeContent> local = new LinkedHashMap<>();
        try {
            nodes.nodes(scope, new NodesQuery(new HashSet<>(byEntity.keySet()), Sensitivity.INTERNAL, 0)).toCompletableFuture()
                    .join().forEach(n -> local.put(n.id(), n));
        } catch (CompletionException e) {
            throw Failures.unavailable("The base could not be read (" + e.getCause().getClass().getSimpleName() + ")");
        }

        List<Entity> entities = new ArrayList<>();
        List<Relation> relations = new ArrayList<>();
        Set<String> written = new HashSet<>();
        long revision = 1;
        int skipped = 0;
        for (Map.Entry<String, GraphPointer> link : byEntity.entrySet()) {
            NodeContent node = local.get(link.getKey());
            if (node == null || !CatalogTypes.NAMES.contains(node.type())) {
                skipped++;
                continue;
            }
            GraphPointer pointer = link.getValue();
            String id = EntityIds.of(REFERENCE_TYPE, pointer.nodeKey());
            if (written.add(id)) {
                Map<String, String> properties = new LinkedHashMap<>();
                properties.put("pointer", pointer.format());
                properties.put("remoteType", node.type());
                properties.put("domain", pointer.scope().key());
                properties.put("observedVersion", Long.toString(pointer.version()));
                entities.add(new Entity(id, REFERENCE_TYPE, node.type() + " @ " + pointer.scope().key() + " "
                        + PassageIds.sha256Hex(pointer.nodeId()).substring(0, 8), properties, Sensitivity.INTERNAL));
            }
            relations.add(new Relation(RELATION, node.id(), id, Sensitivity.INTERNAL));
            revision = Math.max(revision, pointer.version());
        }
        if (entities.isEmpty()) {
            record(ctx, scope, "answered", Map.of("status", "NOTHING", "linked", "0", "skipped", Integer.toString(skipped)));
            ObjectNode none = Json.obj();
            none.put("status", "NOTHING").put("linked", 0).put("skipped", skipped);
            return none;
        }
        String hash = PassageIds.sha256Hex(entities.toString() + relations);
        KnowledgeBatch batch = new KnowledgeBatch("catalog-link:" + ctx.instanceId() + ":" + ctx.taskId(),
                "catalog-links:" + documentId, hash, revision, new Source(ctx.instanceId(), ctx.chainId(), ctx.definition()),
                SCHEMA_VERSION, List.of(), entities, relations, List.of());
        WriteReceipt receipt;
        try {
            receipt = store.write(scope, batch).toCompletableFuture().join();
        } catch (RuntimeException e) {
            Throwable cause = e instanceof CompletionException && e.getCause() != null ? e.getCause() : e;
            if (cause instanceof KnowledgeValidationException invalid) {
                throw Failures.validation("The base refused the links: " + invalid.getMessage());
            }
            throw Failures.unavailable("The base could not be written (" + cause.getClass().getSimpleName() + ")");
        }
        record(ctx, scope, "answered", Map.of("status", receipt.status().name(), "linked", Integer.toString(relations.size()),
                "skipped", Integer.toString(skipped)));
        ObjectNode out = Json.obj();
        out.put("status", receipt.status().name()).put("linked", relations.size()).put("skipped", skipped);
        out.put("lastSequence", receipt.lastSequence());
        return out;
    }

    private void record(TaskContext ctx, KnowledgeScope scope, String outcome, Map<String, String> more) {
        Map<String, String> detail = new LinkedHashMap<>(more);
        detail.put("base", scope.key());
        audit.append(new AuditLog.AuditRecord(ctx.instanceId(), ctx.chainId(), clock.instant(), AUDIT_KIND, ctx.taskId(),
                ctx.sensitivity(), "catalog-link", outcome, detail)).toCompletableFuture().join();
    }
}
