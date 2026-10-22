package io.stintflow.worker.knowledge;

import java.time.InstantSource;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.knowledge.CatalogTypes;
import io.stintflow.knowledge.EntityIds;
import io.stintflow.knowledge.EntityMatch;
import io.stintflow.knowledge.EntityQuery;
import io.stintflow.knowledge.GraphPointer;
import io.stintflow.knowledge.GraphPointers;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.KnowledgeStore;
import io.stintflow.spi.AuditLog;
import io.stintflow.spi.Sensitivity;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;

/**
 * The task {@code catalog-lookup} (SDD 3c.3, 4.2), served by the domain that holds the catalog: which node of the catalog a name
 * is. The match is exact — the name or an alias after normalizing, inside the type, at or below {@code internal} — and never by
 * similarity: what does not match is {@code NONE}, with no candidates.
 * <p>
 * <b>Input.</b> {@code {"mentions": [{"type", "name"}], "scope": {"namespace", "base"}}}, one to {@value #MAX_MENTIONS}.
 * <b>Answer.</b> {@code {"results": [{"status": "MATCH", "pointer"} | {"status": "NONE"}], "counts": {"MATCH", "NONE"}}} — in the
 * order of the mentions, with the pointer of the node and nothing the catalog says about it: no canonical name, no alias, no
 * definition.
 * <p>
 * <b>Who asks</b> is the definition of the invitation, listed in {@link CatalogRequesters}. A definition not listed, and a base
 * this worker does not serve, answer {@code NONE} to every mention — the same shape and the same size as the catalog that does not
 * know the names, so the answer tells nothing about what the catalog holds. Only a mention that is not a mention (a type the
 * catalog does not hold, no name, a long one) refuses the batch with {@code validation} 400: that is the asker's mistake.
 */
public final class CatalogLookupHandler implements TaskHandler {

    public static final String AUDIT_KIND = "catalog-lookup";

    static final int MAX_MENTIONS = 50;
    static final int MAX_NAME = 200;
    private static final Logger LOG = LoggerFactory.getLogger(CatalogLookupHandler.class);

    private final KnowledgeStore store;
    private final ScopeAllowList scopes;
    private final CatalogRequesters requesters;
    private final AuditLog audit;
    private final InstantSource clock;

    public CatalogLookupHandler(KnowledgeStore store, ScopeAllowList scopes, CatalogRequesters requesters, AuditLog audit,
            InstantSource clock) {
        this.store = Objects.requireNonNull(store, "store");
        this.scopes = Objects.requireNonNull(scopes, "scopes");
        this.requesters = Objects.requireNonNull(requesters, "requesters");
        this.audit = audit == null ? AuditLog.NONE : audit;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext ctx) {
        try {
            return CompletableFuture.completedFuture(lookup(ctx));
        } catch (TaskFailure failure) {
            return CompletableFuture.failedFuture(failure);
        } catch (RuntimeException e) {
            LOG.warn("Catalog lookup of task {} in instance {} failed ({})", ctx.taskId(), ctx.instanceId(), e.getClass().getName());
            return CompletableFuture.failedFuture(Failures.unavailable("The catalog could not be read ("
                    + e.getClass().getSimpleName() + ")"));
        }
    }

    private JsonNode lookup(TaskContext ctx) {
        JsonNode in = ctx.input();
        JsonNode mentions = in.path("mentions");
        if (!mentions.isArray() || mentions.isEmpty() || mentions.size() > MAX_MENTIONS) {
            throw Failures.validation("mentions is one to " + MAX_MENTIONS + " names");
        }
        for (int i = 0; i < mentions.size(); i++) {
            String type = mentions.get(i).path("type").asText("");
            String name = mentions.get(i).path("name").asText("");
            if (!CatalogTypes.NAMES.contains(type)) {
                throw Failures.validation("mentions[" + i + "]: type is one of " + CatalogTypes.NAMES.stream().sorted().toList());
            }
            if (name.isBlank() || name.length() > MAX_NAME) {
                throw Failures.validation("mentions[" + i + "]: name is one to " + MAX_NAME + " characters");
            }
        }
        KnowledgeScope scope = served(in);
        boolean granted = scope != null && requesters.allows(ctx.definition());

        ObjectNode out = Json.obj();
        ArrayNode results = out.putArray("results");
        int matched = 0;
        if (granted) {
            List<io.stintflow.knowledge.ChangeEntry> log = null;
            for (JsonNode mention : mentions) {
                String type = mention.path("type").asText();
                List<EntityMatch> found = read(scope, type, mention.path("name").asText());
                String pointer = null;
                if (found.size() == 1) {
                    if (log == null) {
                        log = ChangeLogs.all(store, scope);
                    }
                    OptionalLong version = GraphPointers.versionOf(log, found.getFirst().entityId());
                    if (version.isPresent()) {
                        pointer = GraphPointer.of(scope, found.getFirst().entityId(), version.getAsLong()).format();
                    }
                }
                ObjectNode result = results.addObject();
                if (pointer == null) {
                    result.put("status", "NONE");
                } else {
                    result.put("status", "MATCH").put("pointer", pointer);
                    matched++;
                }
            }
        } else {
            mentions.forEach(m -> results.addObject().put("status", "NONE"));
        }
        out.putObject("counts").put("MATCH", matched).put("NONE", mentions.size() - matched);
        record(ctx, scope, mentions.size(), matched);
        return out;
    }

    /** The base the input names, if this worker serves it; {@code null} if not (the same answer as an unknown name). */
    private KnowledgeScope served(JsonNode in) {
        try {
            return scopes.require(in);
        } catch (TaskFailure failure) {
            if (Integer.valueOf(403).equals(failure.toErrorInfo().status())) {
                return null;
            }
            throw failure;
        }
    }

    private List<EntityMatch> read(KnowledgeScope scope, String type, String name) {
        try {
            return store.entities(scope, new EntityQuery(type, EntityIds.normalize(name), Sensitivity.INTERNAL, 2))
                    .toCompletableFuture().join();
        } catch (CompletionException e) {
            throw Failures.unavailable("The catalog could not be read (" + e.getCause().getClass().getSimpleName() + ")");
        }
    }

    private void record(TaskContext ctx, KnowledgeScope scope, int asked, int matched) {
        Map<String, String> detail = new LinkedHashMap<>();
        String namespace = ctx.definition() == null ? "" : ctx.definition().namespace();
        String name = ctx.definition() == null ? "" : ctx.definition().name();
        detail.put("requester", namespace + ":" + name);
        detail.put("base", scope == null ? "" : scope.key());
        detail.put("MATCH", Integer.toString(matched));
        detail.put("NONE", Integer.toString(asked - matched));
        audit.append(new AuditLog.AuditRecord(ctx.instanceId(), ctx.chainId(), clock.instant(), AUDIT_KIND, ctx.taskId(),
                ctx.sensitivity(), "catalog-lookup", "answered", detail)).toCompletableFuture().join();
    }
}
