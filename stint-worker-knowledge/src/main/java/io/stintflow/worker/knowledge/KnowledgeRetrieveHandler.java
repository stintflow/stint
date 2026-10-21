package io.stintflow.worker.knowledge;

import java.time.InstantSource;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.knowledge.Audience;
import io.stintflow.knowledge.ContextItem;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.KnowledgeStore;
import io.stintflow.knowledge.KnowledgeValidationException;
import io.stintflow.knowledge.RetrievalQuery;
import io.stintflow.spi.AuditLog;
import io.stintflow.spi.Sensitivity;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;

/**
 * The task {@code knowledge-retrieve} (SDD 3b.3, 4.6): the context closest to a question, at or below a ceiling.
 * <p>
 * <b>Input.</b> {@code {"query", "topK"?: 5, "hops"?: 0, "minScore"?, "maxLevel"?, "scope": {"namespace", "base"}}}.
 * <b>Ceiling.</b> The level of the instance asking — absent reads as restricted — lowered, never raised, by {@code maxLevel}.
 * Nothing above it comes back, and nothing that tells it is there (SDD 3b.1, 4.4). <b>Answer.</b> {@code {"items": [{"content",
 * "kind", "score", "level", "entities", "origin"}], "counts": {"internal", "confidential", "restricted"}}}.
 * <p>
 * <b>Audit.</b> Each read adds a record of kind {@value #AUDIT_KIND} with the base, the ceiling, {@code topK} and the counts by
 * level — never the question and never a content. With no audit log, nothing is recorded.
 */
public final class KnowledgeRetrieveHandler implements TaskHandler {

    /** The kind of the audit record of a read of the base. */
    public static final String AUDIT_KIND = "knowledge-read";

    private static final Logger LOG = LoggerFactory.getLogger(KnowledgeRetrieveHandler.class);

    private final KnowledgeStore store;
    private final ScopeAllowList scopes;
    private final AuditLog audit;
    private final InstantSource clock;

    public KnowledgeRetrieveHandler(KnowledgeStore store, ScopeAllowList scopes, AuditLog audit, InstantSource clock) {
        this.store = Objects.requireNonNull(store, "store");
        this.scopes = Objects.requireNonNull(scopes, "scopes");
        this.audit = audit == null ? AuditLog.NONE : audit;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext ctx) {
        try {
            return CompletableFuture.completedFuture(retrieve(ctx));
        } catch (TaskFailure failure) {
            return CompletableFuture.failedFuture(failure);
        } catch (RuntimeException e) {
            LOG.warn("Knowledge retrieval of task {} in instance {} failed ({})", ctx.taskId(), ctx.instanceId(),
                    e.getClass().getName());
            return CompletableFuture.failedFuture(Failures.unavailable("The knowledge retrieval failed ("
                    + e.getClass().getSimpleName() + ")"));
        }
    }

    private JsonNode retrieve(TaskContext ctx) {
        JsonNode in = ctx.input();
        KnowledgeScope scope = scopes.require(in);
        String question = in.path("query").asText("");
        if (question.isBlank()) {
            throw Failures.validation("query is required");
        }
        Sensitivity ceiling = Audience.ceiling(ctx.sensitivity(), null, false);   // the level of the instance itself
        if (in.hasNonNull("maxLevel")) {
            Sensitivity asked = Sensitivity.parse(in.get("maxLevel").asText()).orElseThrow(() ->
                    Failures.validation("maxLevel is internal, confidential or restricted"));
            ceiling = asked.compareTo(ceiling) < 0 ? asked : ceiling;   // it only lowers
        }
        RetrievalQuery query;
        try {
            query = new RetrievalQuery(question, in.path("topK").asInt(5), in.path("minScore").asDouble(0), ceiling, null,
                    null, null, in.path("hops").asInt(0));
        } catch (KnowledgeValidationException e) {
            throw Failures.validation(e.getMessage());
        }
        List<ContextItem> found;
        try {
            found = store.retrieve(scope, query).toCompletableFuture().join();
        } catch (RuntimeException e) {
            Throwable cause = e instanceof CompletionException && e.getCause() != null ? e.getCause() : e;
            if (cause instanceof KnowledgeValidationException invalid) {
                throw Failures.validation(invalid.getMessage());
            }
            throw Failures.unavailable("The base could not be read (" + cause.getClass().getSimpleName() + ")");
        }

        Map<Sensitivity, Integer> byLevel = new EnumMap<>(Sensitivity.class);
        ArrayNode items = Json.MAPPER.createArrayNode();
        for (ContextItem item : found) {
            byLevel.merge(item.sensitivity(), 1, Integer::sum);
            ObjectNode node = items.addObject();
            node.put("content", item.content()).put("kind", item.origin().kind().name()).put("score", item.score())
                    .put("level", item.sensitivity().wireName());
            ArrayNode entities = node.putArray("entities");
            item.entities().forEach(e -> entities.addObject().put("id", e.id()).put("type", e.type()).put("label", e.label()));
            ObjectNode origin = node.putObject("origin");
            origin.put("kind", item.origin().kind().name()).put("id", item.origin().id());
            if (item.origin().documentId() != null) {
                origin.put("documentId", item.origin().documentId());
            }
        }
        ObjectNode out = Json.obj();
        out.set("items", items);
        ObjectNode counts = out.putObject("counts");
        for (Sensitivity level : Sensitivity.values()) {
            counts.put(level.wireName(), byLevel.getOrDefault(level, 0));
        }
        record(ctx, scope, query, counts);
        return out;
    }

    private void record(TaskContext ctx, KnowledgeScope scope, RetrievalQuery query, ObjectNode counts) {
        Map<String, String> detail = new LinkedHashMap<>();
        detail.put("base", scope.key());
        detail.put("topK", Integer.toString(query.topK()));
        detail.put("ceiling", query.ceiling().wireName());
        counts.properties().forEach(e -> detail.put(e.getKey(), e.getValue().asText()));
        audit.append(new AuditLog.AuditRecord(ctx.instanceId(), ctx.chainId(), clock.instant(), AUDIT_KIND, ctx.taskId(),
                ctx.sensitivity(), "knowledge-retrieve", "answered", detail)).toCompletableFuture().join();
    }
}
