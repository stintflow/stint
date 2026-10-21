package io.stintflow.worker.knowledge;

import java.time.InstantSource;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.knowledge.Audience;
import io.stintflow.knowledge.ContextBudget;
import io.stintflow.knowledge.ContextItem;
import io.stintflow.knowledge.KnowledgeReadAudit;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.KnowledgeSeeds;
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
 * <b>Input.</b> {@code {"query", "topK"?: 5, "hops"?, "minScore"?, "maxLevel"?, "mode"?: "auto", "entities"?: [name],
 * "budgetTokens"?, "scope": {"namespace", "base"}}}.
 * <b>Mode (SDD 3b.4, 4.3).</b> {@code semantic} searches by meaning ({@code hops} 0 unless asked). {@code relational} starts from the
 * entities the question names — those in {@code entities}, and those the base knows in the question's words — keeps the passages that
 * mention them and follows the graph ({@code hops} 2 unless asked); with none named it answers as {@code semantic}. {@code auto}
 * (the default) is relational when the question names something the base knows, else semantic. The answer says which it was.
 * <b>Budget.</b> {@code budgetTokens}, estimated ({@link ContextBudget}), keeps the best whole items that fit.
 * <b>Ceiling.</b> The level of the instance asking — absent reads as restricted — lowered, never raised, by {@code maxLevel}.
 * Nothing above it comes back, and nothing that tells it is there (SDD 3b.1, 4.4). <b>Answer.</b> {@code {"items": [{"content",
 * "kind", "score", "level", "entities", "origin"}], "counts": {"internal", "confidential", "restricted"}, "mode", "trimmed"}}.
 * <p>
 * <b>Audit.</b> Each read adds a record of kind {@value #AUDIT_KIND} with the base, the ceiling, {@code topK} and the counts by
 * level — never the question and never a content. With no audit log, nothing is recorded.
 */
public final class KnowledgeRetrieveHandler implements TaskHandler {

    /** The kind of the audit record of a read of the base. */
    public static final String AUDIT_KIND = KnowledgeReadAudit.KIND;

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
        String asked = in.path("mode").asText("auto");
        if (!List.of("auto", "semantic", "relational").contains(asked)) {
            throw Failures.validation("mode is auto, semantic or relational");
        }
        Integer budget = in.hasNonNull("budgetTokens") ? in.get("budgetTokens").asInt(-1) : null;
        if (budget != null && budget < 0) {
            throw Failures.validation("budgetTokens is 0 or more");
        }
        RetrievalQuery query;
        String mode;
        List<ContextItem> found;
        ContextBudget.Fit fit;
        try {
            Set<String> seeds = Set.of();
            if (!asked.equals("semantic")) {
                List<String> names = new ArrayList<>();
                in.path("entities").forEach(n -> names.add(n.asText()));
                seeds = KnowledgeSeeds.find(store, scope, question, names, ceiling);
            }
            mode = seeds.isEmpty() ? "semantic" : "relational";
            query = new RetrievalQuery(question, in.path("topK").asInt(5), in.path("minScore").asDouble(0), ceiling, null,
                    seeds, null, in.path("hops").asInt(mode.equals("relational") ? 2 : 0));
            found = store.retrieve(scope, query).toCompletableFuture().join();
            fit = ContextBudget.fit(found, budget);
            found = fit.kept();
        } catch (KnowledgeValidationException e) {
            throw Failures.validation(e.getMessage());
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
            if (!item.origin().evidence().isEmpty()) {
                ArrayNode evidence = origin.putArray("evidence");
                item.origin().evidence().forEach(evidence::add);
            }
        }
        ObjectNode out = Json.obj();
        out.set("items", items);
        ObjectNode counts = out.putObject("counts");
        for (Sensitivity level : Sensitivity.values()) {
            counts.put(level.wireName(), byLevel.getOrDefault(level, 0));
        }
        out.put("mode", mode).put("trimmed", fit.trimmed());
        KnowledgeReadAudit.record(audit, ctx.instanceId(), ctx.chainId(), clock.instant(), ctx.taskId(), ctx.sensitivity(),
                "knowledge-retrieve", scope, query, mode, fit.trimmed(), byLevel);
        return out;
    }
}
