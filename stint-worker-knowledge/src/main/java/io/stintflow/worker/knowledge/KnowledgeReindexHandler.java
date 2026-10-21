package io.stintflow.worker.knowledge;

import java.time.InstantSource;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.knowledge.Embedder;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.KnowledgeStore;
import io.stintflow.knowledge.KnowledgeValidationException;
import io.stintflow.knowledge.ReindexReceipt;
import io.stintflow.knowledge.ReindexRequest;
import io.stintflow.spi.AuditLog;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;

/**
 * The task {@code knowledge-reindex} (SDD 3b.5, 4.2): moves a base to the embedding model this worker is configured with. The
 * definition names the model it expects; the worker decides — it has the embedder, and refuses a request for another model.
 * <p>
 * <b>Input.</b> {@code {"requestId", "targetModel", "scope": {"namespace", "base"}}}. <b>Answer.</b> {@code {"status", "from",
 * "to", "passages", "skipped", "lastSequence"}}. A base already on the model is {@code UNCHANGED}, a normal answer — a
 * redelivered request is not a failure. <b>Failure.</b> Another model than the worker's, or a model that does not embed a
 * language the base holds: {@code validation} 400. A base that can not be reached: {@code communication} 503.
 * <b>Audit.</b> A record of kind {@value #AUDIT_KIND} with the base, the two models and the counts.
 */
public final class KnowledgeReindexHandler implements TaskHandler {

    public static final String AUDIT_KIND = "knowledge-reindex";

    private static final Logger LOG = LoggerFactory.getLogger(KnowledgeReindexHandler.class);
    private static final Pattern REQUEST_ID = Pattern.compile("[A-Za-z0-9._:-]{1,80}");

    private final KnowledgeStore store;
    private final Embedder embedder;
    private final ScopeAllowList scopes;
    private final AuditLog audit;
    private final InstantSource clock;

    /** @param embedder the model this worker serves: the only one it moves a base to */
    public KnowledgeReindexHandler(KnowledgeStore store, Embedder embedder, ScopeAllowList scopes, AuditLog audit,
            InstantSource clock) {
        this.store = Objects.requireNonNull(store, "store");
        this.embedder = Objects.requireNonNull(embedder, "embedder");
        this.scopes = Objects.requireNonNull(scopes, "scopes");
        this.audit = audit == null ? AuditLog.NONE : audit;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext ctx) {
        try {
            return CompletableFuture.completedFuture(reindex(ctx));
        } catch (TaskFailure failure) {
            return CompletableFuture.failedFuture(failure);
        } catch (RuntimeException e) {
            LOG.warn("Knowledge reindexation of task {} in instance {} failed ({})", ctx.taskId(), ctx.instanceId(),
                    e.getClass().getName());
            return CompletableFuture.failedFuture(Failures.unavailable("The reindexation failed ("
                    + e.getClass().getSimpleName() + ")"));
        }
    }

    private JsonNode reindex(TaskContext ctx) {
        JsonNode in = ctx.input();
        KnowledgeScope scope = scopes.require(in);
        String requestId = in.path("requestId").asText("");
        if (!REQUEST_ID.matcher(requestId).matches()) {
            throw Failures.validation("requestId is 1 to 80 letters, digits or . _ : -");
        }
        String asked = in.path("targetModel").asText("");
        if (!asked.equals(embedder.model())) {
            throw Failures.validation("This worker serves another embedding model than the one asked");
        }
        ReindexReceipt receipt;
        try {
            receipt = store.reindex(scope, new ReindexRequest(requestId, embedder, "reindex:model")).toCompletableFuture()
                    .join();
        } catch (RuntimeException e) {
            Throwable cause = e instanceof CompletionException && e.getCause() != null ? e.getCause() : e;
            if (cause instanceof KnowledgeValidationException invalid) {
                throw Failures.validation("The base refused the reindexation: " + invalid.getMessage());
            }
            throw Failures.unavailable("The base could not be reindexed (" + cause.getClass().getSimpleName() + ")");
        }
        LOG.info("Knowledge reindexation of task {} in instance {}: {} {} passages, {} skipped", ctx.taskId(),
                ctx.instanceId(), receipt.status(), receipt.passages(), receipt.skipped());
        ObjectNode out = Json.obj();
        out.put("status", receipt.status().name());
        out.put("from", receipt.fromModel());
        out.put("to", receipt.toModel());
        out.put("passages", receipt.passages());
        out.put("skipped", receipt.skipped());
        out.put("lastSequence", receipt.lastSequence());

        Map<String, String> detail = new LinkedHashMap<>();
        detail.put("base", scope.key());
        detail.put("from", String.valueOf(receipt.fromModel()));
        detail.put("to", receipt.toModel());
        detail.put("passages", Integer.toString(receipt.passages()));
        detail.put("skipped", Integer.toString(receipt.skipped()));
        audit.append(new AuditLog.AuditRecord(ctx.instanceId(), ctx.chainId(), clock.instant(), AUDIT_KIND, ctx.taskId(),
                ctx.sensitivity(), "knowledge-reindex", receipt.status().name().toLowerCase(java.util.Locale.ROOT), detail))
                .toCompletableFuture().join();
        return out;
    }
}
