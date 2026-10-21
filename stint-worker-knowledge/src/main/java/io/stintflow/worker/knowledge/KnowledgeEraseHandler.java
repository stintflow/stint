package io.stintflow.worker.knowledge;

import java.time.InstantSource;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.knowledge.DeleteReceipt;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.KnowledgeStore;
import io.stintflow.spi.AuditLog;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;

/**
 * The task {@code knowledge-erase} (SDD 3b.5, 4.3): erases a document, or a person by the id of their entity, from the vector
 * index, the graph and the blobs, in the store's one atomic step, and keeps the tombstone.
 * <p>
 * <b>Input.</b> See {@link ErasureInput}. <b>Answer.</b> {@code {"requestId", "counts", "lastSequence"}}. Repeating the
 * {@code requestId} erases nothing more and answers with no counts; erasing what the base does not hold is not a failure — the
 * tombstone is what protects against a later write.
 * <p>
 * <b>Failure.</b> A scope this worker does not serve: {@code authorization} 403. A blob that could not be deleted, or a base that
 * can not be reached: {@code communication} 503 — the erasure did not happen, and the engine retries it with the same id.
 * <b>Audit.</b> A record of kind {@value #AUDIT_KIND}: the base, the kind of erasure and the counts. No id of a person.
 */
public final class KnowledgeEraseHandler implements TaskHandler {

    public static final String AUDIT_KIND = "knowledge-erase";

    private static final Logger LOG = LoggerFactory.getLogger(KnowledgeEraseHandler.class);

    private final KnowledgeStore store;
    private final ScopeAllowList scopes;
    private final AuditLog audit;
    private final InstantSource clock;

    public KnowledgeEraseHandler(KnowledgeStore store, ScopeAllowList scopes, AuditLog audit, InstantSource clock) {
        this.store = Objects.requireNonNull(store, "store");
        this.scopes = Objects.requireNonNull(scopes, "scopes");
        this.audit = audit == null ? AuditLog.NONE : audit;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext ctx) {
        try {
            return CompletableFuture.completedFuture(erase(ctx));
        } catch (TaskFailure failure) {
            return CompletableFuture.failedFuture(failure);
        } catch (RuntimeException e) {
            LOG.warn("Knowledge erasure of task {} in instance {} failed ({})", ctx.taskId(), ctx.instanceId(),
                    e.getClass().getName());
            return CompletableFuture.failedFuture(Failures.unavailable("The knowledge erasure failed ("
                    + e.getClass().getSimpleName() + ")"));
        }
    }

    private JsonNode erase(TaskContext ctx) {
        JsonNode in = ctx.input();
        KnowledgeScope scope = scopes.require(in);
        ErasureInput input = ErasureInput.parse(in);
        DeleteReceipt receipt;
        try {
            receipt = store.delete(scope, input.request()).toCompletableFuture().join();
        } catch (RuntimeException e) {
            Throwable cause = e instanceof CompletionException && e.getCause() != null ? e.getCause() : e;
            throw Failures.unavailable("The base could not be erased (" + cause.getClass().getSimpleName() + ")");
        }
        LOG.info("Knowledge erasure of task {} in instance {}: {} {}", ctx.taskId(), ctx.instanceId(), input.kind(),
                receipt.counts());
        ObjectNode out = Json.obj();
        out.put("requestId", receipt.requestId());
        ObjectNode counts = out.putObject("counts");
        receipt.counts().forEach(counts::put);
        out.put("lastSequence", receipt.lastSequence());

        Map<String, String> detail = new LinkedHashMap<>();
        detail.put("base", scope.key());
        detail.put("kind", input.kind());
        receipt.counts().forEach((k, v) -> detail.put(k, Integer.toString(v)));
        audit.append(new AuditLog.AuditRecord(ctx.instanceId(), ctx.chainId(), clock.instant(), AUDIT_KIND, ctx.taskId(),
                ctx.sensitivity(), "knowledge-erase", "answered", detail)).toCompletableFuture().join();
        return out;
    }
}
