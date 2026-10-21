package io.stintflow.worker.knowledge;

import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.knowledge.KnowledgeBatch;
import io.stintflow.knowledge.KnowledgeBatchJson;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.KnowledgeStore;
import io.stintflow.knowledge.KnowledgeValidationException;
import io.stintflow.knowledge.WriteReceipt;
import io.stintflow.spi.BlobStore;
import io.stintflow.wire.ClaimCheck;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;

/**
 * The task {@code knowledge-write} (SDD 3b.3, 4.4): reads a knowledge batch by pointer and writes it to the base in one atomic
 * step (vector, graph and change log together — SDD 3b.1).
 * <p>
 * <b>Input.</b> {@code {"batchRef": "<uri>", "derivedFrom"?: ["<uri>", ...], "scope": {"namespace", "base"}}}. <b>Answer.</b> {@code {"status", "counts",
 * "lastSequence", "dropped"}}. {@code UNCHANGED}, {@code STALE} and {@code REJECTED_ERASED} are normal answers — a redelivery
 * and an erasure are not failures.
 * <p>
 * <b>Failure.</b> A scope this worker does not serve: {@code authorization} 403, nothing read. A batch with personal data the
 * patterns still find ({@link PiiGate}), or one the base refuses: {@code validation} 400 with counts and rules, never a value. A
 * blob or a base that can not be reached: {@code communication} 503 — the engine retries, and the write is idempotent.
 */
public final class KnowledgeWriteHandler implements TaskHandler {

    private static final Logger LOG = LoggerFactory.getLogger(KnowledgeWriteHandler.class);

    private final KnowledgeStore store;
    private final BlobStore blobs;
    private final ScopeAllowList scopes;
    private final boolean piiGate;

    /** With the personal-data gate on. */
    public KnowledgeWriteHandler(KnowledgeStore store, BlobStore blobs, ScopeAllowList scopes) {
        this(store, blobs, scopes, true);
    }

    /**
     * @param piiGate whether a batch is checked for personal data before it is written; on by default — switching it off is a
     *                deliberate act of the worker owner
     */
    public KnowledgeWriteHandler(KnowledgeStore store, BlobStore blobs, ScopeAllowList scopes, boolean piiGate) {
        this.store = Objects.requireNonNull(store, "store");
        this.blobs = Objects.requireNonNull(blobs, "blobs");
        this.scopes = Objects.requireNonNull(scopes, "scopes");
        this.piiGate = piiGate;
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext ctx) {
        try {
            return CompletableFuture.completedFuture(write(ctx));
        } catch (TaskFailure failure) {
            return CompletableFuture.failedFuture(failure);
        } catch (RuntimeException e) {
            LOG.warn("Knowledge write of task {} in instance {} failed ({})", ctx.taskId(), ctx.instanceId(),
                    e.getClass().getName());
            return CompletableFuture.failedFuture(Failures.unavailable("The knowledge write failed ("
                    + e.getClass().getSimpleName() + ")"));
        }
    }

    private JsonNode write(TaskContext ctx) {
        JsonNode in = ctx.input();
        KnowledgeScope scope = scopes.require(in);
        KnowledgeBatch batch = read(in.path("batchRef")).withDerivedFrom(origins(in));
        if (piiGate) {
            Map<String, Integer> found = PiiGate.findings(batch);
            if (!found.isEmpty()) {
                LOG.warn("Task {} of instance {}: the batch still holds personal data {}; refused", ctx.taskId(),
                        ctx.instanceId(), found);
                throw Failures.validation("The batch still holds personal data (counts by type: " + found
                        + "); it was not written");
            }
        }
        WriteReceipt receipt;
        try {
            receipt = store.write(scope, batch).toCompletableFuture().join();
        } catch (RuntimeException e) {
            Throwable cause = e instanceof CompletionException && e.getCause() != null ? e.getCause() : e;
            if (cause instanceof KnowledgeValidationException invalid) {
                throw Failures.validation("The base refused the batch: " + invalid.getMessage());
            }
            throw Failures.unavailable("The base could not be written (" + cause.getClass().getSimpleName() + ")");
        }
        LOG.info("Knowledge write of task {} in instance {}: {} {}", ctx.taskId(), ctx.instanceId(), receipt.status(),
                receipt.counts());
        ObjectNode out = Json.obj();
        out.put("status", receipt.status().name());
        ObjectNode counts = out.putObject("counts");
        receipt.counts().forEach(counts::put);
        out.put("lastSequence", receipt.lastSequence());
        out.put("dropped", receipt.dropped());
        return out;
    }

    /**
     * SDD 3b.5, 4.4: the blobs the document came from, so erasing it takes them too — the batch just read and what the task
     * names in {@code derivedFrom} (the approved minutes). A pointer that is not a URI is ignored.
     */
    private static java.util.List<URI> origins(JsonNode in) {
        java.util.List<URI> origins = new java.util.ArrayList<>();
        JsonNode batch = in.path("batchRef");
        addOrigin(origins, batch.isTextual() ? batch.asText() : batch.path(ClaimCheck.POINTER_FIELD).asText(""));
        for (JsonNode ref : in.path("derivedFrom")) {
            addOrigin(origins, ref.isTextual() ? ref.asText() : ref.path(ClaimCheck.POINTER_FIELD).asText(""));
        }
        return origins;
    }

    private static void addOrigin(java.util.List<URI> origins, String uri) {
        if (!uri.isBlank()) {
            try {
                origins.add(URI.create(uri));
            } catch (IllegalArgumentException e) {
                // not a pointer: nothing to delete later
            }
        }
    }

    private KnowledgeBatch read(JsonNode pointer) {
        if (!pointer.isTextual() && !ClaimCheck.isPointer(pointer)) {
            throw Failures.validation("batchRef is required");
        }
        String uri = pointer.isTextual() ? pointer.asText() : pointer.get(ClaimCheck.POINTER_FIELD).asText();
        byte[] bytes;
        try {
            bytes = blobs.get(URI.create(uri)).toCompletableFuture().join();
        } catch (RuntimeException e) {
            throw Failures.unavailable("The batch could not be read");
        }
        try {
            return KnowledgeBatchJson.fromJson(Json.read(bytes));
        } catch (RuntimeException e) {
            throw Failures.validation("The batch is not a knowledge batch");
        }
    }
}
