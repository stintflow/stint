package io.stintflow.worker.knowledge;

import java.net.URI;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.spi.BlobNotFoundException;
import io.stintflow.spi.BlobStore;
import io.stintflow.wire.ClaimCheck;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;

/**
 * The task {@code forget-subject} (SDD 3b.5, 4.3): deletes the blob that held the identifier of a data subject once the erasure
 * was verified, so it does not outlive its use. Idempotent: a blob that is already gone is the answer wanted.
 * <p>
 * <b>Input.</b> {@code {"personRef": "<uri>" | claim-check}}; the task may carry none (a document erasure has no subject) and
 * then does nothing. <b>Answer.</b> {@code {"forgotten": true|false}}. A blob that could not be deleted:
 * {@code communication} 503.
 */
public final class ForgetSubjectHandler implements TaskHandler {

    private final BlobStore blobs;

    public ForgetSubjectHandler(BlobStore blobs) {
        this.blobs = Objects.requireNonNull(blobs, "blobs");
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext ctx) {
        try {
            return CompletableFuture.completedFuture(forget(ctx.input().path("personRef")));
        } catch (TaskFailure failure) {
            return CompletableFuture.failedFuture(failure);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(Failures.unavailable("The subject could not be deleted ("
                    + e.getClass().getSimpleName() + ")"));
        }
    }

    private JsonNode forget(JsonNode pointer) {
        ObjectNode out = Json.obj();
        if (!pointer.isTextual() && !ClaimCheck.isPointer(pointer)) {
            return out.put("forgotten", false);
        }
        String uri = pointer.isTextual() ? pointer.asText() : pointer.get(ClaimCheck.POINTER_FIELD).asText();
        try {
            blobs.delete(URI.create(uri)).toCompletableFuture().join();
        } catch (CompletionException e) {
            if (!(e.getCause() instanceof BlobNotFoundException)) {
                throw e;
            }
        }
        return out.put("forgotten", true);
    }
}
