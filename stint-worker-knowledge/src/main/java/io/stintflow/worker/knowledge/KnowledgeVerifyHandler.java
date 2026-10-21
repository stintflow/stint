package io.stintflow.worker.knowledge;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.knowledge.ErasureCheck;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.KnowledgeStore;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;

/**
 * The task {@code knowledge-verify} (SDD 3b.5, 4.3): looks, through the store's port, for what an erasure should have removed —
 * passages, entities, edges, links, the document's record and the blobs retired — and for the tombstone.
 * <p>
 * <b>Input.</b> The one of {@code knowledge-erase} (see {@link ErasureInput}). <b>Answer.</b> {@code {"requestId", "verified",
 * "tombstone", "remaining": {"PASSAGE": n, ...}}}: {@code verified} is true only with nothing remaining and the tombstone in
 * place. Something left is an answer, not a failure: the workflow tells it apart and raises the alarm.
 */
public final class KnowledgeVerifyHandler implements TaskHandler {

    private static final Logger LOG = LoggerFactory.getLogger(KnowledgeVerifyHandler.class);

    private final KnowledgeStore store;
    private final ScopeAllowList scopes;

    public KnowledgeVerifyHandler(KnowledgeStore store, ScopeAllowList scopes) {
        this.store = Objects.requireNonNull(store, "store");
        this.scopes = Objects.requireNonNull(scopes, "scopes");
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext ctx) {
        try {
            return CompletableFuture.completedFuture(verify(ctx));
        } catch (TaskFailure failure) {
            return CompletableFuture.failedFuture(failure);
        } catch (RuntimeException e) {
            LOG.warn("Knowledge verification of task {} in instance {} failed ({})", ctx.taskId(), ctx.instanceId(),
                    e.getClass().getName());
            return CompletableFuture.failedFuture(Failures.unavailable("The verification failed ("
                    + e.getClass().getSimpleName() + ")"));
        }
    }

    private JsonNode verify(TaskContext ctx) {
        JsonNode in = ctx.input();
        KnowledgeScope scope = scopes.require(in);
        ErasureInput input = ErasureInput.parse(in);
        ErasureCheck check;
        try {
            check = store.verifyErased(scope, input.request()).toCompletableFuture().join();
        } catch (RuntimeException e) {
            Throwable cause = e instanceof CompletionException && e.getCause() != null ? e.getCause() : e;
            throw Failures.unavailable("The base could not be inspected (" + cause.getClass().getSimpleName() + ")");
        }
        if (!check.verified()) {
            LOG.warn("Erasure of task {} in instance {} is not verified: remaining {}, tombstone {}", ctx.taskId(),
                    ctx.instanceId(), check.remaining(), check.tombstone());
        }
        ObjectNode out = Json.obj();
        out.put("requestId", check.requestId());
        out.put("verified", check.verified());
        out.put("tombstone", check.tombstone());
        ObjectNode remaining = out.putObject("remaining");
        check.remaining().forEach(remaining::put);
        return out;
    }
}
