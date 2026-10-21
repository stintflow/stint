package io.stintflow.worker.knowledge;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.knowledge.EntityIds;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.spi.BlobStore;
import io.stintflow.wire.ClaimCheck;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;
import io.stintflow.worker.redaction.Pseudonyms;

/**
 * The task {@code resolve-person} (SDD 3b.5, 4.3, decision 8b): turns who a person is into the id of their entity in the base,
 * with the secret behind the stable pseudonyms — which is why it runs where the redactor's secret is, and nowhere else.
 * <p>
 * <b>Input.</b> {@code {"personRef": "<uri>" | claim-check, "scope": {"namespace", "base"}}}: the pointer is to a restricted blob
 * that holds the identifier the data subject gave, as the redactor saw it in the meetings (the name). <b>Answer.</b>
 * {@code {"entityId"}} — never the name and never the pseudonym. The id is the same one the ingestion gave the {@code Person}
 * ({@code labelStable} of the scope, then {@code EntityIds.of}), so a person who is not in the base yet has an id too, and
 * erasing it leaves the tombstone that keeps them out.
 */
public final class ResolvePersonHandler implements TaskHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ResolvePersonHandler.class);

    private final Pseudonyms pseudonyms;
    private final BlobStore blobs;
    private final ScopeAllowList scopes;

    public ResolvePersonHandler(Pseudonyms pseudonyms, BlobStore blobs, ScopeAllowList scopes) {
        this.pseudonyms = Objects.requireNonNull(pseudonyms, "pseudonyms");
        this.blobs = Objects.requireNonNull(blobs, "blobs");
        this.scopes = Objects.requireNonNull(scopes, "scopes");
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext ctx) {
        try {
            return CompletableFuture.completedFuture(resolve(ctx));
        } catch (TaskFailure failure) {
            return CompletableFuture.failedFuture(failure);
        } catch (RuntimeException e) {
            LOG.warn("Person resolution of task {} in instance {} failed ({})", ctx.taskId(), ctx.instanceId(),
                    e.getClass().getName());
            return CompletableFuture.failedFuture(Failures.unavailable("The person could not be resolved ("
                    + e.getClass().getSimpleName() + ")"));
        }
    }

    private JsonNode resolve(TaskContext ctx) {
        JsonNode in = ctx.input();
        KnowledgeScope scope = scopes.require(in);
        String name = new String(read(in.path("personRef")), StandardCharsets.UTF_8).strip();
        if (name.isEmpty() || name.length() > 200) {
            throw Failures.validation("The subject is empty or longer than 200 characters");
        }
        String pseudonym = pseudonyms.labelStable(scope.key(), "person", name);
        ObjectNode out = Json.obj();
        out.put("entityId", EntityIds.of("Person", pseudonym));
        return out;
    }

    static byte[] read(BlobStore blobs, JsonNode pointer) {
        if (!pointer.isTextual() && !ClaimCheck.isPointer(pointer)) {
            throw Failures.validation("personRef is required");
        }
        String uri = pointer.isTextual() ? pointer.asText() : pointer.get(ClaimCheck.POINTER_FIELD).asText();
        try {
            return blobs.get(URI.create(uri)).toCompletableFuture().join();
        } catch (RuntimeException e) {
            throw Failures.unavailable("The subject could not be read");
        }
    }

    private byte[] read(JsonNode pointer) {
        return read(blobs, pointer);
    }
}
