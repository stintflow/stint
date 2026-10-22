package io.stintflow.worker.knowledge;

import java.net.URI;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.knowledge.GraphPointer;
import io.stintflow.knowledge.KnowledgeValidationException;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.RetentionClass;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;

/**
 * The task {@code pair-material} (SDD 3c.4, 4.3), local to the architecture domain: puts what one pair needs to be judged — the
 * architecture decision and, when the origin resolved it, the product decision as the view of the origin shows it — in a blob of
 * its own, and hands the decision and the human only its pointer. Input {@code {"pairId", "decisionId", "pointer", "draftRef",
 * "viewRef"?}}; output {@code {"pairId", "materialRef", "context": "resolved" | "unavailable"}}. {@code context} is {@code
 * unavailable} when there is no resolved content for the pointer (the origin did not answer, the pointer was denied or removed):
 * the pair is then never decided by a machine, only reviewed. The material rests in {@code materialClass}.
 */
public final class PairMaterialHandler implements TaskHandler {

    private final BlobStore blobs;
    private final OriginAllowList origins;
    private final RetentionClass materialClass;

    public PairMaterialHandler(BlobStore blobs, OriginAllowList origins, RetentionClass materialClass) {
        this.blobs = Objects.requireNonNull(blobs, "blobs");
        this.origins = Objects.requireNonNull(origins, "origins");
        this.materialClass = Objects.requireNonNull(materialClass, "materialClass");
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext ctx) {
        try {
            return CompletableFuture.completedFuture(material(ctx));
        } catch (TaskFailure failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private JsonNode material(TaskContext ctx) {
        JsonNode in = ctx.input();
        String pairId = in.path("pairId").asText("");
        String decisionId = in.path("decisionId").asText("");
        GraphPointer pointer;
        try {
            pointer = GraphPointer.parse(in.path("pointer").asText(null));
        } catch (KnowledgeValidationException e) {
            throw Failures.validation("pointer: " + e.getMessage());
        }
        AdrDrafts.Decision decision = AdrDrafts.read(blobs, in.path("draftRef").asText(""), origins).stream()
                .filter(d -> d.id().equals(decisionId) && d.motivatedBy().contains(pointer)).findFirst()
                .orElseThrow(() -> Failures.validation("The draft has no such decision motivated by that pointer"));
        Optional<JsonNode> product = AdrDrafts.resolved(blobs, in.path("viewRef").asText(null), pointer);

        ObjectNode material = Json.obj();
        material.put("pairId", pairId);
        ObjectNode architecture = material.putObject("architecture");
        architecture.put("title", decision.title()).put("summary", decision.summary());
        ObjectNode other = material.putObject("product");
        other.put("pointer", pointer.format());
        if (product.isPresent()) {
            other.put("label", product.get().path("label").asText(""));
            other.put("status", product.get().path("properties").path("status").asText(""));
            other.put("summary", product.get().path("properties").path("summary").asText(""));
        }
        URI ref = blobs.put(Json.bytes(material), "adr-material/" + ctx.instanceId() + "-" + pairId.replace('#', '-') + ".json",
                materialClass).toCompletableFuture().join();
        ObjectNode out = Json.obj();
        out.put("pairId", pairId).put("materialRef", ref.toString()).put("context", product.isPresent() ? "resolved" : "unavailable");
        return out;
    }
}
