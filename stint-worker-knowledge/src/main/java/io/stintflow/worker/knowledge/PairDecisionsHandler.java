package io.stintflow.worker.knowledge;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.knowledge.GraphPointer;
import io.stintflow.spi.BlobStore;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;

/**
 * The task {@code pair-decisions} (SDD 3c.4, 4.2), local to the architecture domain: reads the draft of the decisions of a meeting
 * and says which pairs (decision, pointer) are to be checked and which pointers are to be resolved. Input {@code {"draftRef"}};
 * output {@code {"pointers": [...], "pairs": [{"pairId", "decisionId", "pointer"}]}}. The limits of {@link AdrDrafts} apply, and
 * a draft that breaks one is refused whole (400), before any pointer is resolved or any pair is checked. It reads one blob and
 * writes nothing.
 */
public final class PairDecisionsHandler implements TaskHandler {

    private final BlobStore blobs;
    private final OriginAllowList origins;

    public PairDecisionsHandler(BlobStore blobs, OriginAllowList origins) {
        this.blobs = Objects.requireNonNull(blobs, "blobs");
        this.origins = Objects.requireNonNull(origins, "origins");
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext ctx) {
        try {
            List<AdrDrafts.Decision> decisions = AdrDrafts.read(blobs, ctx.input().path("draftRef").asText(""), origins);
            Set<String> pointers = new LinkedHashSet<>();
            ObjectNode out = Json.obj();
            ArrayNode pairs = out.putArray("pairs");
            for (AdrDrafts.Decision decision : decisions) {
                for (int i = 0; i < decision.motivatedBy().size(); i++) {
                    GraphPointer pointer = decision.motivatedBy().get(i);
                    pointers.add(pointer.format());
                    pairs.addObject().put("pairId", AdrDrafts.pairId(decision.id(), i)).put("decisionId", decision.id())
                            .put("pointer", pointer.format());
                }
            }
            ArrayNode distinct = out.putArray("pointers");
            pointers.forEach(distinct::add);
            return CompletableFuture.completedFuture(out);
        } catch (TaskFailure failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }
}
