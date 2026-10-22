package io.stintflow.example.meeting;

import java.util.Locale;

import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.TextNode;

import io.stintflow.spi.BlobStore;
import io.stintflow.worker.decision.Decision;
import io.stintflow.worker.decision.FakeDecisionAdapter;

/**
 * The CI's decisions (SDD 3a.6): rules over the redacted transcript, standing for the local LitJev. The document type
 * follows the cue words the corpus uses; a meeting that says it is "not sure what document" it needs is uncertain
 * (confidence 0.3, below the 0.7 threshold), so the review branch runs. Urgency: "urgent" or "today".
 */
public final class FakeDecisions {

    private FakeDecisions() {
    }

    public static FakeDecisionAdapter docType() {
        return new FakeDecisionAdapter("rules", request -> {
            String text = request.input().path("text").asText().toLowerCase(Locale.ROOT);
            if (text.contains("not sure what document")) {
                return Decision.of(TextNode.valueOf("ata"), 0.3);
            }
            if (text.contains("decision record")) {
                return Decision.of(TextNode.valueOf("adr"), 0.92);
            }
            if (text.contains("runbook")) {
                return Decision.of(TextNode.valueOf("runbook"), 0.9);
            }
            if (text.contains("product requirements")) {
                return Decision.of(TextNode.valueOf("prd"), 0.9);
            }
            return Decision.of(TextNode.valueOf("ata"), 0.9);
        });
    }

    /**
     * The conflict check (SDD 3c.4): rules over the material of one pair, which it reads from the blob store. The architecture
     * decision and the product decision that contradict each other — one says the checkout is synchronous, the other makes it
     * asynchronous — conflict (confidence 0.9); a pair that says it is "unclear" is uncertain (0.3, below the threshold, so the
     * review runs); the rest is compatible (0.9). Stands for the local model; the real adapter has the same contract.
     */
    public static FakeDecisionAdapter conflict(BlobStore blobs) {
        return new FakeDecisionAdapter("conflict-rules", request -> {
            com.fasterxml.jackson.databind.JsonNode material = io.stintflow.wire.Json
                    .read(blobs.get(java.net.URI.create(request.input().path("materialRef").asText())).toCompletableFuture().join());
            String architecture = material.path("architecture").path("summary").asText().toLowerCase(Locale.ROOT);
            String product = (material.path("product").path("label").asText() + " "
                    + material.path("product").path("summary").asText()).toLowerCase(Locale.ROOT);
            if (architecture.contains("unclear") || product.contains("unclear")) {
                return Decision.of(TextNode.valueOf("compatible"), 0.3);
            }
            if (product.contains("synchronous") && architecture.contains("asynchronous")) {
                return Decision.of(TextNode.valueOf("conflicts"), 0.9);
            }
            return Decision.of(TextNode.valueOf("compatible"), 0.9);
        });
    }

    public static FakeDecisionAdapter urgency() {
        return new FakeDecisionAdapter("urgency-rules", request -> {
            String text = request.input().path("text").asText().toLowerCase(Locale.ROOT);
            return Decision.of(BooleanNode.valueOf(text.contains("urgent") || text.contains("today")), 0.9);
        });
    }
}
