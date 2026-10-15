package io.stintflow.example.meeting;

import java.util.Locale;

import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.TextNode;

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
            return Decision.of(TextNode.valueOf("ata"), 0.9);
        });
    }

    public static FakeDecisionAdapter urgency() {
        return new FakeDecisionAdapter("urgency-rules", request -> {
            String text = request.input().path("text").asText().toLowerCase(Locale.ROOT);
            return Decision.of(BooleanNode.valueOf(text.contains("urgent") || text.contains("today")), 0.9);
        });
    }
}
