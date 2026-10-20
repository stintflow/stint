package io.stintflow.spi;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * SDD 3a.4, RF7: where human corrections go, by workflow definition — the evaluation set of the decision adapters.
 * A correction carries pointers (to the material, to a note), declared names (options, verdicts), ids and the
 * decision's own record; never the data itself.
 */
public interface CorrectionLog {

    CompletionStage<Void> append(Correction correction);

    /** The corrections of one definition, oldest first. */
    CompletionStage<List<Correction>> of(WorkflowRef definition);

    /** What the engine records with no correction log configured: nothing. */
    CorrectionLog NONE = new CorrectionLog() {
        @Override
        public CompletionStage<Void> append(Correction correction) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<List<Correction>> of(WorkflowRef definition) {
            return CompletableFuture.completedFuture(List.of());
        }
    };

    /**
     * One human answer to a review.
     *
     * @param definition  the definition the review belongs to
     * @param instanceId  the instance
     * @param pointer     the review's position
     * @param reviewId    {@code <instanceId>:<pointer>:<n>}
     * @param kind        {@code choice} or {@code approval}
     * @param sensitivity the instance's level when it was answered, or {@code null}
     * @param materialRef the pointer to the (redacted) material the reviewer saw
     * @param decision    the decision's own record (value, confidence, uncertain, provenance) for a review of a
     *                    decision; {@code null} for an approval
     * @param human       the answer: {@code value}, or {@code verdict} and {@code rejectedItems}, and {@code noteRef}
     * @param role        the role that answered
     * @param reviewer    the channel's opaque id of the reviewer
     * @param requestedAt when the review was asked
     * @param reviewedAt  when the answer was applied
     * @param escalated   whether it was answered after an escalation
     */
    record Correction(WorkflowRef definition, String instanceId, String pointer, String reviewId, String kind,
            Sensitivity sensitivity, String materialRef, JsonNode decision, JsonNode human, String role,
            String reviewer, Instant requestedAt, Instant reviewedAt, boolean escalated) {

        public Correction {
            Objects.requireNonNull(definition, "definition");
            Objects.requireNonNull(reviewId, "reviewId");
        }
    }
}
