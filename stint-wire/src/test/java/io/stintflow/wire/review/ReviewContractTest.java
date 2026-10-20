package io.stintflow.wire.review;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.StringWriter;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.spi.CorrectionLog.Correction;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;

/**
 * SDD 3a.4, 4.2: an answer carries the contract's fields only — a value among the options, or a verdict and one
 * verdict per item asked; a note by pointer; an opaque reviewer — and the problems name fields, never values. The
 * export writes one line per correction at or below the level asked.
 */
class ReviewContractTest {

    static final List<String> OPTIONS = List.of("ata", "adr");

    @Test
    void a_choice_takes_one_of_the_options_and_an_opaque_reviewer() {
        assertThat(ReviewContract.violations(Json.obj().put("value", "adr").put("reviewer", "rev-1")
                .put("noteRef", "s3://notes/n.md"), "choice", OPTIONS, List.of())).isEmpty();
        assertThat(ReviewContract.violations(Json.obj().put("value", "other").put("reviewer", "rev-1"), "choice",
                OPTIONS, List.of())).containsExactly("value must be one of the options");
        assertThat(ReviewContract.violations(Json.obj().put("value", "adr").put("reviewer", "Synth Person"),
                "choice", OPTIONS, List.of())).singleElement().asString().startsWith("reviewer must be an opaque id");
        assertThat(ReviewContract.violations(Json.obj().put("value", "adr").put("reviewer", "a@b.example"),
                "choice", OPTIONS, List.of())).hasSize(1);
        assertThat(ReviewContract.violations(Json.obj().put("value", "adr").put("reviewer", "rev-1")
                .put("note", "SYNTH-PII-0701"), "choice", OPTIONS, List.of()))
                .containsExactly("unknown field 'note' (no free text: a note goes by noteRef)")
                .noneMatch(p -> p.contains("SYNTH-PII"));
        assertThat(ReviewContract.violations(Json.obj().put("value", "adr").put("reviewer", "rev-1")
                .put("noteRef", "just some words"), "choice", OPTIONS, List.of()))
                .containsExactly("noteRef must be a pointer (a URI with a scheme)");
    }

    @Test
    void an_approval_of_items_needs_one_verdict_per_item_asked() {
        List<String> asked = List.of("sdd-1", "sdd-2");
        ObjectNode both = Json.obj().put("reviewer", "rev-1");
        both.putArray("items").add(Json.obj().put("id", "sdd-1").put("verdict", "approved"))
                .add(Json.obj().put("id", "sdd-2").put("verdict", "rejected").put("noteRef", "s3://n/2"));
        assertThat(ReviewContract.violations(both, "approval", List.of(), asked)).isEmpty();
        assertThat(ReviewContract.verdictOf(both)).isEqualTo("rejected");
        assertThat(ReviewContract.rejectedItems(both)).containsExactly("sdd-2");

        ObjectNode unknown = Json.obj().put("reviewer", "rev-1");
        unknown.putArray("items").add(Json.obj().put("id", "sdd-1").put("verdict", "approved"))
                .add(Json.obj().put("id", "sdd-9").put("verdict", "approved"));
        assertThat(ReviewContract.violations(unknown, "approval", List.of(), asked))
                .contains("each item id must be one asked, once", "every item asked needs a verdict");
        assertThat(ReviewContract.violations(Json.obj().put("reviewer", "rev-1"), "approval", List.of(), List.of()))
                .containsExactly("verdict is required");
        assertThat(ReviewContract.verdictOf(Json.obj().put("verdict", "approved"))).isEqualTo("approved");
    }

    @Test
    void only_a_content_free_answer_may_rest_in_the_inbox() {
        assertThat(ReviewContract.isContentFree(Json.obj().put("value", "adr").put("reviewer", "rev-1"))).isTrue();
        assertThat(ReviewContract.isContentFree(Json.obj().put("value", true).put("reviewer", "rev-1"))).isTrue();
        assertThat(ReviewContract.isContentFree(Json.obj().put("value", "some free words").put("reviewer", "rev-1")))
                .isFalse();
        assertThat(ReviewContract.isContentFree(Json.obj().put("verdict", "approved").put("reviewer", "rev-1")
                .put("note", "x"))).isFalse();
        assertThat(ReviewContract.isContentFree(Json.obj().put("verdict", "approved"))).isFalse();   // no reviewer
    }

    @Test
    void the_export_writes_one_line_per_correction_at_or_below_the_level() {
        WorkflowRef ref = new WorkflowRef("meetings", "triage", "1.0.0");
        Instant at = Instant.parse("2026-10-13T12:00:00Z");
        Correction internal = new Correction(ref, "i-1", "/do/0/x/review", "i-1:/do/0/x/review:1", "choice",
                Sensitivity.INTERNAL, "s3://m/1", Json.obj().put("value", "ata").put("confidence", 0.4),
                Json.obj().put("value", "ata"), "business-rep", "rev-1", at, at.plusSeconds(60), false);
        Correction unclassified = new Correction(ref, "i-2", "/do/0/x/review", "i-2:/do/0/x/review:1", "choice",
                null, "s3://m/2", null, Json.obj().put("value", "adr"), "business-rep", "rev-2", at, at, false);
        StringWriter out = new StringWriter();

        assertThat(CorrectionExport.write(List.of(internal, unclassified), Sensitivity.CONFIDENTIAL, out))
                .isEqualTo(1);   // no level counts as restricted
        assertThat(out.toString()).contains("\"agrees\":true", "\"definition\":\"meetings:triage:1.0.0\"",
                "\"reviewedAt\":\"2026-10-13T12:01:00Z\"").endsWith("\n");
    }
}
