package io.stintflow.example.meeting;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.stintflow.worker.transcription.FakeTranscriber;

/**
 * SDD 3c.6, CA3 and CA10 — the corpus of the scenario (format v1.2): each script matches the manifest, each product decision's
 * surface is in its script, each draft the build's model derives from an architecture script is the one the key lists, and the
 * decisions an architecture draft answers are decisions the product meetings take. Synthetic data only.
 */
class ArchitectureCorpusTest {

    @Test
    void ca10_the_scripts_match_the_manifest_and_the_product_decisions_are_in_their_scripts() {
        List<Corpus.ArchitectureMeeting> architecture = Corpus.loadArchitecture();
        assertThat(architecture).extracting(Corpus.ArchitectureMeeting::id).containsExactly("a-001", "a-002", "a-003", "a-004");
        for (Corpus.ArchitectureMeeting meeting : architecture) {
            assertThat(FakeTranscriber.sha256(Corpus.bytes("corpus/meetings/" + meeting.id() + ".yaml")))
                    .as("hash of %s", meeting.id()).isEqualTo(meeting.scriptSha256());
            assertThat(meeting.label()).isEqualTo("confidential");
            assertThat(meeting.text()).doesNotContain("SYNTH-PII-");
        }
        List<Corpus.ProductDecision> decisions = Corpus.loadProductDecisions();
        assertThat(decisions).hasSize(3);
        for (Corpus.ProductDecision decision : decisions) {
            String script = Corpus.load().stream().filter(m -> m.id().equals(decision.meetingId())).findFirst().orElseThrow().text();
            assertThat(script.toLowerCase(Locale.ROOT)).as("surface in %s", decision.meetingId())
                    .contains(decision.surface().toLowerCase(Locale.ROOT));
            assertThat(decision.summary()).startsWith("SYNTH-PRODUCT-SUMMARY");
        }
    }

    @Test
    void ca3_the_draft_the_model_derives_is_the_one_the_key_lists_and_answers_a_product_decision() {
        Set<String> labels = Set.copyOf(Corpus.loadProductDecisions().stream().map(Corpus.ProductDecision::label).toList());
        for (Corpus.ArchitectureMeeting meeting : Corpus.loadArchitecture()) {
            List<FakeArchitectureModel.Drafted> drafted = FakeArchitectureModel.draft(meeting.text());
            assertThat(drafted).as("drafts of %s", meeting.id()).hasSize(meeting.drafts().size()).hasSize(1);
            Corpus.Draft key = meeting.drafts().getFirst();
            assertThat(drafted.getFirst().id()).isEqualTo(key.id());
            assertThat(drafted.getFirst().title()).isEqualTo(key.title());
            assertThat(drafted.getFirst().motivatedBy()).isEqualTo(key.motivatedBy()).isSubsetOf(labels);
        }
        assertThat(Corpus.loadArchitecture()).flatExtracting(Corpus.ArchitectureMeeting::drafts)
                .extracting(Corpus.Draft::verdict).containsExactly("conflicts", "compatible", "compatible", "compatible");
    }
}
