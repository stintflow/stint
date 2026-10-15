package io.stintflow.example.meeting;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

import io.stintflow.worker.transcription.FakeTranscriber;

/**
 * SDD 3a.6, 4.3: the corpus is whole — every script matches the manifest's hash, its key is complete, every planted
 * marker is listed and every listed one is planted, and the set covers each document type, both urgencies and the
 * review branch. Synthetic data only.
 */
class CorpusTest {

    static final Pattern MARKER = Pattern.compile("SYNTH-PII-\\d{4}");

    @Test
    void every_script_matches_the_manifest_and_its_key_is_complete() {
        List<Corpus.Meeting> meetings = Corpus.load();

        assertThat(meetings).hasSize(8);
        for (Corpus.Meeting meeting : meetings) {
            byte[] script = Corpus.bytes("corpus/meetings/" + meeting.id() + ".yaml");
            assertThat(FakeTranscriber.sha256(script)).as("hash of %s", meeting.id()).isEqualTo(meeting.scriptSha256());
            assertThat(meeting.language()).isEqualTo("en");
            assertThat(meeting.label()).isEqualTo("restricted");
            assertThat(meeting.expected().docType()).isIn("ata", "adr", "runbook");
            assertThat(meeting.expected().items()).isNotEmpty()
                    .allSatisfy(item -> assertThat(meeting.text()).contains(item));
            Matcher planted = MARKER.matcher(meeting.text());
            List<String> found = planted.results().map(java.util.regex.MatchResult::group).distinct().toList();
            assertThat(found).as("markers of %s", meeting.id()).containsExactlyInAnyOrderElementsOf(meeting.pii());
        }
        assertThat(meetings).extracting(m -> m.expected().docType()).contains("ata", "adr", "runbook");
        assertThat(meetings).extracting(m -> m.expected().urgent()).contains(true, false);
        assertThat(meetings).filteredOn(m -> m.expected().review()).isNotEmpty();
        assertThat(Corpus.personMarkers(meetings)).isNotEmpty().allMatch(m -> m.startsWith("SYNTH-PII-"));
    }
}
