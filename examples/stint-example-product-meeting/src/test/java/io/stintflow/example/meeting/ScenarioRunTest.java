package io.stintflow.example.meeting;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.stintflow.example.meeting.Corpus.Meeting;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;

/** SDD 3c.6 — the scenario, in the process, against the answer key of the corpus. Fake models, no sleep; synthetic data only. */
class ScenarioRunTest {

    @TempDir
    Path work;

    @Test
    void ca2_ca3_ca4_ca5_the_chain_follows_the_key() throws Exception {
        List<Meeting> meetings = Corpus.load();
        try (LocalScenario scenario = new LocalScenario(meetings, ProductMeetingPipeline.fakes(meetings),
                new RunMetrics(Map.of(), false), work, InstantSource.system(), new InMemoryTimerService())) {
            ScenarioRun.Result result = ScenarioRun.execute(scenario.env(), meetings);

            // the product decisions were born in the product meetings, and the architecture area was told
            assertThat(ScenarioRun.disagreements(result)).as("where the run does not follow the key").isEmpty();
            assertThat(result.product()).hasSize(3).allSatisfy(run -> assertThat(run.status()).isEqualTo(InstanceStatus.COMPLETED));
            assertThat(result.announced()).isNotEmpty();

            assertThat(result.architecture()).hasSize(4);
            for (ScenarioRun.PairRun run : result.architecture()) {
                assertThat(run.status()).as(run.meetingId()).isEqualTo(InstanceStatus.COMPLETED);
                assertThat(run.verdict()).as("verdict of %s", run.meetingId()).isEqualTo(run.expectedVerdict());
                assertThat(run.reviewed()).as("reviewed %s", run.meetingId()).isEqualTo(run.expectedReviewed());
            }
            assertThat(result.architecture()).filteredOn(r -> r.originDown())
                    .singleElement().satisfies(r -> assertThat(r.resolution()).isEqualTo("unavailable"));
            assertThat(result.architecture().getFirst().pageStatus()).isEqualTo("conflicting");
        }
    }
}
