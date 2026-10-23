package io.stintflow.example.meeting;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.example.meeting.Corpus.Meeting;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.knowledge.KnowledgeBatch;
import io.stintflow.knowledge.Passage;
import io.stintflow.knowledge.PassageIds;
import io.stintflow.knowledge.RetrievalQuery;
import io.stintflow.knowledge.Source;
import io.stintflow.spi.Sensitivity;
import io.stintflow.wire.CeWire;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.knowledge.KnowledgeWorkers;

/**
 * SDD 3c.6 — the report of the milestone, what must not leak out of the scenario, the language the base stores, and the channel
 * the origin has to declare. Fake models, synthetic data only.
 */
class ScenarioComplianceTest {

    @TempDir
    Path work;

    private record Ran(ScenarioRun.Result result, ObjectNode report, LocalScenario scenario) {
    }

    private Ran run(List<Meeting> meetings) throws Exception {
        LocalScenario scenario = new LocalScenario(meetings, ProductMeetingPipeline.fakes(meetings), new RunMetrics(Map.of(), false),
                work, InstantSource.system(), new InMemoryTimerService());
        ScenarioRun.Result result = ScenarioRun.execute(scenario.env(), meetings);
        return new Ran(result, ScenarioReport.build(result, scenario.env(), "local", "fake"), scenario);
    }

    @Test
    void ca6_ca7_the_report_follows_the_key_and_holds_no_text_and_nothing_leaks_out_of_the_scenario() throws Exception {
        List<Meeting> meetings = Corpus.load();
        Ran ran = run(meetings);
        try (LocalScenario scenario = ran.scenario()) {
            ObjectNode report = ran.report();

            // the header says what the numbers are
            assertThat(report.path("models").asText()).isEqualTo("fake");
            assertThat(report.path("stack").asText()).isEqualTo("local");
            assertThat(report.path("language").asText()).isEqualTo("en");
            assertThat(report.path("note").asText()).contains("not the quality of a model");

            // the decisions the key says the meetings take are in the product graph
            JsonNode decisions = report.path("productDecisions");
            assertThat(decisions.path("inTheGraph").asInt()).isEqualTo(decisions.path("expected").asInt()).isEqualTo(3);

            // the conflicts: the matrix is the key's (one conflict, found; nothing missed, nothing invented)
            JsonNode conflicts = report.path("conflicts");
            assertThat(conflicts.path("pairs").asInt()).isEqualTo(4);
            assertThat(conflicts.path("truePositive").asInt()).isEqualTo(1);
            assertThat(conflicts.path("falsePositive").asInt()).isZero();
            assertThat(conflicts.path("falseNegative").asInt()).isZero();
            assertThat(conflicts.path("trueNegative").asInt()).isEqualTo(3);
            assertThat(conflicts.path("reviewedByAPerson").asInt()).isEqualTo(3);   // the conflict, the unsure one, the unavailable one
            assertThat(conflicts.path("originUnavailable").asInt()).isEqualTo(1);
            assertThat(report.path("entityResolution").path("wrongMerges").asInt()).isZero();
            assertThat(report.path("product").path("summary").path("completed").asInt()).isEqualTo(3);
            assertThat(report.path("summary").path("architectureCompleted").asInt()).isEqualTo(4);

            // the sweep: no marker of a person, no summary of a product decision, in the report, the facts, the audit, the
            // pages of the product and the architecture areas, the state of every execution, the architecture graph
            List<String> planted = new ArrayList<>(Corpus.allMarkers(meetings));
            Corpus.loadProductDecisions().forEach(d -> planted.add(d.summary()));
            planted.add("SYNTH-PRODUCT-SUMMARY");
            StringBuilder everywhere = new StringBuilder(report.toString());
            scenario.channel.published().forEach(e -> everywhere.append(new String(CeWire.toJson(e), StandardCharsets.UTF_8)));
            everywhere.append(scenario.audit.all());
            scenario.confluence.pages().forEach(p -> everywhere.append(p.title()).append(p.body()).append(p.labels()));
            ran.result().product().forEach(r -> everywhere.append(r.context()));
            ran.result().architecture().forEach(r -> everywhere.append(
                    scenario.state.load(r.instanceId()).toCompletableFuture().join().orElseThrow()));
            everywhere.append(scenario.workers.architecture.dump(ArchitectureAdrPipeline.ARCHITECTURE));
            String dump = everywhere.toString();
            planted.forEach(text -> assertThat(dump).as("nothing leaks: %s", text).doesNotContain(text));

            Files.createDirectories(Path.of("target"));
            Files.writeString(Path.of("target", "3c.6-report.json"), report.toPrettyString(), StandardCharsets.UTF_8);
        }
    }

    @Test
    void ca8_the_base_stores_the_language_of_the_meetings_and_refuses_another() throws Exception {
        List<Meeting> meetings = Corpus.load();
        Ran ran = run(meetings);
        try (LocalScenario scenario = ran.scenario()) {
            var product = scenario.workers.product;
            var everything = new RetrievalQuery("checkout price table event topic", 50, 0.0, Sensitivity.CONFIDENTIAL, null,
                    Set.of(), null, 0);
            var english = new RetrievalQuery("checkout price table event topic", 50, 0.0, Sensitivity.CONFIDENTIAL, null,
                    Set.of(), "en", 0);
            int all = product.retrieve(KnowledgeIngestPipeline.SCOPE, everything).toCompletableFuture().join().size();
            int en = product.retrieve(KnowledgeIngestPipeline.SCOPE, english).toCompletableFuture().join().size();
            int pt = product.retrieve(KnowledgeIngestPipeline.SCOPE, new RetrievalQuery("checkout price table event topic", 50, 0.0,
                    Sensitivity.CONFIDENTIAL, null, Set.of(), "pt", 0)).toCompletableFuture().join().size();
            assertThat(all).isPositive().isEqualTo(en);
            assertThat(pt).as("no passage in another language").isZero();

            Passage portuguese = new Passage(PassageIds.of(KnowledgeIngestPipeline.SCOPE, "meeting:pt", 0, "O checkout continua sincrono."), 0, "O checkout continua sincrono.", "pt",
                    Sensitivity.INTERNAL);
            assertThatThrownBy(() -> product.write(KnowledgeIngestPipeline.SCOPE, new KnowledgeBatch("t:pt", "meeting:pt",
                    PassageIds.sha256Hex("pt"), 1L, Source.NONE, "meetings@1.0.0", List.of(portuguese), List.of(), List.of(),
                    List.of())).toCompletableFuture().join()).hasMessageContaining("language");
        }
    }

    @Test
    void ca9_an_origin_that_does_not_declare_its_channel_does_not_answer() throws Exception {
        try (ArchitectureAdrPipeline p = new ArchitectureAdrPipeline(work, InstantSource.system(), new InMemoryTimerService())) {
            assertThatThrownBy(() -> KnowledgeWorkers.registerResolve(new TaskHandlerRegistry(), p.product, p.product,
                    ProductBridge.contract().policy(), p.blobs, null, p.audit, InstantSource.system(), null))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("declare its channel");
        }
    }
}
