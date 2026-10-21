package io.stintflow.example.meeting;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.cloudevents.CloudEvent;
import io.stintflow.example.meeting.Corpus.Meeting;
import io.stintflow.knowledge.BaseInfo;
import io.stintflow.knowledge.inspector.InspectorAccess;
import io.stintflow.knowledge.inspector.InspectorService;
import io.stintflow.knowledge.inspector.Metrics;
import io.stintflow.knowledge.inspector.Snapshot;

/**
 * SDD 3b.6, on the base the product flow builds from the synthetic corpus: what the inspector shows agrees with what the base says
 * about itself, the timeline is the ingestion of each meeting, and nothing a meeting said in the clear reaches any response.
 */
class KnowledgeInspectionTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir
    Path work;

    private final List<Meeting> meetings = Corpus.load();

    @Test
    void the_inspector_agrees_with_the_base_the_ingestion_built_and_shows_nothing_in_the_clear() throws Exception {
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, ProductMeetingPipeline.fakes(meetings),
                new RunMetrics(Map.of(), false), work);
             KnowledgeIngestPipeline ingest = new KnowledgeIngestPipeline(pipeline.blobs,
                     new FakeKnowledgeModel(Corpus.loadKnowledge().base().glossary()))) {
            for (Meeting meeting : meetings) {
                pipeline.run(meeting, Duration.ofSeconds(60));
                CloudEvent published = pipeline.channel.published().stream().filter(e -> e.getType().endsWith("minutes-published.v1"))
                        .filter(e -> meeting.id().equals(e.getSubject())).findFirst().orElseThrow();
                ingest.ingest(published, Duration.ofSeconds(60));
            }
            InspectorService service = new InspectorService(ingest.store,
                    InspectorAccess.allowing(Map.of("demo", Set.of(KnowledgeIngestPipeline.SCOPE))),
                    new InspectorService.Settings(5000, io.stintflow.spi.Sensitivity.RESTRICTED, Metrics.Thresholds.DEFAULT));
            BaseInfo info = ingest.store.describe(KnowledgeIngestPipeline.SCOPE).toCompletableFuture().join();

            InspectorService.Current current = service.current("demo", KnowledgeIngestPipeline.SCOPE);
            Snapshot snapshot = service.snapshot("demo", KnowledgeIngestPipeline.SCOPE, null, null);
            Metrics.Report metrics = service.metrics("demo", KnowledgeIngestPipeline.SCOPE);

            assertThat(current.nodeCount()).isEqualTo(info.entities()).isPositive();
            assertThat(current.edges()).hasSize(info.relations());
            assertThat(snapshot.nodes()).hasSize(info.entities());
            assertThat(snapshot.edges()).hasSize(info.relations());
            assertThat(snapshot.passages()).isEqualTo(info.passages());
            assertThat(metrics.nodes()).isEqualTo(info.entities());
            // one chain per meeting wrote the base, and each is in the metrics
            assertThat(metrics.addedByChain()).isNotEmpty();
            assertThat(service.snapshot("demo", KnowledgeIngestPipeline.SCOPE, 0L, null).nodes()).isEmpty();
            // a person appears as the stable pseudonym, never as the name the speaker said
            String everything = JSON.writeValueAsString(current) + JSON.writeValueAsString(snapshot)
                    + JSON.writeValueAsString(metrics) + service.graphMl("demo", KnowledgeIngestPipeline.SCOPE)
                    + JSON.writeValueAsString(service.changes("demo", KnowledgeIngestPipeline.SCOPE, 0, 5000,
                            InspectorService.ChangeFilter.ALL));
            for (Meeting meeting : meetings) {
                for (String name : meeting.personMarkers()) {
                    assertThat(everything).as("the name %s", name).doesNotContain(name);
                }
            }
        }
    }
}
