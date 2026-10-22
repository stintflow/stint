package io.stintflow.example.meeting;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.stintflow.example.meeting.Corpus.Meeting;
import io.stintflow.knowledge.GraphPointer;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.wire.CeWire;
import io.stintflow.wire.Json;
import io.stintflow.worker.knowledge.ResolvedViews;

/**
 * SDD 3c.2, CA4, CA5 and CA13 — the milestone of the sub-phase: the architecture domain links to what the product domain
 * announced and then resolves it, and the product domain answers by its own policy; the content comes back in a blob that
 * expires, never in the state of the instance or in the base that asked. When the origin does not answer, the author's choice
 * (follow or suspend) decides. Fake models, no sleep; synthetic data only.
 */
class KnowledgeResolveTest {

    static final String INDEXED = "io.stintflow.knowledge.document-indexed.v1";
    static final String PUBLISHED = "io.stintflow.knowledge.nodes-published.v1";
    static final String LINKED = "io.stintflow.knowledge.references-linked.v1";
    static final String RESOLVED = "io.stintflow.knowledge.pointers-resolved.v1";

    @TempDir
    Path work;

    private final List<Meeting> meetings = Corpus.load();

    private static CloudEvent last(List<CloudEvent> emitted, String type) {
        return emitted.stream().filter(e -> e.getType().equals(type)).reduce((first, second) -> second).orElseThrow();
    }

    private static JsonNode data(CloudEvent event) {
        return Json.read(event.getData().toBytes());
    }

    /** The product domain ingests a meeting, announces it, and the architecture domain links; the fact that starts the resolution. */
    private CloudEvent linked(ProductMeetingPipeline pipeline, KnowledgeIngestPipeline bridge) throws Exception {
        Meeting meeting = meetings.getFirst();
        pipeline.run(meeting, Duration.ofSeconds(60));
        bridge.ingest(pipeline.channel.published().stream().filter(e -> e.getType().endsWith("minutes-published.v1"))
                .filter(e -> meeting.id().equals(e.getSubject())).findFirst().orElseThrow(), Duration.ofSeconds(60));
        bridge.announce(last(bridge.emitted(), INDEXED), Duration.ofSeconds(60));
        InstanceSnapshot done = bridge.link(last(bridge.emitted(), PUBLISHED), Duration.ofSeconds(60));
        assertThat(done.status()).isEqualTo(InstanceStatus.COMPLETED);
        return last(bridge.emitted(), LINKED);
    }

    @Test
    void ca13_the_references_are_resolved_by_the_origin_and_the_content_never_stays_with_the_asker() throws Exception {
        List<String> markers = Corpus.allMarkers(meetings);
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, ProductMeetingPipeline.fakes(meetings),
                new RunMetrics(Map.of(), false), work);
                KnowledgeIngestPipeline bridge = new KnowledgeIngestPipeline(pipeline.blobs,
                        new FakeKnowledgeModel(Corpus.loadKnowledge().base().glossary()))) {
            CloudEvent linked = linked(pipeline, bridge);
            JsonNode announced = data(linked);
            assertThat(announced.path("pointers")).isNotEmpty();
            announced.path("pointers").forEach(p -> assertThat(GraphPointer.parse(p.asText()).scope())
                    .isEqualTo(KnowledgeIngestPipeline.SCOPE));

            InstanceSnapshot resolved = bridge.resolve(linked, Duration.ofSeconds(60));

            assertThat(resolved.status()).as("at %s", resolved.position()).isEqualTo(InstanceStatus.COMPLETED);
            JsonNode report = data(last(bridge.emitted(), RESOLVED));
            assertThat(report.path("resolution").asText()).isEqualTo("resolved");
            assertThat(report.path("counts").path("RESOLVED").asInt()).isEqualTo(announced.path("pointers").size());
            assertThat(report.path("counts").path("DENIED").asInt()).isZero();

            // the content is in the blob the origin wrote, in the language it publishes
            JsonNode blob = ResolvedViews.read(pipeline.blobs, report.path("viewRef").asText()).orElseThrow();
            assertThat(blob.path("entries")).hasSize(announced.path("pointers").size());
            for (JsonNode entry : blob.path("entries")) {
                assertThat(entry.path("status").asText()).isEqualTo("RESOLVED");
                assertThat(entry.path("viewVersion").asText()).isEqualTo("product-v1");
                assertThat(entry.path("view").path("label").asText()).isNotBlank();
            }

            // and nowhere else of the asker's: not in the fact, the state, the audit, nor the base that holds the references
            String label = blob.path("entries").get(0).path("view").path("label").asText();
            List<String> places = new ArrayList<>();
            places.add("fact: " + new String(CeWire.toJson(last(bridge.emitted(), RESOLVED)), StandardCharsets.UTF_8));
            places.add("state: " + bridge.state.load(bridge.current).toCompletableFuture().join());
            places.add("audit: " + bridge.audit.of(bridge.current).toCompletableFuture().join());
            String dump = bridge.architecture.dump(KnowledgeIngestPipeline.ARCHITECTURE);
            assertThat(places).allSatisfy(place -> assertThat(place).doesNotContain(label));
            // (the anchor "Checkout Service" is a node of the asker's own, whatever the origin calls the system)
            for (JsonNode entry : blob.path("entries")) {
                String shown = entry.path("view").path("label").asText();
                if (!shown.equals("Checkout Service")) {
                    assertThat(dump).as("the label %s", shown).doesNotContain(shown);
                }
            }
            assertThat(places).allSatisfy(place -> markers.forEach(marker -> assertThat(place).doesNotContain(marker)));
            assertThat(bridge.audit.of(bridge.current).toCompletableFuture().join()).anySatisfy(r -> {
                assertThat(r.kind()).isEqualTo("pointer-resolve");
                assertThat(r.detail()).containsEntry("requester", "examples:architecture-resolve");
            });

            // once used, the content can be discarded without waiting for its retention
            ResolvedViews.discard(pipeline.blobs, report.path("viewRef").asText());
            assertThat(ResolvedViews.read(pipeline.blobs, report.path("viewRef").asText())).isEmpty();
        }
    }

    @Test
    void ca4_an_origin_that_does_not_answer_gives_a_marker_when_the_author_chose_to_follow() throws Exception {
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, ProductMeetingPipeline.fakes(meetings),
                new RunMetrics(Map.of(), false), work);
                KnowledgeIngestPipeline bridge = new KnowledgeIngestPipeline(pipeline.blobs,
                        new FakeKnowledgeModel(Corpus.loadKnowledge().base().glossary()))) {
            CloudEvent linked = linked(pipeline, bridge);
            bridge.resolveDown(true);

            InstanceSnapshot done = bridge.resolve(linked, Duration.ofSeconds(60));

            assertThat(done.status()).as("at %s", done.position()).isEqualTo(InstanceStatus.COMPLETED);
            JsonNode report = data(last(bridge.emitted(), RESOLVED));
            assertThat(report.path("resolution").asText()).isEqualTo("unavailable");
            assertThat(report.path("viewRef").isNull()).isTrue();
        }
    }

    @Test
    void ca4_an_origin_that_does_not_answer_ends_the_instance_when_the_author_chose_to_suspend() throws Exception {
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, ProductMeetingPipeline.fakes(meetings),
                new RunMetrics(Map.of(), false), work);
                KnowledgeIngestPipeline bridge = new KnowledgeIngestPipeline(pipeline.blobs,
                        new FakeKnowledgeModel(Corpus.loadKnowledge().base().glossary()))) {
            CloudEvent linked = linked(pipeline, bridge);
            bridge.resolveDown(true);
            CloudEvent suspend = CloudEventBuilder.v1(linked).withId("linked-suspend")
                    .withData("application/json", Json.bytes(((ObjectNode) data(linked).deepCopy()).put("onUnavailable", "suspend")))
                    .build();

            InstanceSnapshot done = bridge.resolve(suspend, Duration.ofSeconds(60));

            assertThat(done.status()).isNotEqualTo(InstanceStatus.COMPLETED);
            assertThat(bridge.emitted().stream().filter(e -> e.getType().equals(RESOLVED))).isEmpty();
        }
    }
}
