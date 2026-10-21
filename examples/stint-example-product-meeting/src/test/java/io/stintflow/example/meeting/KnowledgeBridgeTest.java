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

import io.cloudevents.CloudEvent;
import io.stintflow.example.meeting.Corpus.Meeting;
import io.stintflow.knowledge.ChangeEntry;
import io.stintflow.knowledge.GraphPointer;
import io.stintflow.knowledge.GraphPointers;
import io.stintflow.knowledge.GraphView;
import io.stintflow.knowledge.NodeView;
import io.stintflow.knowledge.ViewQuery;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.Sensitivity;
import io.stintflow.wire.CeWire;
import io.stintflow.wire.Json;

/**
 * SDD 3c.1, CA1, CA2 and CA10 — the milestone of the sub-phase: a meeting is ingested by the product domain, which announces the
 * nodes it added (pointers only), and the architecture domain links to them (references only), with none of what the nodes say on
 * the other side. Fake models, no sleep; synthetic data only.
 */
class KnowledgeBridgeTest {

    static final String INDEXED = "io.stintflow.knowledge.document-indexed.v1";
    static final String PUBLISHED = "io.stintflow.knowledge.nodes-published.v1";
    static final String LINKED = "io.stintflow.knowledge.references-linked.v1";

    @TempDir
    Path work;

    private final List<Meeting> meetings = Corpus.load();

    private static CloudEvent fact(List<CloudEvent> emitted, String type) {
        return emitted.stream().filter(e -> e.getType().equals(type)).reduce((first, second) -> second).orElseThrow();
    }

    private static JsonNode data(CloudEvent event) {
        return Json.read(event.getData().toBytes());
    }

    @Test
    void ca10_a_meeting_is_ingested_announced_and_linked_and_the_other_domain_holds_pointers_only() throws Exception {
        List<String> markers = Corpus.allMarkers(meetings);
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, ProductMeetingPipeline.fakes(meetings),
                new RunMetrics(Map.of(), false), work);
                KnowledgeIngestPipeline bridge = new KnowledgeIngestPipeline(pipeline.blobs,
                        new FakeKnowledgeModel(Corpus.loadKnowledge().base().glossary()))) {
            Meeting meeting = meetings.getFirst();
            pipeline.run(meeting, Duration.ofSeconds(60));
            CloudEvent published = pipeline.channel.published().stream().filter(e -> e.getType().endsWith("minutes-published.v1"))
                    .filter(e -> meeting.id().equals(e.getSubject())).findFirst().orElseThrow();
            assertThat(bridge.ingest(published, Duration.ofSeconds(60)).status()).isEqualTo(InstanceStatus.COMPLETED);

            // the product domain announces what the document added
            InstanceSnapshot announced = bridge.announce(fact(bridge.emitted(), INDEXED), Duration.ofSeconds(60));
            assertThat(announced.status()).isEqualTo(InstanceStatus.COMPLETED);
            CloudEvent nodes = fact(bridge.emitted(), PUBLISHED);
            JsonNode announcement = data(nodes);
            assertThat(announcement.path("domain").path("base").asText()).isEqualTo("product-meetings");
            assertThat(announcement.path("documentId").asText()).isEqualTo("meeting:" + meeting.id());
            assertThat(announcement.path("items")).isNotEmpty();
            assertThat(announcement.path("items").findValuesAsText("type")).isSubsetOf("Decision", "Requirement", "System");
            // pointers, types and levels: nothing else, and the version is the node's in the log of the origin
            List<ChangeEntry> log = bridge.store.changes(KnowledgeIngestPipeline.SCOPE, 0, 10_000).toCompletableFuture().join();
            for (JsonNode item : announcement.path("items")) {
                assertThat(item.fieldNames()).toIterable().containsExactlyInAnyOrder("pointer", "type", "level");
                GraphPointer pointer = GraphPointer.parse(item.path("pointer").asText());
                assertThat(pointer.scope()).isEqualTo(KnowledgeIngestPipeline.SCOPE);
                assertThat(GraphPointers.versionOf(log, pointer.nodeId())).hasValue(pointer.version());
                assertThat(item.path("level").asText()).isIn("internal", "confidential");
            }

            // the architecture domain links to them
            InstanceSnapshot linked = bridge.link(nodes, Duration.ofSeconds(60));
            assertThat(linked.status()).as("at %s", linked.position()).isEqualTo(InstanceStatus.COMPLETED);
            JsonNode done = data(fact(bridge.emitted(), LINKED));
            assertThat(done.path("status").asText()).isEqualTo("APPLIED");
            assertThat(done.path("references").asInt()).isEqualTo(announcement.path("items").size());

            GraphView view = bridge.architecture.view(KnowledgeIngestPipeline.ARCHITECTURE,
                    new ViewQuery(Sensitivity.RESTRICTED, 500, null, null)).toCompletableFuture().join();
            List<NodeView> references = view.nodes().stream().filter(n -> n.type().equals("DomainReference")).toList();
            assertThat(references).hasSize(announcement.path("items").size());
            assertThat(view.nodes()).extracting(NodeView::type).contains("System");
            assertThat(view.edges()).hasSize(references.size()).allSatisfy(e -> assertThat(e.type()).isEqualTo("informs"));

            // nothing the origin says crossed the bridge: not the text of its passages, nor any name of its nodes
            String archDump = bridge.architecture.dump(KnowledgeIngestPipeline.ARCHITECTURE);
            var origin = bridge.store.view(KnowledgeIngestPipeline.SCOPE, new ViewQuery(Sensitivity.CONFIDENTIAL, 500, null, null))
                    .toCompletableFuture().join();
            for (NodeView named : origin.nodes()) {
                // a Meeting is named by the id of its document, which the reference batch keeps on purpose; the anchor is a System of its own
                if (named.label() != null && !named.type().equals("System") && !named.type().equals("Meeting")) {
                    assertThat(archDump).as("the name of a %s", named.type()).doesNotContain(named.label());
                }
            }
            List<String> places = new ArrayList<>();
            places.add("fact: " + new String(CeWire.toJson(nodes), StandardCharsets.UTF_8));
            places.add("linked: " + new String(CeWire.toJson(fact(bridge.emitted(), LINKED)), StandardCharsets.UTF_8));
            places.add("architecture: " + archDump);
            places.add("audit: " + bridge.audit.of(bridge.current).toCompletableFuture().join());
            places.add("state: " + bridge.state.load(bridge.current).toCompletableFuture().join());
            assertThat(places).allSatisfy(place -> markers.forEach(marker -> assertThat(place).doesNotContain(marker)));
        }
    }

    @Test
    void a_document_that_added_no_announced_node_announces_nothing() throws Exception {
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, ProductMeetingPipeline.fakes(meetings),
                new RunMetrics(Map.of(), false), work);
                KnowledgeIngestPipeline bridge = new KnowledgeIngestPipeline(pipeline.blobs,
                        new FakeKnowledgeModel(Corpus.loadKnowledge().base().glossary()))) {
            Meeting meeting = meetings.getFirst();
            pipeline.run(meeting, Duration.ofSeconds(60));
            bridge.ingest(pipeline.channel.published().stream().filter(e -> e.getType().endsWith("minutes-published.v1"))
                    .filter(e -> meeting.id().equals(e.getSubject())).findFirst().orElseThrow(), Duration.ofSeconds(60));
            CloudEvent indexed = fact(bridge.emitted(), INDEXED);
            // the same fact about a document the base never held
            JsonNode other = data(indexed);
            CloudEvent about = io.cloudevents.core.builder.CloudEventBuilder.v1(indexed)
                    .withId("indexed-other").withSubject("m-none")
                    .withData("application/json", Json.bytes(((com.fasterxml.jackson.databind.node.ObjectNode) other.deepCopy())
                            .put("documentId", "meeting:m-none"))).build();

            InstanceSnapshot done = bridge.announce(about, Duration.ofSeconds(60));

            assertThat(done.status()).isEqualTo(InstanceStatus.COMPLETED);
            assertThat(bridge.emitted().stream().filter(e -> e.getType().equals(PUBLISHED))).isEmpty();
        }
    }
}
