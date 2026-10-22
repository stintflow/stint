package io.stintflow.example.meeting;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.stintflow.example.meeting.Corpus.Meeting;
import io.stintflow.knowledge.EntityIds;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.NodeView;
import io.stintflow.knowledge.ViewQuery;
import io.stintflow.knowledge.inmemory.CompositeKnowledgeStore;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.Sensitivity;
import io.stintflow.wire.CeWire;
import io.stintflow.wire.Json;

/**
 * SDD 3c.3, CA2, CA10, CA11 and CA13 — three domains in one engine: the product and the architecture domains name the same system
 * differently (a glossary id, a hash), each asks the catalog domain which node of the catalog its name is, and both end up pointing
 * at the same reference. Nothing the catalog says of the node — an alias, a definition — is copied; when the catalog does not
 * answer the author's choice (follow or suspend) decides. Fake models, no sleep; synthetic data only.
 */
class KnowledgeCatalogTest {

    static final String INDEXED = "io.stintflow.knowledge.document-indexed.v1";
    static final String PUBLISHED = "io.stintflow.knowledge.nodes-published.v1";
    static final String LINKED = "io.stintflow.knowledge.references-linked.v1";
    static final String CATALOG_LINKED = "io.stintflow.knowledge.catalog-linked.v1";
    /** The spelling only the catalog knows (an alias of {@code sys-checkout}). */
    static final String CATALOG_ONLY = "checkout-service";

    @TempDir
    Path work;

    private final List<Meeting> meetings = Corpus.load();

    private static CloudEvent last(List<CloudEvent> emitted, String type) {
        return emitted.stream().filter(e -> e.getType().equals(type)).reduce((first, second) -> second).orElseThrow();
    }

    private static JsonNode data(CloudEvent event) {
        return Json.read(event.getData().toBytes());
    }

    /** Product ingests a meeting and announces it; architecture links to the announcement: the two facts that start the catalog links. */
    private CloudEvent[] indexedAndLinked(ProductMeetingPipeline pipeline, KnowledgeIngestPipeline bridge) throws Exception {
        Meeting meeting = meetings.getFirst();
        pipeline.run(meeting, Duration.ofSeconds(60));
        bridge.ingest(pipeline.channel.published().stream().filter(e -> e.getType().endsWith("minutes-published.v1"))
                .filter(e -> meeting.id().equals(e.getSubject())).findFirst().orElseThrow(), Duration.ofSeconds(60));
        CloudEvent indexed = last(bridge.emitted(), INDEXED);
        bridge.announce(indexed, Duration.ofSeconds(60));
        InstanceSnapshot done = bridge.link(last(bridge.emitted(), PUBLISHED), Duration.ofSeconds(60));
        assertThat(done.status()).isEqualTo(InstanceStatus.COMPLETED);
        return new CloudEvent[] {indexed, last(bridge.emitted(), LINKED)};
    }

    /**
     * The meetings of the corpus are restricted at the door and their nodes come out {@code confidential}, which the catalog never
     * hears about (SDD 3c.3, 8b): so the product domain keeps one system of its own at {@code internal} — a legacy spelling the catalog
     * knows as an alias — for the link to find.
     */
    private static void productKeepsALegacySystem(KnowledgeIngestPipeline bridge) {
        var entities = List.of(new io.stintflow.knowledge.Entity("sys-checkout-legacy", "System", "checkout svc", Sensitivity.INTERNAL));
        bridge.store.write(KnowledgeIngestPipeline.SCOPE, new io.stintflow.knowledge.KnowledgeBatch("legacy:1", "glossary:legacy",
                io.stintflow.knowledge.PassageIds.sha256Hex(entities.toString()), 1L, io.stintflow.knowledge.Source.NONE, "meetings@1.0.0",
                List.of(), entities, List.of(), List.of())).toCompletableFuture().join();
    }

    private static List<NodeView> referencesOf(CompositeKnowledgeStore store, KnowledgeScope scope) {
        return store.view(scope, new ViewQuery(Sensitivity.CONFIDENTIAL, 1000, null, "DomainReference")).toCompletableFuture().join()
                .nodes().stream().filter(n -> n.label().contains(KnowledgeIngestPipeline.CATALOG.key())).toList();
    }

    private static CloudEvent again(CloudEvent fact, String id, String key, String value) {
        ObjectNode copy = ((ObjectNode) data(fact).deepCopy());
        if (key != null) {
            copy.put(key, value);
        }
        return CloudEventBuilder.v1(fact).withId(id).withData("application/json", Json.bytes(copy)).build();
    }

    @Test
    void ca13_the_two_areas_name_the_system_differently_and_both_point_at_the_same_reference_of_the_catalog() throws Exception {
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, ProductMeetingPipeline.fakes(meetings),
                new RunMetrics(Map.of(), false), work);
                KnowledgeIngestPipeline bridge = new KnowledgeIngestPipeline(pipeline.blobs,
                        new FakeKnowledgeModel(Corpus.loadKnowledge().base().glossary()))) {
            CloudEvent[] facts = indexedAndLinked(pipeline, bridge);
            productKeepsALegacySystem(bridge);

            InstanceSnapshot product = bridge.catalogLinkProduct(facts[0], Duration.ofSeconds(60));
            JsonNode productReport = data(last(bridge.emitted(), CATALOG_LINKED));
            InstanceSnapshot architecture = bridge.catalogLinkArchitecture(facts[1], Duration.ofSeconds(60));
            JsonNode architectureReport = data(last(bridge.emitted(), CATALOG_LINKED));

            assertThat(product.status()).as("product at %s", product.position()).isEqualTo(InstanceStatus.COMPLETED);
            assertThat(architecture.status()).as("architecture at %s", architecture.position()).isEqualTo(InstanceStatus.COMPLETED);
            assertThat(productReport.path("resolution").asText()).isEqualTo("resolved");
            assertThat(productReport.path("linked").asInt()).isEqualTo(1);   // the meeting's own system is confidential: skipped
            assertThat(architectureReport.path("linked").asInt()).isEqualTo(1);

            // the same system has two local ids; the references are one
            assertThat(EntityIds.of("System", "Checkout Service")).isNotEqualTo("sys-checkout");
            Set<String> inProduct = referencesOf(bridge.store, KnowledgeIngestPipeline.SCOPE).stream().map(NodeView::id)
                    .collect(Collectors.toSet());
            Set<String> inArchitecture = referencesOf(bridge.architecture, KnowledgeIngestPipeline.ARCHITECTURE).stream()
                    .map(NodeView::id).collect(Collectors.toSet());
            assertThat(inArchitecture).hasSize(1);
            assertThat(inProduct).containsAll(inArchitecture);
            assertThat(bridge.architecture.view(KnowledgeIngestPipeline.ARCHITECTURE, new ViewQuery(Sensitivity.CONFIDENTIAL, 1000, null, null))
                    .toCompletableFuture().join().edges()).anySatisfy(edge -> assertThat(edge.type()).isEqualTo("denotes"));
        }
    }

    @Test
    void ca11_nothing_the_catalog_says_of_the_node_is_in_the_areas_the_facts_the_state_or_the_audit() throws Exception {
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, ProductMeetingPipeline.fakes(meetings),
                new RunMetrics(Map.of(), false), work);
                KnowledgeIngestPipeline bridge = new KnowledgeIngestPipeline(pipeline.blobs,
                        new FakeKnowledgeModel(Corpus.loadKnowledge().base().glossary()))) {
            CloudEvent[] facts = indexedAndLinked(pipeline, bridge);
            productKeepsALegacySystem(bridge);
            bridge.catalogLinkProduct(facts[0], Duration.ofSeconds(60));
            bridge.catalogLinkArchitecture(facts[1], Duration.ofSeconds(60));

            List<String> places = List.of(
                    "architecture: " + bridge.architecture.dump(KnowledgeIngestPipeline.ARCHITECTURE),
                    "product: " + bridge.store.dump(KnowledgeIngestPipeline.SCOPE),
                    "fact: " + new String(CeWire.toJson(last(bridge.emitted(), CATALOG_LINKED)), StandardCharsets.UTF_8),
                    "state: " + bridge.state.load(bridge.current).toCompletableFuture().join(),
                    "audit: " + bridge.audit.of(bridge.current).toCompletableFuture().join());

            assertThat(places).allSatisfy(place -> assertThat(place).doesNotContain(CATALOG_ONLY).doesNotContain("Online Shop")
                    .doesNotContain("Payments Team"));
            assertThat(bridge.audit.of(bridge.current).toCompletableFuture().join()).anySatisfy(r -> {
                assertThat(r.kind()).isEqualTo("catalog-link");
                assertThat(r.detail()).containsKey("linked");
            });
        }
    }

    @Test
    void ca10_a_catalog_that_does_not_answer_gives_a_marker_with_follow_and_a_later_run_links() throws Exception {
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, ProductMeetingPipeline.fakes(meetings),
                new RunMetrics(Map.of(), false), work);
                KnowledgeIngestPipeline bridge = new KnowledgeIngestPipeline(pipeline.blobs,
                        new FakeKnowledgeModel(Corpus.loadKnowledge().base().glossary()))) {
            CloudEvent[] facts = indexedAndLinked(pipeline, bridge);
            bridge.catalogDown(true);

            InstanceSnapshot done = bridge.catalogLinkArchitecture(facts[1], Duration.ofSeconds(60));

            assertThat(done.status()).as("at %s", done.position()).isEqualTo(InstanceStatus.COMPLETED);
            JsonNode report = data(last(bridge.emitted(), CATALOG_LINKED));
            assertThat(report.path("resolution").asText()).isEqualTo("unavailable");
            assertThat(report.path("linked").asInt()).isZero();
            assertThat(referencesOf(bridge.architecture, KnowledgeIngestPipeline.ARCHITECTURE)).isEmpty();

            bridge.catalogDown(false);
            InstanceSnapshot retried = bridge.catalogLinkArchitecture(again(facts[1], "linked-retry", null, null), Duration.ofSeconds(60));

            assertThat(retried.status()).isEqualTo(InstanceStatus.COMPLETED);
            assertThat(data(last(bridge.emitted(), CATALOG_LINKED)).path("linked").asInt()).isEqualTo(1);
            assertThat(referencesOf(bridge.architecture, KnowledgeIngestPipeline.ARCHITECTURE)).hasSize(1);
        }
    }

    @Test
    void ca10_a_catalog_that_does_not_answer_ends_the_instance_when_the_author_chose_to_suspend() throws Exception {
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, ProductMeetingPipeline.fakes(meetings),
                new RunMetrics(Map.of(), false), work);
                KnowledgeIngestPipeline bridge = new KnowledgeIngestPipeline(pipeline.blobs,
                        new FakeKnowledgeModel(Corpus.loadKnowledge().base().glossary()))) {
            CloudEvent[] facts = indexedAndLinked(pipeline, bridge);
            bridge.catalogDown(true);
            long before = bridge.emitted().stream().filter(e -> e.getType().equals(CATALOG_LINKED)).count();

            InstanceSnapshot done = bridge.catalogLinkArchitecture(again(facts[1], "linked-suspend", "onUnavailable", "suspend"),
                    Duration.ofSeconds(60));

            assertThat(done.status()).isNotEqualTo(InstanceStatus.COMPLETED);
            assertThat(bridge.emitted().stream().filter(e -> e.getType().equals(CATALOG_LINKED)).count()).isEqualTo(before);
            assertThat(referencesOf(bridge.architecture, KnowledgeIngestPipeline.ARCHITECTURE)).isEmpty();
        }
    }
}
