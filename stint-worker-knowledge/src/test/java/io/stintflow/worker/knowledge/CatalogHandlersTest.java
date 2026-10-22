package io.stintflow.worker.knowledge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryAuditLog;
import io.stintflow.knowledge.CatalogSeed;
import io.stintflow.knowledge.DeterministicEmbedder;
import io.stintflow.knowledge.Entity;
import io.stintflow.knowledge.GraphPointer;
import io.stintflow.knowledge.GraphPointers;
import io.stintflow.knowledge.GraphView;
import io.stintflow.knowledge.KnowledgeBatch;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.PassageIds;
import io.stintflow.knowledge.Source;
import io.stintflow.knowledge.ViewQuery;
import io.stintflow.knowledge.extraction.Glossary;
import io.stintflow.knowledge.extraction.Glossary.Term;
import io.stintflow.knowledge.inmemory.CompositeKnowledgeStore;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.RetentionClass;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;

/**
 * SDD 3c.3: the catalog answers by exact name only, to the definitions it lists, with the pointer and nothing else; an area names
 * its own nodes and links them to the catalog by pointer, without changing them and without copying the catalog. Synthetic data
 * only; the names, aliases and definitions of the catalog must never be in an answer, an audit record or the base of an area.
 */
class CatalogHandlersTest {

    static final KnowledgeScope CATALOG = KnowledgeScope.named("examples", "catalog");
    static final KnowledgeScope AREA = KnowledgeScope.named("examples", "architecture");
    static final WorkflowRef ASKER = new WorkflowRef("examples", "catalog-link", "1.0.0");
    static final Instant T0 = Instant.parse("2026-10-22T10:00:00Z");

    CompositeKnowledgeStore catalog;
    CompositeKnowledgeStore area;
    InMemoryAuditLog audit;

    private static Glossary glossary(int revision) {
        return new Glossary("catalog", Integer.toString(revision), List.of(
                new Term("sys-checkout", "System", "SYNTH-Checkout Service",
                        revision == 1 ? List.of("synth checkout", "synth-checkout svc") : List.of("synth checkout", "synth-checkout svc", "synth checkout-service"),
                        "SYNTH-takes the orders"),
                new Term("sys-search", "System", "SYNTH-Search Cluster", List.of("synth search service"), null),
                new Term("prod-shop", "Product", "SYNTH-Shop", List.of(), null)));
    }

    @BeforeEach
    void setUp() throws Exception {
        var blobs = new FilesystemBlobStore(Files.createTempDirectory("stint-3c3"), () -> T0);
        RetentionClass restricted = new RetentionClass("restricted", Duration.ofDays(30));
        catalog = new CompositeKnowledgeStore(new DeterministicEmbedder(), blobs, restricted, InstantSource.system());
        area = new CompositeKnowledgeStore(new DeterministicEmbedder(), blobs, restricted, InstantSource.system());
        audit = new InMemoryAuditLog();
        catalog.write(CATALOG, CatalogSeed.of(glossary(1), 1)).toCompletableFuture().join();
    }

    private CatalogLookupHandler lookup(String... requesters) {
        return new CatalogLookupHandler(catalog, new ScopeAllowList(Set.of(CATALOG)), new CatalogRequesters(Set.of(requesters)), audit,
                InstantSource.fixed(T0));
    }

    private TaskContext task(WorkflowRef definition, ObjectNode input) {
        return new TaskContext("/do/1/catalog", "i-1", "c-1", 1, input, definition, "chn-1", Lineage.NONE, null,
                Sensitivity.INTERNAL, null);
    }

    private static ObjectNode mentions(KnowledgeScope scope, String... typeAndName) {
        ObjectNode in = Json.obj();
        in.putObject("scope").put("namespace", scope.namespace()).put("base", scope.base());
        ArrayNode list = in.putArray("mentions");
        for (int i = 0; i < typeAndName.length; i += 2) {
            list.addObject().put("type", typeAndName[i]).put("name", typeAndName[i + 1]);
        }
        return in;
    }

    private JsonNode run(CatalogLookupHandler handler, TaskContext ctx) {
        return handler.execute(ctx).toCompletableFuture().join();
    }

    private static List<String> statuses(JsonNode answer) {
        List<String> all = new ArrayList<>();
        answer.path("results").forEach(r -> all.add(r.path("status").asText()));
        return all;
    }

    // --- catalog-lookup ----------------------------------------------------------------------------------------------

    @Test
    void ca2_a_name_or_an_alias_is_a_match_with_the_pointer_and_nothing_the_catalog_says_of_the_node() {
        JsonNode answer = run(lookup("examples:catalog-link"), task(ASKER, mentions(CATALOG,
                "System", "synth checkout", "System", "SYNTH-CHECKOUT SERVICE", "System", "synth-checkout svc")));

        assertThat(statuses(answer)).containsExactly("MATCH", "MATCH", "MATCH");
        GraphPointer pointer = GraphPointer.parse(answer.path("results").get(0).path("pointer").asText());
        assertThat(pointer.scope()).isEqualTo(CATALOG);
        assertThat(pointer.nodeId()).isEqualTo("sys-checkout");
        assertThat(pointer.version()).isEqualTo(GraphPointers.versionOf(
                catalog.changes(CATALOG, 0, 1000).toCompletableFuture().join(), "sys-checkout").orElseThrow());
        assertThat(answer.path("counts").path("MATCH").asInt()).isEqualTo(3);
        assertThat(answer.toString()).doesNotContain("Checkout").doesNotContain("takes the orders").doesNotContain("svc");
    }

    @Test
    void ca3_and_ca4_what_the_catalog_does_not_know_is_none_and_a_likeness_is_never_a_match() {
        JsonNode answer = run(lookup("examples:catalog-link"), task(ASKER, mentions(CATALOG,
                "System", "SYNTH-Billing", "System", "synth checkouts", "System", "SYNTH-Checkout Service 2",
                "System", "synth chekout", "Product", "SYNTH-Checkout Service")));

        assertThat(statuses(answer)).containsOnly("NONE").hasSize(5);
        assertThat(answer.path("results").get(0).has("pointer")).isFalse();
        assertThat(answer.path("counts").path("NONE").asInt()).isEqualTo(5);
    }

    @Test
    void ca5_a_definition_that_is_not_listed_and_a_base_that_is_not_served_answer_as_a_catalog_that_knows_nothing() {
        ObjectNode known = mentions(CATALOG, "System", "synth checkout", "System", "synth search service");
        JsonNode nothingKnown = run(lookup("examples:catalog-link"), task(ASKER, mentions(CATALOG, "System", "SYNTH-No", "System", "SYNTH-Nope")));

        JsonNode notListed = run(lookup("examples:other"), task(ASKER, known));
        JsonNode noList = run(lookup(), task(ASKER, known));
        JsonNode otherBase = run(lookup("examples:catalog-link"), task(ASKER, mentions(KnowledgeScope.named("examples", "elsewhere"),
                "System", "synth checkout", "System", "synth search service")));
        ObjectNode forged = known.deepCopy();
        forged.put("requester", "examples:catalog-link");
        JsonNode withRequesterField = run(lookup("examples:other"), task(ASKER, forged));

        assertThat(notListed).isEqualTo(nothingKnown);
        assertThat(noList).isEqualTo(nothingKnown);
        assertThat(otherBase).isEqualTo(nothingKnown);
        assertThat(withRequesterField).isEqualTo(nothingKnown);
    }

    @Test
    void a_mention_that_is_not_a_mention_refuses_the_batch_and_the_message_does_not_echo_the_name() {
        CatalogLookupHandler handler = lookup("examples:catalog-link");

        assertThatThrownBy(() -> run(handler, task(ASKER, mentions(CATALOG, "Person", "SYNTH-PII-Ana Souza"))))
                .hasCauseInstanceOf(TaskFailure.class).hasMessageNotContaining("SYNTH-PII").hasMessageNotContaining("Souza");
        assertThatThrownBy(() -> run(handler, task(ASKER, mentions(CATALOG, "System", " "))))
                .hasCauseInstanceOf(TaskFailure.class);
        ObjectNode many = Json.obj();
        many.putObject("scope").put("namespace", "examples").put("base", "catalog");
        ArrayNode list = many.putArray("mentions");
        for (int i = 0; i < 51; i++) {
            list.addObject().put("type", "System").put("name", "n" + i);
        }
        assertThatThrownBy(() -> run(handler, task(ASKER, many))).hasCauseInstanceOf(TaskFailure.class);
        assertThatThrownBy(() -> run(handler, task(ASKER, Json.obj()))).hasCauseInstanceOf(TaskFailure.class);
    }

    @Test
    void ca9_a_removed_entry_is_none_and_a_changed_one_answers_its_new_version() {
        CatalogLookupHandler handler = lookup("examples:catalog-link");
        long before = GraphPointer.parse(run(handler, task(ASKER, mentions(CATALOG, "System", "synth checkout")))
                .path("results").get(0).path("pointer").asText()).version();

        catalog.write(CATALOG, CatalogSeed.of(new Glossary("catalog", "2", List.of(glossary(2).byId("sys-checkout").orElseThrow(),
                glossary(2).byId("prod-shop").orElseThrow())), 2)).toCompletableFuture().join();

        JsonNode answer = run(handler, task(ASKER, mentions(CATALOG, "System", "synth checkout-service", "System", "synth search service")));
        assertThat(statuses(answer)).containsExactly("MATCH", "NONE");
        assertThat(GraphPointer.parse(answer.path("results").get(0).path("pointer").asText()).version()).isGreaterThan(before);
    }

    @Test
    void ca12_one_record_per_call_with_the_requester_and_the_counts_and_no_name() {
        run(lookup("examples:catalog-link"), task(ASKER, mentions(CATALOG, "System", "synth checkout", "System", "SYNTH-No")));

        var records = audit.of("i-1").toCompletableFuture().join();
        assertThat(records).singleElement().satisfies(r -> {
            assertThat(r.kind()).isEqualTo("catalog-lookup");
            assertThat(r.detail()).containsEntry("requester", "examples:catalog-link").containsEntry("MATCH", "1")
                    .containsEntry("NONE", "1").containsEntry("base", "examples/catalog");
            assertThat(r.toString()).doesNotContain("SYNTH-No").doesNotContain("checkout");
        });
    }

    // --- catalog-mentions and catalog-link --------------------------------------------------------------------------

    private static KnowledgeBatch areaDoc(String doc, long revision, List<Entity> entities) {
        return new KnowledgeBatch("extract:" + doc, "doc:" + doc, PassageIds.sha256Hex(entities.toString()), revision,
                new Source("i-" + doc, "chn-" + doc, new WorkflowRef("examples", "knowledge-ingest", "1.0.0")),
                "architecture@1.0.0", List.of(), entities, List.of(), List.of());
    }

    private void areaWith(Entity... entities) {
        area.write(AREA, areaDoc("d-1", 1, List.of(entities))).toCompletableFuture().join();
    }

    private CatalogMentionsHandler mentionsHandler() {
        return new CatalogMentionsHandler(area, new ScopeAllowList(Set.of(AREA)));
    }

    private CatalogLinkHandler linkHandler(Set<KnowledgeScope> origins) {
        return new CatalogLinkHandler(area, area, new ScopeAllowList(Set.of(AREA)), new OriginAllowList(origins), audit,
                InstantSource.fixed(T0));
    }

    private static ObjectNode areaInput() {
        ObjectNode in = Json.obj();
        in.putObject("scope").put("namespace", AREA.namespace()).put("base", AREA.base());
        return in;
    }

    @Test
    void ca6_the_mentions_are_the_internal_nodes_of_the_types_of_the_catalog_and_what_is_above_is_only_counted() {
        areaWith(new Entity("e-1", "System", "SYNTH-checkout", Sensitivity.INTERNAL),
                new Entity("e-2", "Product", "SYNTH-Shop front", Sensitivity.INTERNAL),
                new Entity("e-3", "Team", "SYNTH-Board", Sensitivity.CONFIDENTIAL),
                new Entity("e-4", "Person", "SYNTH-PII-0404", Sensitivity.RESTRICTED),
                new Entity("e-5", "Decision", "SYNTH-Use a queue", Sensitivity.INTERNAL));

        JsonNode answer = mentionsHandler().execute(task(ASKER, areaInput())).toCompletableFuture().join();

        assertThat(answer.path("mentions")).hasSize(2);
        assertThat(answer.path("mentions").get(0).path("entityId").asText()).isEqualTo("e-1");
        assertThat(answer.path("mentions").get(0).path("name").asText()).isEqualTo("SYNTH-checkout");
        assertThat(answer.path("skipped").path("confidential").asInt()).isEqualTo(1);
        assertThat(answer.toString()).doesNotContain("SYNTH-Board").doesNotContain("SYNTH-PII").doesNotContain("queue");
        assertThat(answer.path("next").isNull()).isTrue();
    }

    @Test
    void the_mentions_come_in_pages_of_fifty() {
        List<Entity> many = new ArrayList<>();
        for (int i = 0; i < 60; i++) {
            many.add(new Entity(String.format("e-%03d", i), "System", "SYNTH-system " + i, Sensitivity.INTERNAL));
        }
        areaWith(many.toArray(new Entity[0]));

        JsonNode first = mentionsHandler().execute(task(ASKER, areaInput())).toCompletableFuture().join();
        ObjectNode again = areaInput();
        again.put("after", first.path("next").asText());
        JsonNode second = mentionsHandler().execute(task(ASKER, again)).toCompletableFuture().join();

        assertThat(first.path("mentions")).hasSize(50);
        assertThat(first.path("next").asText()).isEqualTo("e-049");
        assertThat(second.path("mentions")).hasSize(10);
        assertThat(second.path("next").isNull()).isTrue();
    }

    private ObjectNode links(String documentId, String... entityAndPointer) {
        ObjectNode in = areaInput();
        in.put("documentId", documentId);
        ArrayNode list = in.putArray("links");
        for (int i = 0; i < entityAndPointer.length; i += 2) {
            list.addObject().put("entityId", entityAndPointer[i]).put("pointer", entityAndPointer[i + 1]);
        }
        return in;
    }

    private String pointerTo(String id) {
        return GraphPointer.of(CATALOG, id, GraphPointers.versionOf(
                catalog.changes(CATALOG, 0, 1000).toCompletableFuture().join(), id).orElseThrow()).format();
    }

    @Test
    void ca2_the_local_node_denotes_the_reference_and_nothing_of_the_catalog_is_copied() {
        areaWith(new Entity("e-1", "System", "SYNTH-checkout", Sensitivity.INTERNAL));

        JsonNode answer = linkHandler(Set.of(CATALOG)).execute(task(ASKER, links("d-1", "e-1", pointerTo("sys-checkout"))))
                .toCompletableFuture().join();

        assertThat(answer.path("status").asText()).isEqualTo("APPLIED");
        assertThat(answer.path("linked").asInt()).isEqualTo(1);
        GraphView view = area.view(AREA, new ViewQuery(Sensitivity.CONFIDENTIAL, 100, null, null)).toCompletableFuture().join();
        assertThat(view.nodes()).extracting(n -> n.type()).containsExactlyInAnyOrder("System", "DomainReference");
        assertThat(view.edges()).singleElement().satisfies(edge -> {
            assertThat(edge.type()).isEqualTo("denotes");
            assertThat(edge.from()).isEqualTo("e-1");
        });
        assertThat(view.toString()).doesNotContain("takes the orders").doesNotContain("synth-checkout svc").doesNotContain("Checkout Service");
        assertThat(area.changes(AREA, 0, 1000).toCompletableFuture().join()).allSatisfy(entry ->
                assertThat(entry.toString()).doesNotContain("takes the orders").doesNotContain("synth-checkout svc"));
    }

    @Test
    void ca2_two_areas_that_name_the_same_system_differently_point_at_the_same_reference() {
        CompositeKnowledgeStore other = new CompositeKnowledgeStore(new DeterministicEmbedder(),
                new FilesystemBlobStore(tempDir(), () -> T0), new RetentionClass("restricted", Duration.ofDays(30)), InstantSource.system());
        KnowledgeScope product = KnowledgeScope.named("examples", "product-meetings");
        areaWith(new Entity("e-hash", "System", "SYNTH-Checkout Service", Sensitivity.INTERNAL));
        other.write(product, areaDoc("d-2", 1, List.of(new Entity("sys-checkout-local", "System", "synth checkout", Sensitivity.INTERNAL))))
                .toCompletableFuture().join();
        String pointer = pointerTo("sys-checkout");

        linkHandler(Set.of(CATALOG)).execute(task(ASKER, links("d-1", "e-hash", pointer))).toCompletableFuture().join();
        new CatalogLinkHandler(other, other, new ScopeAllowList(Set.of(product)), new OriginAllowList(Set.of(CATALOG)), audit,
                InstantSource.fixed(T0)).execute(task(ASKER, productLinks(product, pointer))).toCompletableFuture().join();

        var inArea = area.view(AREA, new ViewQuery(Sensitivity.INTERNAL, 100, null, "DomainReference")).toCompletableFuture().join();
        var inProduct = other.view(product, new ViewQuery(Sensitivity.INTERNAL, 100, null, "DomainReference")).toCompletableFuture().join();
        assertThat(inArea.nodes()).hasSize(1);
        assertThat(inProduct.nodes()).extracting(n -> n.id()).isEqualTo(inArea.nodes().stream().map(n -> n.id()).toList());
    }

    private java.nio.file.Path tempDir() {
        try {
            return Files.createTempDirectory("stint-3c3-b");
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static ObjectNode productLinks(KnowledgeScope scope, String pointer) {
        ObjectNode in = Json.obj();
        in.putObject("scope").put("namespace", scope.namespace()).put("base", scope.base());
        in.put("documentId", "d-2");
        in.putArray("links").addObject().put("entityId", "sys-checkout-local").put("pointer", pointer);
        return in;
    }

    @Test
    void the_same_link_again_writes_nothing_and_a_newer_catalog_version_replaces_the_reference() {
        areaWith(new Entity("e-1", "System", "SYNTH-checkout", Sensitivity.INTERNAL));
        CatalogLinkHandler handler = linkHandler(Set.of(CATALOG));
        handler.execute(task(ASKER, links("d-1", "e-1", pointerTo("sys-checkout")))).toCompletableFuture().join();
        long last = area.changes(AREA, 0, 1000).toCompletableFuture().join().getLast().sequence();

        JsonNode again = handler.execute(task(ASKER, links("d-1", "e-1", pointerTo("sys-checkout")))).toCompletableFuture().join();
        assertThat(again.path("status").asText()).isEqualTo("UNCHANGED");
        assertThat(area.changes(AREA, 0, 1000).toCompletableFuture().join().getLast().sequence()).isEqualTo(last);

        catalog.write(CATALOG, CatalogSeed.of(glossary(2), 2)).toCompletableFuture().join();
        JsonNode moved = handler.execute(task(ASKER, links("d-1", "e-1", pointerTo("sys-checkout")))).toCompletableFuture().join();
        assertThat(moved.path("status").asText()).isEqualTo("APPLIED");
    }

    @Test
    void a_link_never_creates_a_node_and_skips_what_is_not_a_catalog_type_or_is_above_internal() {
        areaWith(new Entity("e-dec", "Decision", "SYNTH-Use a queue", Sensitivity.INTERNAL),
                new Entity("e-conf", "System", "SYNTH-Confidential system", Sensitivity.CONFIDENTIAL),
                new Entity("e-ok", "System", "SYNTH-ok", Sensitivity.INTERNAL));
        String pointer = pointerTo("sys-checkout");

        JsonNode answer = linkHandler(Set.of(CATALOG)).execute(task(ASKER, links("d-1", "e-dec", pointer, "e-conf", pointer,
                "e-missing", pointer, "e-ok", pointer))).toCompletableFuture().join();

        assertThat(answer.path("linked").asInt()).isEqualTo(1);
        assertThat(answer.path("skipped").asInt()).isEqualTo(3);
        assertThat(area.view(AREA, new ViewQuery(Sensitivity.CONFIDENTIAL, 100, null, "System")).toCompletableFuture().join().nodes())
                .extracting(n -> n.id()).containsExactlyInAnyOrder("e-conf", "e-ok");
    }

    @Test
    void a_pointer_of_a_domain_not_listed_or_not_well_formed_refuses_the_whole_call_and_writes_nothing() {
        areaWith(new Entity("e-1", "System", "SYNTH-checkout", Sensitivity.INTERNAL));
        long last = area.changes(AREA, 0, 1000).toCompletableFuture().join().getLast().sequence();

        assertThatThrownBy(() -> linkHandler(Set.of()).execute(task(ASKER, links("d-1", "e-1", pointerTo("sys-checkout"))))
                .toCompletableFuture().join()).hasCauseInstanceOf(TaskFailure.class);
        assertThatThrownBy(() -> linkHandler(Set.of(CATALOG)).execute(task(ASKER, links("d-1", "e-1", "SYNTH-not-a-pointer")))
                .toCompletableFuture().join()).hasCauseInstanceOf(TaskFailure.class).hasMessageNotContaining("SYNTH-not-a-pointer");

        assertThat(area.changes(AREA, 0, 1000).toCompletableFuture().join().getLast().sequence()).isEqualTo(last);
    }

    @Test
    void ca12_the_link_leaves_one_record_with_counts_and_no_name() {
        areaWith(new Entity("e-1", "System", "SYNTH-checkout", Sensitivity.INTERNAL));

        linkHandler(Set.of(CATALOG)).execute(task(ASKER, links("d-1", "e-1", pointerTo("sys-checkout")))).toCompletableFuture().join();

        assertThat(audit.of("i-1").toCompletableFuture().join()).singleElement().satisfies(r -> {
            assertThat(r.kind()).isEqualTo("catalog-link");
            assertThat(r.detail()).containsEntry("linked", "1").containsEntry("skipped", "0");
            assertThat(r.toString()).doesNotContain("SYNTH-checkout").doesNotContain("sys-checkout");
        });
        assertThat(Map.of("base", AREA.key())).isNotEmpty();
    }
}
