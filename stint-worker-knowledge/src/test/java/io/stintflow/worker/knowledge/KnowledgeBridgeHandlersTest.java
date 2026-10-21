package io.stintflow.worker.knowledge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.inmemory.InMemoryAuditLog;
import io.stintflow.knowledge.DeleteRequest;
import io.stintflow.knowledge.DeterministicEmbedder;
import io.stintflow.knowledge.Entity;
import io.stintflow.knowledge.EntityIds;
import io.stintflow.knowledge.GraphPointer;
import io.stintflow.knowledge.GraphView;
import io.stintflow.knowledge.KnowledgeBatch;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.NodeView;
import io.stintflow.knowledge.PassageIds;
import io.stintflow.knowledge.Relation;
import io.stintflow.knowledge.Source;
import io.stintflow.knowledge.ViewQuery;
import io.stintflow.knowledge.inmemory.CompositeKnowledgeStore;
import io.stintflow.spi.AuditLog.AuditRecord;
import io.stintflow.spi.RetentionClass;
import io.stintflow.spi.Sensitivity;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskFailure;

/**
 * SDD 3c.1: the announcement of the nodes a document added (CA1, CA4, CA5) and the references another domain makes of it (CA2, CA3,
 * CA6, CA7, CA9). Synthetic data only; the names planted in the origin must never be found on the other side.
 */
class KnowledgeBridgeHandlersTest {

    static final Instant T0 = Instant.parse("2026-10-21T10:00:00Z");
    static final KnowledgeScope ARCH = KnowledgeScope.named("examples", "architecture");
    static final String OLD_NAME = "SYNTH-Old retry policy";
    static final String RESTRICTED_NAME = "SYNTH-PII-0042";

    KnowledgeFixture fx;
    CompositeKnowledgeStore arch;
    InMemoryAuditLog audit;
    KnowledgeAnnounceHandler announce;
    KnowledgeReferenceHandler reference;

    private static Entity node(String type, String name, Sensitivity level) {
        return new Entity(EntityIds.of(type, name), type, name, level);
    }

    private static KnowledgeBatch doc(String meeting, long revision, List<Entity> entities, List<Relation> relations) {
        return new KnowledgeBatch("extract:" + meeting, "meeting:" + meeting, PassageIds.sha256Hex(entities.toString() + relations),
                revision, new Source("i-" + meeting, "chn-" + meeting, KnowledgeFixture.DEFINITION), "meetings@1.0.0", List.of(),
                entities, relations, List.of());
    }

    @BeforeEach
    void setUp() throws Exception {
        fx = new KnowledgeFixture();
        arch = new CompositeKnowledgeStore(new DeterministicEmbedder(), fx.blobs, new RetentionClass("restricted", Duration.ofDays(30)),
                InstantSource.system());
        audit = new InMemoryAuditLog();
        ScopeAllowList scopes = new ScopeAllowList(Set.of(KnowledgeFixture.SCOPE, ARCH));
        announce = new KnowledgeAnnounceHandler(fx.store, scopes, audit, InstantSource.fixed(T0));
        reference = new KnowledgeReferenceHandler(arch, scopes, new OriginAllowList(Set.of(KnowledgeFixture.SCOPE)), audit,
                InstantSource.fixed(T0));
        fx.store.write(KnowledgeFixture.SCOPE, doc("m-001", 1, List.of(node("Decision", OLD_NAME, Sensitivity.CONFIDENTIAL),
                node("Requirement", "SYNTH-Idempotent writes", Sensitivity.INTERNAL),
                node("Decision", "SYNTH-Hold the budget", Sensitivity.RESTRICTED),
                node("System", "SYNTH-Checkout Service", Sensitivity.INTERNAL),
                node("Person", RESTRICTED_NAME, Sensitivity.RESTRICTED)), List.of())).toCompletableFuture().join();
    }

    private ObjectNode announceInput(String documentId) {
        ObjectNode in = Json.obj();
        in.put("documentId", documentId);
        in.putArray("types").add("Decision").add("Requirement");
        in.set("scope", KnowledgeFixture.scopeNode());
        return in;
    }

    private JsonNode announced(ObjectNode in) {
        return announce.execute(KnowledgeFixture.task("i-a", in, Sensitivity.CONFIDENTIAL)).toCompletableFuture().join();
    }

    private static ObjectNode archScope(ObjectNode in) {
        ObjectNode scope = Json.obj();
        scope.put("namespace", ARCH.namespace()).put("base", ARCH.base());
        in.set("scope", scope);
        return in;
    }

    /** The event a workflow would hand to {@code knowledge-reference}, from what {@code knowledge-announce} answered. */
    private ObjectNode referenceInput(JsonNode answer, String documentId) {
        ObjectNode event = Json.obj();
        event.putObject("domain").put("namespace", KnowledgeFixture.SCOPE.namespace()).put("base", KnowledgeFixture.SCOPE.base());
        event.put("documentId", documentId).put("upTo", answer.get("upTo").asLong());
        event.set("items", answer.get("items"));
        ObjectNode in = Json.obj();
        in.set("event", event);
        in.putArray("anchors").addObject().put("type", "System").put("name", "Checkout Service");
        return archScope(in);
    }

    private JsonNode referenced(ObjectNode in) {
        return reference.execute(KnowledgeFixture.task("i-r", in, Sensitivity.CONFIDENTIAL)).toCompletableFuture().join();
    }

    private static int status(Throwable thrown) {
        return ((TaskFailure) thrown.getCause()).toErrorInfo().status();
    }

    private GraphView archView() {
        return arch.view(ARCH, new ViewQuery(Sensitivity.RESTRICTED, 100, null, null)).toCompletableFuture().join();
    }

    // --- the announcement --------------------------------------------------------------------------------------------------

    @Test
    void ca1_the_announcement_has_pointers_types_and_levels_and_nothing_else() {
        JsonNode answer = announced(announceInput("meeting:m-001"));

        assertThat(answer.get("items")).hasSize(2);   // the confidential decision and the internal requirement
        assertThat(answer.get("withheld").asInt()).isEqualTo(1);   // the restricted decision
        for (JsonNode item : answer.get("items")) {
            GraphPointer pointer = GraphPointer.parse(item.get("pointer").asText());
            assertThat(pointer.scope()).isEqualTo(KnowledgeFixture.SCOPE);
            assertThat(item.fieldNames()).toIterable().containsExactlyInAnyOrder("pointer", "type", "level");
        }
        assertThat(answer.get("items").findValuesAsText("type")).containsExactlyInAnyOrder("Decision", "Requirement");
        assertThat(answer.toString()).doesNotContain("SYNTH", "retry", "Idempotent", "budget");
        assertThat(answer.get("upTo").asLong()).isEqualTo(fx.store.changes(KnowledgeFixture.SCOPE, 0, 1000).toCompletableFuture()
                .join().getLast().sequence());
    }

    @Test
    void ca4_a_restricted_node_is_never_announced_and_the_ceiling_only_lowers() {
        ObjectNode internalOnly = announceInput("meeting:m-001");
        internalOnly.put("maxLevel", "internal");
        JsonNode lowered = announced(internalOnly);

        assertThat(lowered.get("items")).hasSize(1);
        assertThat(lowered.get("items").get(0).get("level").asText()).isEqualTo("internal");
        assertThat(lowered.get("withheld").asInt()).isEqualTo(2);

        ObjectNode raised = announceInput("meeting:m-001");
        raised.put("maxLevel", "restricted");   // asking for more changes nothing
        JsonNode same = announced(raised);
        assertThat(same.get("items")).hasSize(2);
        assertThat(same.toString()).doesNotContain("restricted");

        ObjectNode person = announceInput("meeting:m-001");
        ((ArrayNode) person.get("types")).add("Person");
        assertThat(announced(person).get("withheld").asInt()).isEqualTo(2);   // the person and the restricted decision
    }

    @Test
    void ca5_the_version_in_the_pointer_is_the_node_s_and_moves_when_another_document_holds_it() throws Exception {
        JsonNode first = announced(announceInput("meeting:m-001"));
        long before = GraphPointer.parse(first.get("items").get(0).get("pointer").asText()).version();

        fx.store.write(KnowledgeFixture.SCOPE, doc("m-002", 1, List.of(node("Decision", OLD_NAME, Sensitivity.CONFIDENTIAL)),
                List.of())).toCompletableFuture().join();
        JsonNode again = announced(announceInput("meeting:m-001"));

        // the old announcement says what the node was; the new one for the same document says what it is now
        assertThat(GraphPointer.parse(again.get("items").get(0).get("pointer").asText()).version()).isGreaterThan(before);
        assertThat(GraphPointer.parse(again.get("items").get(1).get("pointer").asText()).version())
                .isEqualTo(GraphPointer.parse(first.get("items").get(1).get("pointer").asText()).version());
    }

    @Test
    void a_node_that_is_gone_is_not_announced() {
        fx.store.delete(KnowledgeFixture.SCOPE, new DeleteRequest.ByDocument("req-1", "erasure:document", "meeting:m-001"))
                .toCompletableFuture().join();

        assertThat(announced(announceInput("meeting:m-001")).get("items")).isEmpty();
    }

    @Test
    void the_audit_of_an_announcement_has_the_counts_and_no_node() {
        announced(announceInput("meeting:m-001"));

        List<AuditRecord> records = audit.of("i-a").toCompletableFuture().join();

        assertThat(records).singleElement().satisfies(r -> {
            assertThat(r.kind()).isEqualTo(KnowledgeAnnounceHandler.AUDIT_KIND);
            assertThat(r.detail()).containsEntry("base", "examples/product-meetings").containsEntry("confidential", "1")
                    .containsEntry("internal", "1").containsEntry("withheld", "1");
            assertThat(r.toString()).doesNotContainPattern("e-[0-9a-f]{32}").doesNotContain("SYNTH", "m-001");
        });
    }

    @Test
    void an_announcement_refuses_a_scope_it_does_not_serve_and_a_malformed_input() {
        ObjectNode other = announceInput("meeting:m-001");
        ((ObjectNode) other.get("scope")).put("base", "hr");
        assertThatThrownBy(() -> announced(other)).satisfies(e -> assertThat(status(e)).isEqualTo(403));

        ObjectNode noDocument = announceInput(" ");
        assertThatThrownBy(() -> announced(noDocument)).satisfies(e -> assertThat(status(e)).isEqualTo(400));
        ObjectNode noTypes = announceInput("meeting:m-001");
        noTypes.putArray("types");
        assertThatThrownBy(() -> announced(noTypes)).satisfies(e -> assertThat(status(e)).isEqualTo(400));
        ObjectNode badType = announceInput("meeting:m-001");
        ((ArrayNode) badType.get("types")).add("../Decision");
        assertThatThrownBy(() -> announced(badType)).satisfies(e -> assertThat(status(e)).isEqualTo(400));
        ObjectNode badLevel = announceInput("meeting:m-001");
        badLevel.put("maxLevel", "secret");
        assertThatThrownBy(() -> announced(badLevel)).satisfies(e -> assertThat(status(e)).isEqualTo(400));
    }

    // --- the references ----------------------------------------------------------------------------------------------------

    @Test
    void ca2_the_other_domain_gets_a_reference_per_node_and_a_relation_and_none_of_the_content() {
        JsonNode answer = announced(announceInput("meeting:m-001"));

        JsonNode done = referenced(referenceInput(answer, "meeting:m-001"));

        assertThat(done.get("status").asText()).isEqualTo("APPLIED");
        assertThat(done.get("references").asInt()).isEqualTo(2);
        GraphView view = archView();
        List<NodeView> references = view.nodes().stream().filter(n -> n.type().equals("DomainReference")).toList();
        assertThat(references).hasSize(2);
        assertThat(references).allSatisfy(n -> assertThat(n.label()).matches("(Decision|Requirement) @ examples/product-meetings [0-9a-f]{8}"));
        assertThat(view.nodes()).extracting(NodeView::type).contains("System");
        assertThat(view.edges()).hasSize(2).allSatisfy(e -> assertThat(e.type()).isEqualTo("informs"));
        // nothing the origin holds crossed: no name, no property, no trace of the restricted ones
        assertThat(arch.dump(ARCH)).doesNotContain("SYNTH-Old", "retry", "Idempotent", "budget", RESTRICTED_NAME)
                .contains("stint-kg://examples/product-meetings/").contains("observedVersion");
        assertThat(view.toString()).doesNotContain("retry", "Idempotent", RESTRICTED_NAME);
    }

    @Test
    void ca2_the_reference_is_at_the_level_of_the_node_it_points_to() {
        referenced(referenceInput(announced(announceInput("meeting:m-001")), "meeting:m-001"));

        List<NodeView> references = archView().nodes().stream().filter(n -> n.type().equals("DomainReference")).toList();

        assertThat(references).extracting(NodeView::level).containsExactlyInAnyOrder(Sensitivity.CONFIDENTIAL, Sensitivity.INTERNAL);
    }

    @Test
    void ca3_a_pointer_that_is_not_well_formed_refuses_the_whole_announcement_and_writes_nothing() {
        JsonNode answer = announced(announceInput("meeting:m-001"));
        for (String bad : List.of("", "https://example.com/x", "stint-kg://examples/product-meetings/Bad_Id?v=1",
                answer.get("items").get(0).get("pointer").asText().replace("?v=", "?w="),
                answer.get("items").get(0).get("pointer").asText() + "&x=1")) {
            ObjectNode in = referenceInput(answer, "meeting:m-001");
            ((ObjectNode) in.get("event").get("items").get(1)).put("pointer", bad);

            assertThatThrownBy(() -> referenced(in)).as(bad).satisfies(e -> {
                assertThat(status(e)).isEqualTo(400);
                assertThat(e.getCause().getMessage()).startsWith("items[1]: Invalid graph pointer (").doesNotContain("https", "example");
            });
        }
        assertThat(arch.changes(ARCH, 0, 100).toCompletableFuture().join()).isEmpty();
    }

    @Test
    void ca3_a_pointer_of_another_domain_than_the_event_s_or_a_restricted_level_is_refused() {
        JsonNode answer = announced(announceInput("meeting:m-001"));
        ObjectNode foreign = referenceInput(answer, "meeting:m-001");
        String pointer = answer.get("items").get(0).get("pointer").asText().replace("product-meetings", "hr");
        ((ObjectNode) foreign.get("event").get("items").get(0)).put("pointer", pointer);
        assertThatThrownBy(() -> referenced(foreign)).satisfies(e -> assertThat(status(e)).isEqualTo(400));

        ObjectNode restricted = referenceInput(answer, "meeting:m-001");
        ((ObjectNode) restricted.get("event").get("items").get(0)).put("level", "restricted");
        assertThatThrownBy(() -> referenced(restricted)).satisfies(e -> assertThat(status(e)).isEqualTo(400));

        ObjectNode none = referenceInput(answer, "meeting:m-001");
        ((ObjectNode) none.get("event")).putArray("items");
        assertThatThrownBy(() -> referenced(none)).satisfies(e -> assertThat(status(e)).isEqualTo(400));
        assertThat(arch.changes(ARCH, 0, 100).toCompletableFuture().join()).isEmpty();
    }

    @Test
    void ca6_a_domain_that_is_not_listened_to_is_refused_and_so_is_every_domain_when_the_list_is_empty() {
        JsonNode answer = announced(announceInput("meeting:m-001"));
        ObjectNode other = referenceInput(answer, "meeting:m-001");
        ((ObjectNode) other.get("event").get("domain")).put("base", "hr");
        assertThatThrownBy(() -> referenced(other)).satisfies(e -> assertThat(status(e)).isEqualTo(403));

        KnowledgeReferenceHandler deaf = new KnowledgeReferenceHandler(arch,
                new ScopeAllowList(Set.of(KnowledgeFixture.SCOPE, ARCH)), new OriginAllowList(Set.of()), audit, InstantSource.fixed(T0));
        assertThatThrownBy(() -> deaf.execute(KnowledgeFixture.task("i-r2", referenceInput(answer, "meeting:m-001"),
                Sensitivity.CONFIDENTIAL)).toCompletableFuture().join()).satisfies(e -> assertThat(status(e)).isEqualTo(403));

        assertThat(arch.changes(ARCH, 0, 100).toCompletableFuture().join()).isEmpty();
        assertThat(audit.of("i-r").toCompletableFuture().join()).singleElement().satisfies(r -> {
            assertThat(r.outcome()).isEqualTo("refused");
            assertThat(r.detail()).containsEntry("origin", "examples/hr");
        });
    }

    @Test
    void a_scope_this_worker_does_not_serve_is_refused() {
        ObjectNode in = referenceInput(announced(announceInput("meeting:m-001")), "meeting:m-001");
        ((ObjectNode) in.get("scope")).put("base", "payroll");

        assertThatThrownBy(() -> referenced(in)).satisfies(e -> assertThat(status(e)).isEqualTo(403));
    }

    @Test
    void ca7_the_same_announcement_changes_nothing_a_later_one_replaces_it_and_an_earlier_one_is_stale() {
        JsonNode first = announced(announceInput("meeting:m-001"));
        assertThat(referenced(referenceInput(first, "meeting:m-001")).get("status").asText()).isEqualTo("APPLIED");
        long entries = arch.changes(ARCH, 0, 1000).toCompletableFuture().join().size();

        // a redelivery of the same fact
        assertThat(referenced(referenceInput(first, "meeting:m-001")).get("status").asText()).isEqualTo("UNCHANGED");
        assertThat(arch.changes(ARCH, 0, 1000).toCompletableFuture().join()).hasSize((int) entries);

        // the origin changed the node and announced again: the reference is the same node, at the new version
        fx.store.write(KnowledgeFixture.SCOPE, doc("m-002", 1, List.of(node("Decision", OLD_NAME, Sensitivity.CONFIDENTIAL)),
                List.of())).toCompletableFuture().join();
        JsonNode later = announced(announceInput("meeting:m-001"));
        assertThat(referenced(referenceInput(later, "meeting:m-001")).get("status").asText()).isEqualTo("APPLIED");
        assertThat(archView().nodes().stream().filter(n -> n.type().equals("DomainReference"))).hasSize(2);
        assertThat(arch.dump(ARCH)).contains("observedVersion=" + GraphPointer.parse(later.get("items").get(0).get("pointer").asText())
                .version());

        // the first announcement arriving after the later one
        assertThat(referenced(referenceInput(first, "meeting:m-001")).get("status").asText()).isEqualTo("STALE");
    }

    @Test
    void ca9_erasing_the_document_of_the_references_takes_them_and_the_relations_and_the_log_keeps_only_ids() {
        referenced(referenceInput(announced(announceInput("meeting:m-001")), "meeting:m-001"));

        arch.delete(ARCH, new DeleteRequest.ByDocument("req-9", "erasure:document", "pointers:examples/product-meetings:meeting:m-001"))
                .toCompletableFuture().join();

        GraphView view = archView();
        assertThat(view.nodes()).extracting(NodeView::type).doesNotContain("DomainReference", "System");
        assertThat(view.edges()).isEmpty();
        assertThat(arch.changes(ARCH, 0, 1000).toCompletableFuture().join().toString()).doesNotContain("stint-kg", "SYNTH");
    }

    @Test
    void an_anchor_that_still_holds_personal_data_refuses_the_announcement() {
        ObjectNode in = referenceInput(announced(announceInput("meeting:m-001")), "meeting:m-001");
        ((ObjectNode) in.get("anchors").get(0)).put("name", "Contact maria.silva@example.com");

        assertThatThrownBy(() -> referenced(in)).satisfies(e -> {
            assertThat(status(e)).isEqualTo(400);
            assertThat(e.getCause().getMessage()).doesNotContain("maria");
        });
        assertThat(arch.changes(ARCH, 0, 100).toCompletableFuture().join()).isEmpty();
    }
}
