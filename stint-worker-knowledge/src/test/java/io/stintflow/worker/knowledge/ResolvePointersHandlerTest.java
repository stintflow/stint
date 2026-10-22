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
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryAuditLog;
import io.stintflow.knowledge.ChangeEntry;
import io.stintflow.knowledge.DeleteRequest;
import io.stintflow.knowledge.DeterministicEmbedder;
import io.stintflow.knowledge.Entity;
import io.stintflow.knowledge.GraphPointer;
import io.stintflow.knowledge.GraphPointers;
import io.stintflow.knowledge.KnowledgeBatch;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.PassageIds;
import io.stintflow.knowledge.Relation;
import io.stintflow.knowledge.Source;
import io.stintflow.knowledge.inmemory.CompositeKnowledgeStore;
import io.stintflow.knowledge.schema.KnowledgeSchema;
import io.stintflow.spi.AuditLog.AuditRecord;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.RetentionClass;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;

/**
 * SDD 3c.2: the origin answers a pointer by its own policy, through the view it publishes, to the definition that asks, with
 * the content in a blob that expires and not in the answer. Synthetic data only; the names planted here must never be in an
 * answer, an audit record or a denial.
 */
class ResolvePointersHandlerTest {

    static final KnowledgeScope BASE = KnowledgeScope.named("examples", "product-meetings");
    static final WorkflowRef ASKER = new WorkflowRef("examples", "architecture-links", "1.0.0");
    static final Instant T0 = Instant.parse("2026-10-21T10:00:00Z");

    final AtomicReference<Instant> now = new AtomicReference<>(T0);
    CompositeKnowledgeStore store;
    FilesystemBlobStore blobs;
    InMemoryAuditLog audit;
    KnowledgeSchema schema = KnowledgeSchema.meetings();

    private static Entity node(String id, String type, String name, Map<String, String> properties, Sensitivity level) {
        return new Entity(id, type, name, properties, level);
    }

    private static KnowledgeBatch doc(String meeting, long revision, List<Entity> entities, List<Relation> relations) {
        return new KnowledgeBatch("extract:" + meeting, "meeting:" + meeting, PassageIds.sha256Hex(entities.toString() + relations),
                revision, new Source("i-" + meeting, "chn-" + meeting, new WorkflowRef("examples", "knowledge-ingest", "1.0.0")),
                "meetings@1.0.0", List.of(), entities, relations, List.of());
    }

    private PublishedView view(boolean allowSensitive) {
        Set<String> decisionProperties = allowSensitive ? Set.of("status", "summary") : Set.of("status");
        return PublishedView.of("product-v1", BASE, schema, List.of(
                PublishedType.of("Decision", true, decisionProperties, Set.of("supersedes", "affects")),
                PublishedType.of("System", true, Set.of(), Set.of())), allowSensitive);
    }

    private ResolvePointersHandler handler(Sensitivity ceiling, boolean allowSensitive) {
        ResolutionPolicy policy = new ResolutionPolicy(
                List.of(new Grant("examples", "architecture-links", Set.of(BASE), Set.of("Decision", "System"), ceiling)),
                List.of(view(allowSensitive)));
        return new ResolvePointersHandler(store, store, policy, blobs, null, audit, InstantSource.fixed(T0));
    }

    @BeforeEach
    void setUp() throws Exception {
        blobs = new FilesystemBlobStore(Files.createTempDirectory("stint-3c2"), now::get);
        store = new CompositeKnowledgeStore(new DeterministicEmbedder(), blobs, new RetentionClass("restricted", Duration.ofDays(30)),
                InstantSource.system());
        audit = new InMemoryAuditLog();
        store.write(BASE, doc("m-001", 1, List.of(
                node("d-old", "Decision", "SYNTH-Old retry policy", Map.of("status", "accepted", "summary", "SYNTH-secret rationale"),
                        Sensitivity.INTERNAL),
                node("d-new", "Decision", "SYNTH-New retry policy", Map.of("status", "proposed"), Sensitivity.INTERNAL),
                node("s-checkout", "System", "SYNTH-Checkout Service", Map.of(), Sensitivity.INTERNAL),
                node("t-board", "Team", "SYNTH-Board", Map.of(), Sensitivity.CONFIDENTIAL),
                node("p-person", "Person", "SYNTH-PII-0303", Map.of(), Sensitivity.RESTRICTED),
                node("c-secret", "Decision", "SYNTH-Confidential decision", Map.of("status", "accepted"), Sensitivity.CONFIDENTIAL)),
                List.of(new Relation("supersedes", "d-new", "d-old", Sensitivity.INTERNAL),
                        new Relation("affects", "d-old", "s-checkout", Sensitivity.INTERNAL),
                        new Relation("owns", "t-board", "d-old", Sensitivity.CONFIDENTIAL),
                        new Relation("knows", "p-person", "d-old", Sensitivity.RESTRICTED)))).toCompletableFuture().join();
    }

    private GraphPointer pointer(String id) {
        return pointer(BASE, id);
    }

    private GraphPointer pointer(KnowledgeScope scope, String id) {
        List<ChangeEntry> log = store.changes(BASE, 0, 1000).toCompletableFuture().join();
        return GraphPointer.of(scope, id, GraphPointers.versionOf(log, id).orElse(1));
    }

    private TaskContext task(WorkflowRef definition, ObjectNode input) {
        return new TaskContext("/do/1/resolve", "i-ask", "c-1", 1, input, definition, "chn-ask", Lineage.NONE, null,
                Sensitivity.CONFIDENTIAL, null);
    }

    private ObjectNode input(int depth, GraphPointer... pointers) {
        ObjectNode in = Json.obj();
        ArrayNode array = in.putArray("pointers");
        for (GraphPointer pointer : pointers) {
            array.add(pointer.format());
        }
        in.put("depth", depth);
        return in;
    }

    private JsonNode resolved(ResolvePointersHandler handler, WorkflowRef asker, ObjectNode in) {
        return handler.execute(task(asker, in)).toCompletableFuture().join();
    }

    private JsonNode blob(JsonNode answer) {
        return ResolvedViews.read(blobs, answer.get("viewRef").asText()).orElseThrow();
    }

    private static int status(Throwable thrown) {
        return ((TaskFailure) thrown.getCause()).toErrorInfo().status();
    }

    // --- CA1, CA9 -----------------------------------------------------------------------------------------------------------

    @Test
    void ca1_a_pointer_resolves_to_the_published_view_in_a_blob_and_the_answer_has_no_content() {
        JsonNode answer = resolved(handler(Sensitivity.CONFIDENTIAL, false), ASKER, input(0, pointer("d-old")));

        assertThat(answer.get("results")).hasSize(1);
        assertThat(answer.get("results").get(0).get("status").asText()).isEqualTo("RESOLVED");
        assertThat(answer.get("results").get(0).get("changed").asBoolean()).isFalse();
        assertThat(answer.get("counts").get("RESOLVED").asInt()).isEqualTo(1);
        assertThat(answer.toString()).doesNotContain("SYNTH", "accepted", "rationale");   // status and a pointer to a blob
        JsonNode entry = blob(answer).get("entries").get(0);
        assertThat(entry.get("viewVersion").asText()).isEqualTo("product-v1");
        assertThat(entry.get("view").get("type").asText()).isEqualTo("Decision");
        assertThat(entry.get("view").get("label").asText()).isEqualTo("SYNTH-Old retry policy");
        assertThat(entry.get("view").get("properties").toString()).isEqualTo("{\"status\":\"accepted\"}");
    }

    @Test
    void ca9_a_sensitive_property_is_shown_only_when_the_view_allows_it_and_the_audit_says_so() {
        assertThatThrownBy(() -> PublishedView.of("v", BASE, schema, List.of(
                PublishedType.of("Decision", true, Set.of("summary"), Set.of())), false)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sensitive");
        assertThatThrownBy(() -> PublishedView.of("v", BASE, schema, List.of(PublishedType.of("Decision", true, Set.of("nope"), Set.of())),
                false)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PublishedView.of("v", BASE, schema, List.of(PublishedType.of("Ghost", true, Set.of(), Set.of())),
                false)).isInstanceOf(IllegalArgumentException.class);

        JsonNode answer = resolved(handler(Sensitivity.CONFIDENTIAL, true), ASKER, input(0, pointer("d-old")));

        assertThat(blob(answer).get("entries").get(0).get("view").get("properties").get("summary").asText())
                .isEqualTo("SYNTH-secret rationale");
        assertThat(audit.of("i-ask").toCompletableFuture().join()).singleElement()
                .satisfies(r -> assertThat(r.detail()).containsEntry("allowSensitive", "true"));
    }

    // --- CA2, CA3 -----------------------------------------------------------------------------------------------------------

    @Test
    void ca2_everything_the_asker_may_not_know_is_denied_the_same_way() {
        ResolvePointersHandler internalOnly = handler(Sensitivity.INTERNAL, false);
        GraphPointer person = pointer("p-person");                                                // restricted
        GraphPointer nobody = GraphPointer.of(BASE, "p-nobody", 7);                               // never existed, same length
        GraphPointer team = pointer("t-board");                                                   // a type not granted
        GraphPointer secret = pointer("c-secret");                                                // above the ceiling
        GraphPointer elsewhere = GraphPointer.of(KnowledgeScope.named("examples", "hr"), "d-old", 3);   // a base not served

        JsonNode answer = resolved(internalOnly, ASKER, input(0, person, nobody, team, secret, elsewhere));

        assertThat(answer.get("results")).hasSize(5).allSatisfy(r -> {
            assertThat(r.get("status").asText()).isEqualTo("DENIED");
            assertThat(r.has("changed")).isFalse();
            assertThat(r.has("currentVersion")).isFalse();
        });
        assertThat(answer.get("counts").get("DENIED").asInt()).isEqualTo(5);
        // the entries in the blob are the same shape: the restricted one and the one that never existed differ only by their text
        JsonNode entries = blob(answer).get("entries");
        assertThat(entries.get(0).toString().replace(person.format(), "P")).isEqualTo(entries.get(1).toString().replace(nobody.format(), "P"));
        assertThat(answer.toString() + entries).doesNotContain("SYNTH");
    }

    @Test
    void ca2_a_definition_with_no_grant_resolves_nothing_and_the_input_cannot_say_who_asks() {
        ResolvePointersHandler handler = handler(Sensitivity.CONFIDENTIAL, false);
        ObjectNode in = input(0, pointer("d-old"));
        in.put("requester", "examples:architecture-links");   // a field of the input decides nothing
        in.putObject("definition").put("namespace", "examples").put("name", "architecture-links");

        JsonNode other = resolved(handler, new WorkflowRef("examples", "someone-else", "1.0.0"), in);
        JsonNode none = handler.execute(new TaskContext("/do/1/resolve", "i-ask", "c-1", 1, in, null, null, Lineage.NONE, null,
                Sensitivity.CONFIDENTIAL, null)).toCompletableFuture().join();

        assertThat(other.get("results").get(0).get("status").asText()).isEqualTo("DENIED");
        assertThat(none.get("results").get(0).get("status").asText()).isEqualTo("DENIED");
        ResolvePointersHandler empty = new ResolvePointersHandler(store, store, new ResolutionPolicy(List.of(), List.of()), blobs, null,
                audit, InstantSource.fixed(T0));
        assertThat(resolved(empty, ASKER, input(0, pointer("d-old"))).get("results").get(0).get("status").asText()).isEqualTo("DENIED");
    }

    @Test
    void ca3_a_node_that_was_there_and_is_gone_is_removed_unless_it_was_restricted() {
        GraphPointer decision = pointer("d-old");
        GraphPointer person = pointer("p-person");
        store.delete(BASE, new DeleteRequest.ByDocument("req-1", "erasure:document", "meeting:m-001")).toCompletableFuture().join();

        JsonNode answer = resolved(handler(Sensitivity.CONFIDENTIAL, false), ASKER, input(0, decision, person));

        assertThat(answer.get("results").get(0).get("status").asText()).isEqualTo("REMOVED");
        assertThat(answer.get("results").get(1).get("status").asText()).isEqualTo("DENIED");
        assertThat(answer.get("counts").get("REMOVED").asInt()).isEqualTo(1);
    }

    // --- CA6, CA7 -----------------------------------------------------------------------------------------------------------

    @Test
    void ca6_an_old_pointer_resolves_the_current_content_and_says_it_changed() {
        GraphPointer before = pointer("d-old");
        store.write(BASE, doc("m-002", 1, List.of(node("d-old", "Decision", "SYNTH-Old retry policy",
                Map.of("status", "accepted", "summary", "SYNTH-secret rationale"), Sensitivity.INTERNAL)), List.of()))
                .toCompletableFuture().join();
        GraphPointer after = pointer("d-old");

        JsonNode answer = resolved(handler(Sensitivity.CONFIDENTIAL, false), ASKER, input(0, before, after));

        assertThat(after.version()).isGreaterThan(before.version());
        assertThat(answer.get("results").get(0).get("changed").asBoolean()).isTrue();
        assertThat(answer.get("results").get(0).get("currentVersion").asLong()).isEqualTo(after.version());
        assertThat(answer.get("results").get(1).get("changed").asBoolean()).isFalse();
    }

    @Test
    void ca7_a_batch_is_one_to_fifty_pointers_well_formed_and_the_depth_is_zero_or_one() {
        ResolvePointersHandler handler = handler(Sensitivity.CONFIDENTIAL, false);
        List<GraphPointer> many = new ArrayList<>();
        for (int i = 1; i <= 51; i++) {
            many.add(GraphPointer.of(BASE, "n-" + i, 1));
        }
        assertThatThrownBy(() -> resolved(handler, ASKER, input(0, many.toArray(new GraphPointer[0]))))
                .satisfies(e -> assertThat(status(e)).isEqualTo(400));
        assertThat(resolved(handler, ASKER, input(0, many.subList(0, 50).toArray(new GraphPointer[0]))).get("results")).hasSize(50);
        assertThatThrownBy(() -> resolved(handler, ASKER, input(0))).satisfies(e -> assertThat(status(e)).isEqualTo(400));
        assertThatThrownBy(() -> resolved(handler, ASKER, input(2, pointer("d-old")))).satisfies(e -> assertThat(status(e)).isEqualTo(400));

        ObjectNode bad = input(0, pointer("d-old"));
        ((ArrayNode) bad.get("pointers")).add("https://example.com/not-a-pointer");
        assertThatThrownBy(() -> resolved(handler, ASKER, bad)).satisfies(e -> {
            assertThat(status(e)).isEqualTo(400);
            assertThat(e.getCause().getMessage()).startsWith("pointers[1]: Invalid graph pointer (").doesNotContain("example.com");
        });
    }

    @Test
    void ca7_at_depth_one_the_neighbours_come_through_the_same_policy_and_the_rest_is_not_there() {
        JsonNode answer = resolved(handler(Sensitivity.CONFIDENTIAL, false), ASKER, input(1, pointer("d-old")));

        JsonNode related = blob(answer).get("entries").get(0).get("view").get("related");
        List<String> shown = new ArrayList<>();
        related.forEach(r -> shown.add(r.get("relation").asText() + ":" + r.get("direction").asText() + ":" + r.get("type").asText()
                + ":" + r.get("label").asText()));
        // the decision that supersedes it and the system it affects; not the team (not granted), not the person (restricted)
        assertThat(shown).containsExactlyInAnyOrder("supersedes:IN:Decision:SYNTH-New retry policy",
                "affects:OUT:System:SYNTH-Checkout Service");
        related.forEach(r -> assertThat(GraphPointer.parse(r.get("pointer").asText()).scope()).isEqualTo(BASE));
        assertThat(related.toString()).doesNotContain("Board", "PII-0303", "owns", "knows");
    }

    // --- CA8, CA10 ----------------------------------------------------------------------------------------------------------

    @Test
    void ca8_the_content_lives_in_a_blob_that_expires_and_can_be_discarded_at_once() {
        JsonNode answer = resolved(handler(Sensitivity.CONFIDENTIAL, false), ASKER, input(0, pointer("d-old")));
        String ref = answer.get("viewRef").asText();
        assertThat(ref).contains("resolved-view");   // the class is in the key (the local store flattens the slashes)

        assertThat(ResolvedViews.read(blobs, ref)).isPresent();
        now.set(T0.plus(Duration.ofHours(23)));
        assertThat(ResolvedViews.read(blobs, ref)).isPresent();
        now.set(T0.plus(Duration.ofDays(1)).plusSeconds(1));
        assertThat(ResolvedViews.read(blobs, ref)).isEmpty();   // gone: not an error, not a leftover

        now.set(T0);
        JsonNode again = resolved(handler(Sensitivity.CONFIDENTIAL, false), ASKER, input(0, pointer("d-old")));
        ResolvedViews.discard(blobs, again.get("viewRef").asText());
        assertThat(ResolvedViews.read(blobs, again.get("viewRef").asText())).isEmpty();
    }

    @Test
    void ca10_the_audit_has_who_asked_the_base_and_counts_and_no_node_and_no_content() {
        resolved(handler(Sensitivity.CONFIDENTIAL, false), ASKER, input(1, pointer("d-old"), pointer("p-person")));

        List<AuditRecord> records = audit.of("i-ask").toCompletableFuture().join();

        assertThat(records).singleElement().satisfies(r -> {
            assertThat(r.kind()).isEqualTo(ResolvePointersHandler.AUDIT_KIND);
            assertThat(r.detail()).containsEntry("requester", "examples:architecture-links")
                    .containsEntry("bases", "examples/product-meetings").containsEntry("resolved", "1").containsEntry("denied", "1")
                    .containsEntry("removed", "0").containsEntry("level.internal", "1").containsEntry("depth", "1");
            assertThat(r.toString()).doesNotContain("SYNTH", "d-old", "p-person", "accepted");
        });
    }

    @Test
    void a_failure_of_the_base_is_a_communication_error_the_engine_retries() {
        KnowledgeStoreFailing failing = new KnowledgeStoreFailing();
        ResolvePointersHandler handler = new ResolvePointersHandler(failing, store, new ResolutionPolicy(List.of(
                new Grant("examples", "architecture-links", Set.of(BASE), Set.of("Decision"), Sensitivity.INTERNAL)),
                List.of(view(false))), blobs, null, audit, InstantSource.fixed(T0));

        assertThatThrownBy(() -> resolved(handler, ASKER, input(0, pointer("d-old")))).satisfies(e -> assertThat(status(e)).isEqualTo(503));
    }

    /** A store whose log cannot be read. */
    private static final class KnowledgeStoreFailing extends FailingStore {
    }
}
