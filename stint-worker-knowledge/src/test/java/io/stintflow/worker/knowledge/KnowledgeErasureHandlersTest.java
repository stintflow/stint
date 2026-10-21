package io.stintflow.worker.knowledge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Proxy;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.inmemory.InMemoryAuditLog;
import io.stintflow.knowledge.DeterministicEmbedder;
import io.stintflow.knowledge.Embedder;
import io.stintflow.knowledge.EntityIds;
import io.stintflow.knowledge.KnowledgeBatch;
import io.stintflow.knowledge.RetrievalQuery;
import io.stintflow.knowledge.inmemory.CompositeKnowledgeStore;
import io.stintflow.spi.AuditLog;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.ErrorInfo.StandardType;
import io.stintflow.spi.RetentionClass;
import io.stintflow.spi.Sensitivity;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;
import io.stintflow.worker.redaction.Pseudonyms;

/**
 * SDD 3b.5: the tasks of erasure and reindexation — finding a person from who they are, erasing by document and by person with
 * the blobs the document came from, verifying, forgetting the subject, moving the base to another model — and what they leave in
 * the audit log (counts only). Synthetic data only.
 */
class KnowledgeErasureHandlersTest {

    static final Pseudonyms PSEUDONYMS = new Pseudonyms("synthetic-secret-0123456789".getBytes(StandardCharsets.UTF_8));
    static final String NAME = "Synthia Example";

    KnowledgeFixture fx;
    InMemoryAuditLog audit;
    KnowledgeWriteHandler write;
    KnowledgeEraseHandler erase;
    KnowledgeVerifyHandler verify;
    ResolvePersonHandler resolve;
    ForgetSubjectHandler forget;

    @BeforeEach
    void setUp() throws Exception {
        fx = new KnowledgeFixture();
        audit = new InMemoryAuditLog();
        write = new KnowledgeWriteHandler(fx.store, fx.blobs, KnowledgeFixture.allowed());
        erase = new KnowledgeEraseHandler(fx.store, KnowledgeFixture.allowed(), audit, InstantSource.system());
        verify = new KnowledgeVerifyHandler(fx.store, KnowledgeFixture.allowed());
        resolve = new ResolvePersonHandler(PSEUDONYMS, fx.blobs, KnowledgeFixture.allowed());
        forget = new ForgetSubjectHandler(fx.blobs);
    }

    private static JsonNode run(TaskHandler handler, ObjectNode input) {
        return handler.execute(KnowledgeFixture.task("i-er", input, Sensitivity.RESTRICTED)).toCompletableFuture().join();
    }

    private static TaskFailure failure(TaskHandler handler, ObjectNode input) {
        try {
            run(handler, input);
        } catch (CompletionException e) {
            return (TaskFailure) e.getCause();
        }
        throw new AssertionError("no failure");
    }

    private ObjectNode withScope(ObjectNode in) {
        in.set("scope", KnowledgeFixture.scopeNode());
        return in;
    }

    private URI blob(String text) {
        return fx.blobs.put(text.getBytes(StandardCharsets.UTF_8), "origin/" + Math.abs(text.hashCode()),
                new RetentionClass("minutes", Duration.ofDays(90))).toCompletableFuture().join();
    }

    /** A meeting naming the person {@link #NAME} as the ingestion would: the entity id of her stable pseudonym. */
    private KnowledgeBatch batchNaming(String meetingId, String text) {
        String pseudonym = PSEUDONYMS.labelStable(KnowledgeFixture.SCOPE.key(), "person", NAME);
        KnowledgeBatch plain = KnowledgeFixture.batch(meetingId, 1L, Sensitivity.CONFIDENTIAL, text);
        var person = new io.stintflow.knowledge.Entity(EntityIds.of("Person", pseudonym), "Person", pseudonym,
                Sensitivity.CONFIDENTIAL);
        var system = plain.entities().get(0);
        return new KnowledgeBatch(plain.batchId(), plain.documentId(), plain.contentHash(), plain.revision(), plain.source(),
                plain.schemaVersion(), plain.passages(), List.of(system, person),
                List.of(new io.stintflow.knowledge.Relation("affects", person.entityId(), system.entityId(),
                        Sensitivity.CONFIDENTIAL)),
                List.of(new io.stintflow.knowledge.Link(plain.passages().get(0).passageId(), system.entityId()),
                        new io.stintflow.knowledge.Link(plain.passages().get(0).passageId(), person.entityId())));
    }

    private JsonNode ingest(KnowledgeBatch batch, URI... origins) {
        ObjectNode in = withScope(Json.obj());
        in.put("batchRef", fx.blobOf(batch));
        var array = in.putArray("derivedFrom");
        for (URI origin : origins) {
            array.add(origin.toString());
        }
        return run(write, in);
    }

    private ObjectNode erasureOf(String requestId, String kind, String idField, String id) {
        ObjectNode in = withScope(Json.obj());
        in.put("requestId", requestId).put("kind", kind).put(idField, id);
        return in;
    }

    private boolean exists(URI uri) {
        try {
            fx.blobs.get(uri).toCompletableFuture().join();
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    // --- resolve-person ------------------------------------------------------------------------------------------

    @Test
    void a_person_is_found_from_who_she_is_and_the_answer_holds_neither_name_nor_pseudonym() {
        URI subject = blob("  " + NAME + "\n");
        ObjectNode in = withScope(Json.obj());
        in.put("personRef", subject.toString());

        JsonNode out = run(resolve, in);

        String pseudonym = PSEUDONYMS.labelStable(KnowledgeFixture.SCOPE.key(), "person", NAME);
        assertThat(out.get("entityId").asText()).isEqualTo(EntityIds.of("Person", pseudonym));
        assertThat(out.toString()).doesNotContain("Synthia").doesNotContain(pseudonym);
        // the same person written another way is the same entity; another person is another
        ObjectNode other = withScope(Json.obj());
        other.put("personRef", blob("SYNTHIA   example").toString());
        assertThat(run(resolve, other).get("entityId").asText()).isEqualTo(out.get("entityId").asText());
        ObjectNode another = withScope(Json.obj());
        another.put("personRef", blob("Pat Synthetic").toString());
        assertThat(run(resolve, another).get("entityId").asText()).isNotEqualTo(out.get("entityId").asText());
    }

    @Test
    void an_empty_subject_and_a_missing_pointer_are_refused_and_a_missing_blob_is_retried() {
        ObjectNode empty = withScope(Json.obj());
        empty.put("personRef", blob("   ").toString());
        assertThat(failure(resolve, empty).toErrorInfo().status()).isEqualTo(400);
        assertThat(failure(resolve, withScope(Json.obj())).toErrorInfo().status()).isEqualTo(400);
        ObjectNode gone = withScope(Json.obj());
        gone.put("personRef", "file:///nowhere/at/all.txt");
        assertThat(failure(resolve, gone).toErrorInfo().type()).isEqualTo(StandardType.COMMUNICATION.uri());
    }

    // --- erase and verify -----------------------------------------------------------------------------------------

    @Test
    void erasing_a_person_removes_her_and_the_blobs_of_the_documents_that_named_her_and_the_verification_agrees() {
        URI minutes = blob("minutes of m-e1 naming the person");
        URI otherMinutes = blob("minutes of m-e2");
        ingest(batchNaming("m-e1", "The Checkout Service handover is owned by the person."), minutes);
        ingest(KnowledgeFixture.batch("m-e2", 1L, Sensitivity.CONFIDENTIAL, "The retro is on Friday."), otherMinutes);
        ObjectNode ref = withScope(Json.obj());
        ref.put("personRef", blob(NAME).toString());
        String entityId = run(resolve, ref).get("entityId").asText();

        JsonNode erased = run(erase, erasureOf("req-p1", "person", "entityId", entityId));
        JsonNode checked = run(verify, erasureOf("req-p1", "person", "entityId", entityId));

        assertThat(erased.get("counts").fieldNames()).toIterable().contains("ERASE.ENTITY");
        assertThat(checked.get("verified").asBoolean()).isTrue();
        assertThat(checked.get("tombstone").asBoolean()).isTrue();
        assertThat(checked.get("remaining").size()).isZero();
        assertThat(exists(minutes)).isFalse();
        assertThat(exists(otherMinutes)).isTrue();
        // L2 (SDD 3b.1, 8d): the log keeps the opaque id with an ERASE entry; the pseudonym itself is nowhere
        assertThat(fx.store.dump(KnowledgeFixture.SCOPE)).doesNotContain(PSEUDONYMS.labelStable(KnowledgeFixture.SCOPE.key(),
                "person", NAME));
    }

    @Test
    void erasing_a_document_removes_the_batch_and_the_minutes_it_came_from() {
        URI minutes = blob("minutes of m-e3");
        JsonNode written = ingest(KnowledgeFixture.batch("m-e3", 1L, Sensitivity.CONFIDENTIAL, "Capacity planning for Q4."),
                minutes);
        assertThat(written.get("status").asText()).isEqualTo("APPLIED");

        run(erase, erasureOf("req-d1", "document", "documentId", "meeting:m-e3"));

        assertThat(exists(minutes)).isFalse();
        JsonNode checked = run(verify, erasureOf("req-d1", "document", "documentId", "meeting:m-e3"));
        assertThat(checked.get("verified").asBoolean()).isTrue();
        assertThat(fx.store.retrieve(KnowledgeFixture.SCOPE, new RetrievalQuery("capacity planning", 5, Sensitivity.RESTRICTED))
                .toCompletableFuture().join()).isEmpty();
    }

    @Test
    void the_same_request_again_erases_nothing_and_a_person_who_is_not_there_still_gets_the_tombstone() {
        ingest(KnowledgeFixture.batch("m-e4", 1L, Sensitivity.CONFIDENTIAL, "Sprint review notes."));
        run(erase, erasureOf("req-d2", "document", "documentId", "meeting:m-e4"));
        assertThat(run(erase, erasureOf("req-d2", "document", "documentId", "meeting:m-e4")).get("counts").size()).isZero();

        String absent = EntityIds.of("Person", "PERSON_ffffffffffffffffffffffffffffffff");
        JsonNode none = run(erase, erasureOf("req-p2", "person", "entityId", absent));
        assertThat(none.get("counts").size()).isZero();
        assertThat(run(verify, erasureOf("req-p2", "person", "entityId", absent)).get("verified").asBoolean()).isTrue();

        // and a later meeting that names her is written without her
        String pseudonym = "PERSON_ffffffffffffffffffffffffffffffff";
        var person = new io.stintflow.knowledge.Entity(absent, "Person", pseudonym, Sensitivity.CONFIDENTIAL);
        KnowledgeBatch plain = KnowledgeFixture.batch("m-e5", 1L, Sensitivity.CONFIDENTIAL, "Planning notes.");
        KnowledgeBatch later = new KnowledgeBatch(plain.batchId(), plain.documentId(), plain.contentHash(), 1L, plain.source(),
                plain.schemaVersion(), plain.passages(), List.of(plain.entities().get(0), person), List.of(),
                List.of(new io.stintflow.knowledge.Link(plain.passages().get(0).passageId(), plain.entities().get(0).entityId())));
        assertThat(ingest(later).get("dropped").asInt()).isEqualTo(1);
    }

    @Test
    void a_blob_the_store_could_not_really_delete_is_found_by_the_verification() throws Exception {
        BlobStore lying = (BlobStore) Proxy.newProxyInstance(BlobStore.class.getClassLoader(), new Class<?>[] {BlobStore.class},
                (proxy, method, args) -> method.getName().equals("delete") ? CompletableFuture.completedFuture(null)
                        : method.invoke(fx.blobs, args));
        CompositeKnowledgeStore store = new CompositeKnowledgeStore(new DeterministicEmbedder(), lying,
                new RetentionClass("restricted", Duration.ofDays(30)), InstantSource.system());
        URI minutes = blob("minutes of m-e6");
        KnowledgeBatch batch = KnowledgeFixture.batch("m-e6", 1L, Sensitivity.CONFIDENTIAL, "Budget talk.");
        store.write(KnowledgeFixture.SCOPE, batch.withDerivedFrom(List.of(minutes))).toCompletableFuture().join();
        KnowledgeEraseHandler eraser = new KnowledgeEraseHandler(store, KnowledgeFixture.allowed(), audit, InstantSource.system());
        KnowledgeVerifyHandler checker = new KnowledgeVerifyHandler(store, KnowledgeFixture.allowed());

        run(eraser, erasureOf("req-l", "document", "documentId", "meeting:m-e6"));
        JsonNode checked = run(checker, erasureOf("req-l", "document", "documentId", "meeting:m-e6"));

        assertThat(checked.get("verified").asBoolean()).isFalse();
        assertThat(checked.get("tombstone").asBoolean()).isTrue();
        assertThat(checked.get("remaining").get("BLOB").asInt()).isEqualTo(1);
    }

    @Test
    void the_audit_of_an_erasure_holds_counts_and_no_id_of_a_person() {
        ingest(batchNaming("m-e7", "The Checkout Service rollout is owned by the person."));
        String entityId = EntityIds.of("Person", PSEUDONYMS.labelStable(KnowledgeFixture.SCOPE.key(), "person", NAME));

        run(erase, erasureOf("req-a", "person", "entityId", entityId));

        List<AuditLog.AuditRecord> records = audit.of("i-er").toCompletableFuture().join();
        assertThat(records).singleElement().satisfies(r -> {
            assertThat(r.kind()).isEqualTo(KnowledgeEraseHandler.AUDIT_KIND);
            assertThat(r.detail()).containsEntry("kind", "person").containsEntry("base", KnowledgeFixture.SCOPE.key());
        });
        assertThat(records.toString()).doesNotContain(entityId).doesNotContain("Synthia").doesNotContain("PERSON_");
    }

    @Test
    void a_scope_the_worker_does_not_serve_and_a_malformed_request_are_refused_before_anything_is_touched() {
        ObjectNode foreign = erasureOf("req-x", "document", "documentId", "meeting:m-e8");
        ObjectNode scope = foreign.putObject("scope");
        scope.put("namespace", "examples").put("base", "other-base");
        assertThat(failure(erase, foreign).toErrorInfo().status()).isEqualTo(403);
        assertThat(failure(verify, foreign).toErrorInfo().status()).isEqualTo(403);

        assertThat(failure(erase, erasureOf("bad id!", "document", "documentId", "d")).toErrorInfo().status()).isEqualTo(400);
        assertThat(failure(erase, erasureOf("req-y", "team", "documentId", "d")).toErrorInfo().status()).isEqualTo(400);
        assertThat(failure(erase, erasureOf("req-y", "person", "entityId", "PERSON_abc")).toErrorInfo().status()).isEqualTo(400);
        assertThat(failure(erase, erasureOf("req-y", "document", "entityId", "x")).toErrorInfo().status()).isEqualTo(400);
        ObjectNode badPolicy = erasureOf("req-y", "person", "entityId", EntityIds.of("Person", "p"));
        badPolicy.put("passages", "SOME");
        assertThat(failure(erase, badPolicy).toErrorInfo().status()).isEqualTo(400);
        assertThat(new KnowledgeEraseHandler(fx.store, new ScopeAllowList(Set.of()), audit, InstantSource.system())
                .execute(KnowledgeFixture.task("i-er", erasureOf("req-z", "document", "documentId", "d"), Sensitivity.RESTRICTED))
                .toCompletableFuture().isCompletedExceptionally()).isTrue();   // an empty list refuses everything
    }

    // --- forget-subject --------------------------------------------------------------------------------------------

    @Test
    void the_subject_blob_is_forgotten_once_and_forgetting_it_again_or_none_is_not_a_failure() {
        URI subject = blob(NAME);
        ObjectNode in = Json.obj();
        in.put("personRef", subject.toString());

        assertThat(run(forget, in).get("forgotten").asBoolean()).isTrue();
        assertThat(exists(subject)).isFalse();
        assertThat(run(forget, in).get("forgotten").asBoolean()).isTrue();   // already gone: the answer wanted
        assertThat(run(forget, Json.obj()).get("forgotten").asBoolean()).isFalse();   // a document erasure has no subject
    }

    // --- knowledge-reindex ------------------------------------------------------------------------------------------

    private static final class Other implements Embedder {
        @Override
        public float[] embed(String text) {
            float[] v = new float[16];
            for (String w : EntityIds.normalize(text).split(" ")) {
                if (!w.isEmpty()) {
                    v[Math.floorMod(w.hashCode(), 16)] += 1f;
                }
            }
            return v;
        }

        @Override
        public int dimension() {
            return 16;
        }

        @Override
        public String model() {
            return "other-bag-16";
        }

        @Override
        public Set<String> languages() {
            return Set.of("en");
        }
    }

    @Test
    void the_worker_moves_a_base_only_to_its_own_model_and_a_repeat_is_unchanged() {
        ingest(KnowledgeFixture.batch("m-r1", 1L, Sensitivity.CONFIDENTIAL, "The data cluster needs more storage."));
        KnowledgeReindexHandler reindex = new KnowledgeReindexHandler(fx.store, new Other(), KnowledgeFixture.allowed(), audit,
                InstantSource.system());
        ObjectNode ask = withScope(Json.obj());
        ask.put("requestId", "req-r1").put("targetModel", "some-other-model");
        assertThat(failure(reindex, ask).toErrorInfo().status()).isEqualTo(400);   // the definition asks, the worker decides

        ask.put("targetModel", "other-bag-16");
        JsonNode done = run(reindex, ask);
        assertThat(done.get("status").asText()).isEqualTo("APPLIED");
        assertThat(done.get("from").asText()).isEqualTo(new DeterministicEmbedder().model());
        assertThat(done.get("to").asText()).isEqualTo("other-bag-16");
        assertThat(done.get("passages").asInt()).isEqualTo(1);
        assertThat(run(reindex, ask).get("status").asText()).isEqualTo("UNCHANGED");
        assertThat(fx.store.retrieve(KnowledgeFixture.SCOPE, new RetrievalQuery("data cluster storage", 3, Sensitivity.CONFIDENTIAL))
                .toCompletableFuture().join()).hasSize(1);

        List<AuditLog.AuditRecord> records = audit.of("i-er").toCompletableFuture().join();
        assertThat(records).hasSize(2).allSatisfy(r -> assertThat(r.kind()).isEqualTo(KnowledgeReindexHandler.AUDIT_KIND));
        assertThat(records.toString()).doesNotContain("storage");
        assertThatThrownBy(() -> new KnowledgeReindexHandler(null, new Other(), KnowledgeFixture.allowed(), audit,
                InstantSource.system())).isInstanceOf(NullPointerException.class);
    }

    @Test
    void the_routes_are_registered_with_the_embedder_and_the_secret_only_when_given() {
        var without = KnowledgeWorkers.registerErasure(new io.stintflow.worker.TaskHandlerRegistry(), fx.store, fx.blobs,
                KnowledgeFixture.allowed(), audit, InstantSource.system(), null, null);
        var with = KnowledgeWorkers.registerErasure(new io.stintflow.worker.TaskHandlerRegistry(), fx.store, fx.blobs,
                KnowledgeFixture.allowed(), audit, InstantSource.system(), new Other(), PSEUDONYMS);

        assertThat(without.find(KnowledgeWorkers.ERASE_ROUTE)).isPresent();
        assertThat(without.find(KnowledgeWorkers.VERIFY_ROUTE)).isPresent();
        assertThat(without.find(KnowledgeWorkers.FORGET_SUBJECT_ROUTE)).isPresent();
        assertThat(without.find(KnowledgeWorkers.REINDEX_ROUTE)).isEmpty();
        assertThat(without.find(KnowledgeWorkers.RESOLVE_PERSON_ROUTE)).isEmpty();
        assertThat(with.find(KnowledgeWorkers.REINDEX_ROUTE)).isPresent();
        assertThat(with.find(KnowledgeWorkers.RESOLVE_PERSON_ROUTE)).isPresent();
    }
}
