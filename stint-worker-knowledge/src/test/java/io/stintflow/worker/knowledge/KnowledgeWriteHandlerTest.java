package io.stintflow.worker.knowledge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.knowledge.ChangeEntry;
import io.stintflow.knowledge.DeleteRequest;
import io.stintflow.knowledge.KnowledgeBatch;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.spi.ErrorInfo.StandardType;
import io.stintflow.spi.Sensitivity;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskFailure;

/** SDD 3b.3, CA2 to CA4 and CA7: the write by pointer, the gate, the allowed scopes, the idempotence. */
class KnowledgeWriteHandlerTest {

    KnowledgeFixture fx;
    KnowledgeWriteHandler handler;

    @BeforeEach
    void setUp() throws Exception {
        fx = new KnowledgeFixture();
        handler = new KnowledgeWriteHandler(fx.store, fx.blobs, KnowledgeFixture.allowed());
    }

    private ObjectNode input(String batchRef) {
        ObjectNode in = Json.obj();
        in.put("batchRef", batchRef);
        in.set("scope", KnowledgeFixture.scopeNode());
        return in;
    }

    private JsonNode write(KnowledgeBatch batch) {
        return handler.execute(KnowledgeFixture.task("i-1", input(fx.blobOf(batch)), Sensitivity.CONFIDENTIAL))
                .toCompletableFuture().join();
    }

    private TaskFailure failure(ObjectNode in) {
        assertThatThrownBy(() -> handler.execute(KnowledgeFixture.task("i-1", in, Sensitivity.CONFIDENTIAL))
                .toCompletableFuture().join()).isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(TaskFailure.class);
        try {
            handler.execute(KnowledgeFixture.task("i-1", in, Sensitivity.CONFIDENTIAL)).toCompletableFuture().join();
        } catch (CompletionException e) {
            return (TaskFailure) e.getCause();
        }
        throw new AssertionError("no failure");
    }

    @Test
    void a_batch_by_pointer_is_written_once_and_a_redelivery_changes_nothing() {
        KnowledgeBatch batch = KnowledgeFixture.batch("m-1", 1_000L, Sensitivity.CONFIDENTIAL,
                "The Checkout Service has a new retry policy.", "Next steps were agreed.");
        JsonNode first = write(batch);
        assertThat(first.get("status").asText()).isEqualTo("APPLIED");
        assertThat(first.get("counts").get("ADD.PASSAGE").asInt()).isEqualTo(2);
        long sequence = first.get("lastSequence").asLong();

        JsonNode again = write(batch);   // the engine retried, or the fact was redelivered
        assertThat(again.get("status").asText()).isEqualTo("UNCHANGED");
        assertThat(again.get("lastSequence").asLong()).isEqualTo(sequence);   // no entry of the log was added
        assertThat(fx.store.changes(KnowledgeFixture.SCOPE, 0, 1000).toCompletableFuture().join()).hasSize((int) sequence);
    }

    @Test
    void the_passage_is_linked_to_its_entities_after_the_write() {
        write(KnowledgeFixture.batch("m-1", 1_000L, Sensitivity.CONFIDENTIAL, "The Checkout Service has a new retry policy."));
        var items = fx.store.retrieve(KnowledgeFixture.SCOPE, new io.stintflow.knowledge.RetrievalQuery(
                "checkout retry policy", 3, Sensitivity.CONFIDENTIAL)).toCompletableFuture().join();
        assertThat(items).isNotEmpty();
        assertThat(items.get(0).entities()).extracting(e -> e.label()).contains("Checkout Service");
    }

    @Test
    void an_older_revision_is_stale_and_not_a_failure() {
        write(KnowledgeFixture.batch("m-1", 2_000L, Sensitivity.CONFIDENTIAL, "A newer text."));
        JsonNode old = write(KnowledgeFixture.batch("m-1", 1_000L, Sensitivity.CONFIDENTIAL, "An older text."));
        assertThat(old.get("status").asText()).isEqualTo("STALE");
    }

    @Test
    void a_document_erased_does_not_come_back_with_a_redelivery() {
        KnowledgeBatch batch = KnowledgeFixture.batch("m-1", 1_000L, Sensitivity.CONFIDENTIAL, "Some text of the meeting.");
        write(batch);
        fx.store.delete(KnowledgeFixture.SCOPE, new DeleteRequest.ByDocument("req-1", "test", batch.documentId()))
                .toCompletableFuture().join();
        assertThat(write(batch).get("status").asText()).isEqualTo("REJECTED_ERASED");
    }

    @Test
    void a_batch_with_personal_data_is_refused_with_counts_by_type_and_no_value() {
        String planted = "Write to SYNTH-PII-0102@example.com or call +55 11 91234-5678.";
        ObjectNode in = input(fx.blobOf(KnowledgeFixture.batch("m-1", 1_000L, Sensitivity.CONFIDENTIAL, planted)));
        TaskFailure refused = failure(in);
        assertThat(refused.toErrorInfo().type()).isEqualTo(StandardType.VALIDATION.uri());
        assertThat(refused.toErrorInfo().status()).isEqualTo(400);
        assertThat(refused.getMessage()).contains("email=1", "phone=1").doesNotContain("SYNTH-PII", "example.com", "91234");
        assertThat(fx.store.changes(KnowledgeFixture.SCOPE, 0, 10).toCompletableFuture().join()).isEmpty();   // nothing written
    }

    @Test
    void an_entity_label_is_gated_too() {
        KnowledgeBatch clean = KnowledgeFixture.batch("m-1", 1_000L, Sensitivity.CONFIDENTIAL, "A clean text.");
        var entities = new java.util.ArrayList<>(clean.entities());
        entities.set(0, new io.stintflow.knowledge.Entity(entities.get(0).entityId(), "System", "SYNTH-PII-0102@example.com",
                Sensitivity.CONFIDENTIAL));
        KnowledgeBatch dirty = new KnowledgeBatch(clean.batchId(), clean.documentId(), clean.contentHash(), clean.revision(),
                clean.source(), clean.schemaVersion(), clean.passages(), entities, clean.relations(), clean.links());
        assertThat(failure(input(fx.blobOf(dirty))).getMessage()).contains("email=1").doesNotContain("SYNTH-PII");
    }

    @Test
    void a_scope_not_in_the_list_is_refused_before_anything_is_read() {
        ObjectNode in = input("file:///does-not-exist");
        ((ObjectNode) in.get("scope")).put("base", "hr");
        TaskFailure refused = failure(in);
        assertThat(refused.toErrorInfo().type()).isEqualTo(StandardType.AUTHORIZATION.uri());
        assertThat(refused.toErrorInfo().status()).isEqualTo(403);
    }

    @Test
    void an_empty_list_refuses_every_scope() {
        KnowledgeWriteHandler none = new KnowledgeWriteHandler(fx.store, fx.blobs, new ScopeAllowList(Set.of()));
        assertThatThrownBy(() -> none.execute(KnowledgeFixture.task("i-1", input("x"), Sensitivity.CONFIDENTIAL))
                .toCompletableFuture().join()).hasCauseInstanceOf(TaskFailure.class)
                .cause().extracting(c -> ((TaskFailure) c).toErrorInfo().status()).isEqualTo(403);
    }

    @Test
    void a_scope_is_required_and_a_pointer_is_required() {
        ObjectNode noScope = Json.obj();
        noScope.put("batchRef", "x");
        assertThat(failure(noScope).toErrorInfo().status()).isEqualTo(400);
        ObjectNode noRef = Json.obj();
        noRef.set("scope", KnowledgeFixture.scopeNode());
        assertThat(failure(noRef).getMessage()).contains("batchRef");
    }

    @Test
    void a_blob_that_cannot_be_read_is_a_communication_failure_the_engine_retries() {
        TaskFailure unreachable = failure(input("file:///no/such/blob"));
        assertThat(unreachable.toErrorInfo().type()).isEqualTo(StandardType.COMMUNICATION.uri());
        assertThat(unreachable.toErrorInfo().status()).isEqualTo(503);
    }

    @Test
    void a_blob_that_is_not_a_batch_is_a_validation_failure() {
        String ref = fx.blobs.put("not json at all".getBytes(), "knowledge/bad.json", KnowledgeFixture.BATCH)
                .toCompletableFuture().join().toString();
        assertThat(failure(input(ref)).toErrorInfo().type()).isEqualTo(StandardType.VALIDATION.uri());
    }

    @Test
    void the_log_names_the_instance_that_wrote_and_never_a_text() {
        write(KnowledgeFixture.batch("m-1", 1_000L, Sensitivity.CONFIDENTIAL, "The Checkout Service has a new retry policy."));
        var log = fx.store.changes(KnowledgeFixture.SCOPE, 0, 1000).toCompletableFuture().join();
        assertThat(log).isNotEmpty().allSatisfy(e -> {
            assertThat(e.instanceId()).isEqualTo("i-m-1");
            assertThat(e.definition()).isEqualTo(KnowledgeFixture.DEFINITION.canonical());
        });
        assertThat(log).extracting(ChangeEntry::reason).containsOnlyNulls();
        assertThat(fx.store.dump(KnowledgeFixture.SCOPE)).doesNotContain("SYNTH-PII");
        assertThat(new KnowledgeScope("examples", "product-meetings")).isEqualTo(KnowledgeFixture.SCOPE);
    }
}
