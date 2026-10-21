package io.stintflow.worker.knowledge;

import java.nio.file.Files;
import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.knowledge.DeterministicEmbedder;
import io.stintflow.knowledge.Entity;
import io.stintflow.knowledge.EntityIds;
import io.stintflow.knowledge.KnowledgeBatch;
import io.stintflow.knowledge.KnowledgeBatchJson;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.Link;
import io.stintflow.knowledge.Passage;
import io.stintflow.knowledge.PassageIds;
import io.stintflow.knowledge.Relation;
import io.stintflow.knowledge.Source;
import io.stintflow.knowledge.inmemory.CompositeKnowledgeStore;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.RetentionClass;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;

/** Shared by the tests: a base, a blob store, batches and task contexts. Synthetic data only. */
final class KnowledgeFixture {

    static final KnowledgeScope SCOPE = KnowledgeScope.named("examples", "product-meetings");
    static final RetentionClass BATCH = new RetentionClass("knowledge-batch", Duration.ofDays(7));
    static final WorkflowRef DEFINITION = new WorkflowRef("examples", "knowledge-ingest", "1.0.0");

    final BlobStore blobs;
    final CompositeKnowledgeStore store;

    KnowledgeFixture() throws Exception {
        this.blobs = new FilesystemBlobStore(Files.createTempDirectory("stint-3b3"));
        this.store = new CompositeKnowledgeStore(new DeterministicEmbedder(), blobs,
                new RetentionClass("restricted", Duration.ofDays(30)), InstantSource.system());
    }

    static ScopeAllowList allowed() {
        return new ScopeAllowList(Set.of(SCOPE));
    }

    /** A batch of one meeting: its passages, a system and a person (a pseudonym) mentioned in the first. */
    static KnowledgeBatch batch(String meetingId, Long revision, Sensitivity level, String... texts) {
        String documentId = "meeting:" + meetingId;
        List<Passage> passages = new java.util.ArrayList<>();
        for (int i = 0; i < texts.length; i++) {
            passages.add(new Passage(PassageIds.of(SCOPE, documentId, i, texts[i]), i, texts[i], "en", level));
        }
        Entity system = new Entity(EntityIds.of("System", "Checkout Service"), "System", "Checkout Service", level);
        Entity person = new Entity(EntityIds.of("Person", "PERSON_0123456789abcdef0123456789abcdef"), "Person",
                "PERSON_0123456789abcdef0123456789abcdef", level);
        return new KnowledgeBatch("extract:" + meetingId, documentId, PassageIds.sha256Hex(String.join("\0", texts)), revision,
                new Source("i-" + meetingId, "chn-" + meetingId, DEFINITION), "meetings@1.0.0", passages,
                List.of(system, person), List.of(new Relation("affects", person.entityId(), system.entityId(), level)),
                passages.isEmpty() ? List.of() : List.of(new Link(passages.get(0).passageId(), system.entityId()),
                        new Link(passages.get(0).passageId(), person.entityId())));
    }

    /** Puts the batch in a blob and returns its pointer. */
    String blobOf(KnowledgeBatch batch) {
        return blobs.put(Json.bytes(KnowledgeBatchJson.toJson(batch)), "knowledge/" + batch.batchId() + ".json", BATCH)
                .toCompletableFuture().join().toString();
    }

    static ObjectNode scopeNode() {
        ObjectNode scope = Json.obj();
        scope.put("namespace", SCOPE.namespace()).put("base", SCOPE.base());
        return scope;
    }

    static TaskContext task(String instanceId, JsonNode input, Sensitivity level) {
        return new TaskContext("/do/2/write", instanceId, "c-" + instanceId, 1, input, DEFINITION, "chn-" + instanceId,
                Lineage.NONE, null, level, null);
    }
}
