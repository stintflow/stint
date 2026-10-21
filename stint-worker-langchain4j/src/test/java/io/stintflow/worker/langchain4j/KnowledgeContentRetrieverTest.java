package io.stintflow.worker.langchain4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import dev.langchain4j.rag.content.Content;
import dev.langchain4j.rag.query.Query;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryAuditLog;
import io.stintflow.knowledge.ContextItem;
import io.stintflow.knowledge.DeterministicEmbedder;
import io.stintflow.knowledge.Entity;
import io.stintflow.knowledge.EntityIds;
import io.stintflow.knowledge.KnowledgeBatch;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.KnowledgeStore;
import io.stintflow.knowledge.Link;
import io.stintflow.knowledge.Passage;
import io.stintflow.knowledge.PassageIds;
import io.stintflow.knowledge.Relation;
import io.stintflow.knowledge.RetrievalQuery;
import io.stintflow.knowledge.Source;
import io.stintflow.knowledge.inmemory.CompositeKnowledgeStore;
import io.stintflow.spi.AuditLog.AuditRecord;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.ModelProvider;
import io.stintflow.spi.RetentionClass;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;

/** SDD 3b.4, CA2 and CA7: the retriever's ceiling comes from the instance and the model, the origin travels, a failure is no context. */
class KnowledgeContentRetrieverTest {

    static final KnowledgeScope SCOPE = KnowledgeScope.named("examples", "product-meetings");
    static final WorkflowRef DEFINITION = new WorkflowRef("examples", "knowledge-ingest", "1.0.0");
    static final ModelProvider LOCAL = new ModelProvider("ollama", "llama", false);
    static final ModelProvider REMOTE = new ModelProvider("openai", "gpt", true);

    CompositeKnowledgeStore store;

    private static KnowledgeBatch doc(String meeting, Sensitivity level, String text, Entity entity, Relation... relations) {
        String documentId = "meeting:" + meeting;
        Passage passage = new Passage(PassageIds.of(SCOPE, documentId, 0, text), 0, text, "en", level);
        List<Entity> entities = new java.util.ArrayList<>(List.of(entity));
        for (Relation r : relations) {
            for (String end : List.of(r.fromId(), r.toId())) {
                if (entities.stream().noneMatch(e -> e.entityId().equals(end))) {
                    entities.add(new Entity(end, "System", "Other " + end.substring(2, 6), Sensitivity.INTERNAL));
                }
            }
        }
        return new KnowledgeBatch("extract:" + meeting, documentId, PassageIds.sha256Hex(text), 1L,
                new Source("i-" + meeting, "chn-" + meeting, DEFINITION), "meetings@1.0.0", List.of(passage), entities,
                List.of(relations), List.of(new Link(passage.passageId(), entity.entityId())));
    }

    @BeforeEach
    void setUp() throws Exception {
        store = new CompositeKnowledgeStore(new DeterministicEmbedder(), new FilesystemBlobStore(Files.createTempDirectory("stint-3b4")),
                new RetentionClass("restricted", Duration.ofDays(30)), InstantSource.system());
        Entity checkout = new Entity(EntityIds.of("System", "Checkout Service"), "System", "Checkout Service", Sensitivity.INTERNAL);
        store.write(SCOPE, doc("m-001", Sensitivity.INTERNAL, "The checkout service retries a failed payment.", checkout))
                .toCompletableFuture().join();
        store.write(SCOPE, doc("m-002", Sensitivity.CONFIDENTIAL, "The checkout service budget is under review.", checkout))
                .toCompletableFuture().join();
    }

    private static List<String> texts(List<Content> contents) {
        return contents.stream().map(c -> c.textSegment().text()).toList();
    }

    @Test
    void ca2_the_ceiling_is_the_lower_of_the_instance_and_the_model_and_nothing_above_it_comes_back() {
        var internalInstance = KnowledgeContentRetriever.builder(store, SCOPE).instanceLevel(Sensitivity.INTERNAL)
                .provider(LOCAL).build();
        var unapproved = KnowledgeContentRetriever.builder(store, SCOPE).instanceLevel(Sensitivity.CONFIDENTIAL)
                .provider(REMOTE).approvedRemote(false).build();
        var approved = KnowledgeContentRetriever.builder(store, SCOPE).instanceLevel(Sensitivity.CONFIDENTIAL)
                .provider(REMOTE).approvedRemote(true).build();
        var unknownLevel = KnowledgeContentRetriever.builder(store, SCOPE).provider(REMOTE).approvedRemote(true).build();

        Query question = Query.from("checkout service budget");

        assertThat(internalInstance.ceiling()).isEqualTo(Sensitivity.INTERNAL);
        assertThat(texts(internalInstance.retrieve(question))).noneMatch(t -> t.contains("budget is under review"));
        assertThat(unapproved.ceiling()).isEqualTo(Sensitivity.INTERNAL);
        assertThat(texts(unapproved.retrieve(question))).noneMatch(t -> t.contains("budget is under review"));
        assertThat(approved.ceiling()).isEqualTo(Sensitivity.CONFIDENTIAL);
        assertThat(texts(approved.retrieve(question))).anyMatch(t -> t.contains("budget is under review"));
        assertThat(unknownLevel.ceiling()).isEqualTo(Sensitivity.CONFIDENTIAL);   // no level reads as restricted, the approved model caps it
    }

    @Test
    void ca7_every_content_carries_the_origin_to_cite() {
        var retriever = KnowledgeContentRetriever.builder(store, SCOPE).instanceLevel(Sensitivity.CONFIDENTIAL).provider(LOCAL)
                .hops(1).build();

        List<Content> contents = retriever.retrieve(Query.from("checkout service retries a failed payment"));

        var passage = contents.stream().filter(c -> c.textSegment().metadata().getString("kind").equals("PASSAGE")).findFirst()
                .orElseThrow().textSegment().metadata();
        assertThat(passage.getString("documentId")).startsWith("meeting:m-00");
        assertThat(passage.getString("instanceId")).startsWith("i-m-00");
        assertThat(passage.getString("chainId")).startsWith("chn-m-00");
        assertThat(passage.getString("level")).isIn("internal", "confidential");
        assertThat(passage.getDouble("score")).isPositive();
        var entity = contents.stream().filter(c -> c.textSegment().metadata().getString("kind").equals("ENTITY")).findFirst()
                .orElseThrow().textSegment();
        assertThat(entity.text()).isEqualTo("System: Checkout Service");
        assertThat(entity.metadata().getString("evidence")).isEqualTo("meeting:m-001,meeting:m-002");
    }

    @Test
    void a_question_that_names_an_entity_follows_the_graph_two_hops_by_default() throws Exception {
        Entity a = new Entity("e-aaaa0000", "System", "Alpha", Sensitivity.INTERNAL);
        store.write(SCOPE, doc("m-003", Sensitivity.INTERNAL, "Alpha talks to Beta.", a,
                new Relation("affects", "e-aaaa0000", "e-bbbb0000", Sensitivity.INTERNAL),
                new Relation("affects", "e-bbbb0000", "e-cccc0000", Sensitivity.INTERNAL))).toCompletableFuture().join();
        var retriever = KnowledgeContentRetriever.builder(store, SCOPE).instanceLevel(Sensitivity.INTERNAL).provider(LOCAL).build();

        List<String> found = texts(retriever.retrieve(Query.from("what do we know about alpha")));

        assertThat(found).anyMatch(t -> t.contains("-[affects]->")).anyMatch(t -> t.startsWith("System: Other bbbb"));
    }

    @Test
    void excluded_documents_never_come_back_and_the_graph_items_lose_them_from_their_evidence() {
        var retriever = KnowledgeContentRetriever.builder(store, SCOPE).instanceLevel(Sensitivity.CONFIDENTIAL).provider(LOCAL)
                .hops(1).excludeDocuments(Set.of("meeting:m-001")).build();

        List<Content> contents = retriever.retrieve(Query.from("checkout service retries a failed payment"));

        assertThat(contents).isNotEmpty();
        assertThat(contents).noneMatch(c -> "meeting:m-001".equals(c.textSegment().metadata().getString("documentId")));
        assertThat(contents).noneMatch(c -> {
            String evidence = c.textSegment().metadata().getString("evidence");
            return evidence != null && evidence.contains("meeting:m-001");
        });
    }

    @Test
    void the_budget_cuts_whole_items_and_the_audit_has_counts_only() {
        InMemoryAuditLog audit = new InMemoryAuditLog();
        TaskContext task = new TaskContext("/do/2/structure", "i-5", "c-5", 1, Json.obj(), DEFINITION, "chn-5", Lineage.NONE,
                null, Sensitivity.CONFIDENTIAL, null);
        var retriever = KnowledgeContentRetriever.builder(store, SCOPE).instanceLevel(Sensitivity.CONFIDENTIAL).provider(LOCAL)
                .budgetTokens(20).audit(audit, task, InstantSource.fixed(Instant.parse("2026-10-21T10:00:00Z"))).build();

        List<Content> contents = retriever.retrieve(Query.from("checkout service retries a failed payment"));

        assertThat(contents.size()).isLessThanOrEqualTo(1);
        List<AuditRecord> records = audit.of("i-5").toCompletableFuture().join();
        assertThat(records).hasSize(1);
        assertThat(records.get(0).detail()).containsKeys("mode", "hops", "trimmed", "internal").containsEntry("ceiling", "confidential");
        assertThat(records.get(0).toString()).doesNotContain("checkout", "retries");
    }

    @Test
    void a_failure_of_the_base_is_no_context_and_not_an_exception() {
        KnowledgeStore broken = new KnowledgeStore() {
            @Override
            public CompletionStage<List<ContextItem>> retrieve(KnowledgeScope scope, RetrievalQuery query) {
                return CompletableFuture.failedFuture(new IllegalStateException("SYNTH-PII-0001 must not be logged"));
            }

            @Override
            public CompletionStage<io.stintflow.knowledge.WriteReceipt> write(KnowledgeScope scope, KnowledgeBatch batch) {
                throw new UnsupportedOperationException();
            }

            @Override
            public CompletionStage<io.stintflow.knowledge.DeleteReceipt> delete(KnowledgeScope scope,
                    io.stintflow.knowledge.DeleteRequest request) {
                throw new UnsupportedOperationException();
            }

            @Override
            public CompletionStage<io.stintflow.knowledge.BaseInfo> describe(KnowledgeScope scope) {
                throw new UnsupportedOperationException();
            }

            @Override
            public CompletionStage<io.stintflow.knowledge.ReindexReceipt> reindex(KnowledgeScope scope,
                    io.stintflow.knowledge.ReindexRequest request) {
                throw new UnsupportedOperationException();
            }

            @Override
            public CompletionStage<io.stintflow.knowledge.ErasureCheck> verifyErased(KnowledgeScope scope,
                    io.stintflow.knowledge.DeleteRequest request) {
                throw new UnsupportedOperationException();
            }

            @Override
            public CompletionStage<List<io.stintflow.knowledge.ChangeEntry>> changes(KnowledgeScope scope, long after, int limit) {
                throw new UnsupportedOperationException();
            }

            @Override
            public CompletionStage<List<io.stintflow.knowledge.EntityMatch>> entities(KnowledgeScope scope,
                    io.stintflow.knowledge.EntityQuery query) {
                return CompletableFuture.completedFuture(List.of());
            }
        };

        var retriever = KnowledgeContentRetriever.builder(broken, SCOPE).instanceLevel(Sensitivity.INTERNAL).provider(LOCAL).build();

        assertThat(retriever.retrieve(Query.from("anything at all"))).isEmpty();
    }
}
