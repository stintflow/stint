package io.stintflow.example.meeting;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.nio.file.Files;
import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.dsl.DslLoader;
import io.stintflow.dsl.LoadOptions;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryAuditLog;
import io.stintflow.inmemory.InMemoryEventPublisher;
import io.stintflow.inmemory.InMemoryStateStore;
import io.stintflow.inmemory.InMemoryTaskTransport;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.knowledge.DeterministicEmbedder;
import io.stintflow.knowledge.Entity;
import io.stintflow.knowledge.EntityIds;
import io.stintflow.knowledge.KnowledgeBatch;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.Link;
import io.stintflow.knowledge.Passage;
import io.stintflow.knowledge.PassageIds;
import io.stintflow.knowledge.Relation;
import io.stintflow.knowledge.Source;
import io.stintflow.knowledge.inmemory.CompositeKnowledgeStore;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.RetentionClass;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.CeWire;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerPool;
import io.stintflow.worker.WorkerRuntime;
import io.stintflow.worker.decision.Decision;
import io.stintflow.worker.decision.DecisionConfig;
import io.stintflow.worker.decision.DecisionHandlers;
import io.stintflow.worker.decision.FakeDecisionAdapter;
import io.stintflow.worker.decision.SensitivityPolicy;
import io.stintflow.worker.knowledge.KnowledgeWorkers;
import io.stintflow.worker.knowledge.ScopeAllowList;

/**
 * SDD 3b.4, CA11: a decision worker chooses how a question is answered, in the workflow, and the choice reaches
 * {@code knowledge-retrieve}. No code of the base knows about the decision: the step is the DSL's. Synthetic data only.
 */
class KnowledgeRecallTest {

    private static final KnowledgeScope SCOPE = KnowledgeIngestPipeline.SCOPE;

    private record Run(InstanceSnapshot snapshot, JsonNode fact) {
    }

    private Run ask(String question, FakeDecisionAdapter router) throws Exception {
        FilesystemBlobStore blobs = new FilesystemBlobStore(Files.createTempDirectory("stint-3b4-recall"));
        CompositeKnowledgeStore store = new CompositeKnowledgeStore(new DeterministicEmbedder(), blobs,
                new RetentionClass("restricted", Duration.ofDays(30)), InstantSource.system());
        Entity old = new Entity(EntityIds.of("Decision", "Old retry policy"), "Decision", "Old retry policy", Sensitivity.INTERNAL);
        Entity renewed = new Entity(EntityIds.of("Decision", "New retry policy"), "Decision", "New retry policy", Sensitivity.INTERNAL);
        write(store, "m-001", "We keep the old retry policy for checkout.", List.of(old), List.of(), old);
        write(store, "m-002", "The plan for payments is a longer story.", List.of(renewed, old),
                List.of(new Relation("supersedes", renewed.entityId(), old.entityId(), Sensitivity.INTERNAL)), renewed);

        SensitivityPolicy policy = SensitivityPolicy.fromProperties(Map.of());
        TaskHandlerRegistry handlers = DecisionHandlers.builder(DecisionConfig.fromProperties(Map.of(
                "stint.decision.routes.route-question.adapter", router.descriptor().id(),
                "stint.decision.routes.route-question.instructions", "Is the question about how named things relate?")))
                .adapter(router).policy(policy).build().registerAll(new TaskHandlerRegistry());
        InMemoryAuditLog audit = new InMemoryAuditLog();
        KnowledgeWorkers.register(handlers, store, blobs, new ScopeAllowList(Set.of(SCOPE)), audit, InstantSource.system());
        InMemoryEventPublisher facts = new InMemoryEventPublisher();
        InMemoryStateStore state = new InMemoryStateStore();
        try (WorkerPool pool = new WorkerPool(new WorkerRuntime(handlers, blobs, io.stintflow.spi.ExecutionTracer.NOOP), 2)) {
            InMemoryTaskTransport transport = new InMemoryTaskTransport();
            transport.connectWorker(invoke -> {
                CompletableFuture<CloudEvent> result = new CompletableFuture<>();
                try {
                    pool.submit(invoke, event -> {
                        result.complete(event);
                        return CompletableFuture.completedFuture(null);
                    }, () -> { });
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    result.completeExceptionally(e);
                }
                return result;
            });
            WorkflowDefinition definition;
            try (InputStream yaml = getClass().getClassLoader().getResourceAsStream("stint/workflows/knowledge-recall.yaml")) {
                definition = new DslLoader().load(yaml, "knowledge-recall.yaml", LoadOptions.STRICT);
            }
            WorkflowRegistry registry = new WorkflowRegistry().requirePolicy();
            registry.register(definition);
            WorkflowEngine engine = new WorkflowEngine(registry, transport, state, new InMemoryTimerService(), blobs,
                    new TreeInterpreter(new JqExpressionEvaluator()), InstantSource.system(), facts, null,
                    io.stintflow.spi.ExecutionTracer.NOOP).auditLog(audit);
            CloudEvent asked = CloudEventBuilder.v1().withId("q-1").withSource(java.net.URI.create("https://ask.example"))
                    .withType("io.stintflow.examples.question-asked.v1").withSubject("q-1")
                    .withData("application/json", Json.bytes(Json.obj().put("questionId", "q-1").put("question", question)
                            .put("label", "internal"))).withExtension("stintsensitivity", "internal").build();
            String id = engine.startFromEvent(definition.ref(), asked).toCompletableFuture().get(30, TimeUnit.SECONDS);
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            InstanceSnapshot snapshot = state.load(id).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow();
            while (snapshot.status() != InstanceStatus.COMPLETED && snapshot.status() != InstanceStatus.FAILED
                    && System.nanoTime() < until) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
                snapshot = state.load(id).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow();
            }
            JsonNode fact = null;
            while (fact == null && System.nanoTime() < until) {   // the fact leaves a moment after the instance ends
                fact = facts.published().stream().filter(e -> e.getType().endsWith("question-answered.v1")).findFirst()
                        .map(e -> Json.read(e.getData().toBytes())).orElse(null);
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
            }
            return new Run(snapshot, fact);
        }
    }

    private static void write(CompositeKnowledgeStore store, String meeting, String text, List<Entity> entities,
            List<Relation> relations, Entity mentioned) {
        String documentId = "meeting:" + meeting;
        Passage passage = new Passage(PassageIds.of(SCOPE, documentId, 0, text), 0, text, "en", Sensitivity.INTERNAL);
        store.write(SCOPE, new KnowledgeBatch("extract:" + meeting, documentId, PassageIds.sha256Hex(text + entities), 1L,
                new Source("i-" + meeting, "chn-" + meeting, new WorkflowRef("examples", "knowledge-ingest", "1.0.0")),
                "meetings@1.0.0", List.of(passage), entities, relations, List.of(new Link(passage.passageId(),
                        mentioned.entityId())))).toCompletableFuture().join();
    }

    @Test
    void ca11_the_decision_worker_chooses_the_mode_and_it_reaches_the_retrieval() throws Exception {
        FakeDecisionAdapter router = new FakeDecisionAdapter("router", request -> Decision.of(TextNode.valueOf("relational"), 0.95));

        Run run = ask("Which decision superseded the old retry policy?", router);

        assertThat(run.snapshot().status()).isEqualTo(InstanceStatus.COMPLETED);
        assertThat(router.requests).hasSize(1);
        assertThat(router.requests.getFirst().input().path("question").asText()).contains("superseded");
        assertThat(run.fact().path("asked").asText()).isEqualTo("relational");
        assertThat(run.fact().path("mode").asText()).isEqualTo("relational");   // the base found a named entity
        assertThat(run.fact().path("counts").path("internal").asInt()).isPositive();
        assertThat(run.fact().toString()).doesNotContain("superseded", "retry policy");   // ids and counts only
    }

    @Test
    void ca11_a_decision_for_a_semantic_answer_is_honoured() throws Exception {
        FakeDecisionAdapter router = new FakeDecisionAdapter("router", request -> Decision.of(TextNode.valueOf("semantic"), 0.95));

        Run run = ask("Which decision superseded the old retry policy?", router);

        assertThat(run.snapshot().status()).isEqualTo(InstanceStatus.COMPLETED);
        assertThat(run.fact().path("asked").asText()).isEqualTo("semantic");
        assertThat(run.fact().path("mode").asText()).isEqualTo("semantic");
    }
}
