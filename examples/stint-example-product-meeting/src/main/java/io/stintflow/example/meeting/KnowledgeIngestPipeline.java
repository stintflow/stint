package io.stintflow.example.meeting;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import dev.langchain4j.model.chat.ChatModel;
import io.cloudevents.CloudEvent;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.dsl.DslLoader;
import io.stintflow.dsl.LoadOptions;
import io.stintflow.inmemory.InMemoryAuditLog;
import io.stintflow.inmemory.InMemoryEventPublisher;
import io.stintflow.inmemory.InMemoryStateStore;
import io.stintflow.inmemory.InMemoryTaskTransport;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.knowledge.DeterministicEmbedder;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.extraction.Glossary;
import io.stintflow.knowledge.inmemory.CompositeKnowledgeStore;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.RetentionClass;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerPool;
import io.stintflow.worker.WorkerRuntime;
import io.stintflow.worker.decision.SensitivityPolicy;
import io.stintflow.worker.knowledge.KnowledgeWorkers;
import io.stintflow.worker.knowledge.ScopeAllowList;
import io.stintflow.worker.langchain4j.KnowledgeExtractionWorker;

/**
 * The knowledge base learns from a published meeting (SDD 3b.3): the engine running {@code knowledge-ingest}, and its three
 * workers — {@code extract-knowledge}, {@code knowledge-write} and {@code knowledge-retrieve} — over a base in memory. It shares
 * the blob store with the product pipeline (the approved minutes rest there) and nothing else: the two definitions talk by facts.
 * The fact that starts it is the {@code minutes-published} of the product definition, handed to {@link #ingest}.
 */
public final class KnowledgeIngestPipeline implements AutoCloseable {

    public static final String WORKFLOW = "stint/workflows/knowledge-ingest.yaml";
    public static final KnowledgeScope SCOPE = KnowledgeScope.named("examples", "product-meetings");
    public static final RetentionClass BATCH = new RetentionClass("knowledge-batch", Duration.ofDays(7));
    public static final RetentionClass RESTRICTED = new RetentionClass("restricted", Duration.ofDays(30));

    final InMemoryStateStore state = new InMemoryStateStore();
    final InMemoryAuditLog audit = new InMemoryAuditLog();
    final InMemoryEventPublisher facts = new InMemoryEventPublisher();
    final CompositeKnowledgeStore store;
    final WorkflowEngine engine;
    final WorkflowDefinition definition;
    private final WorkerPool pool;
    /** The instance being run, once started. */
    volatile String current;

    public KnowledgeIngestPipeline(BlobStore blobs, ChatModel model) throws IOException {
        this(blobs, model, InstantSource.system(), true);
    }

    /**
     * @param model   the extraction model (a fake in the build, a local one in the manual run)
     * @param piiGate whether {@code knowledge-write} checks a batch for personal data (it does, unless a test switches it off)
     */
    public KnowledgeIngestPipeline(BlobStore blobs, ChatModel model, InstantSource clock, boolean piiGate) throws IOException {
        this(blobs, model, clock, new InMemoryTimerService(), piiGate);
    }

    /** @param timer the engine timers: a test ticks them to move a retry wait, with {@code clock} moved past its delay */
    public KnowledgeIngestPipeline(BlobStore blobs, ChatModel model, InstantSource clock, InMemoryTimerService timer,
            boolean piiGate) throws IOException {
        this.store = new CompositeKnowledgeStore(new DeterministicEmbedder(), blobs, RESTRICTED, clock);
        Corpus.KnowledgeBase base = Corpus.loadKnowledge().base();
        Glossary glossary = new Glossary("product", "1", base.glossary().stream()
                .map(t -> new Glossary.Term(t.id(), t.type(), t.name(), t.aliases(), null)).toList());
        ScopeAllowList scopes = new ScopeAllowList(Set.of(SCOPE));
        TaskHandlerRegistry handlers = new TaskHandlerRegistry().register("extract-knowledge", KnowledgeExtractionWorker.builder()
                .chatModel(model).provider("fake", false).policy(SensitivityPolicy.fromProperties(java.util.Map.of()))
                .glossaries(ref -> ref.equals("product@1") ? glossary : null).store(store).blobs(blobs, BATCH).build());
        KnowledgeWorkers.register(handlers, store, blobs, scopes, audit, clock);
        if (!piiGate) {
            handlers.register(KnowledgeWorkers.WRITE_ROUTE, new io.stintflow.worker.knowledge.KnowledgeWriteHandler(store,
                    blobs, scopes, false));
        }
        this.pool = new WorkerPool(new WorkerRuntime(handlers, blobs, io.stintflow.spi.ExecutionTracer.NOOP), 2);
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
        WorkflowRegistry registry = new WorkflowRegistry().requirePolicy();
        try (InputStream yaml = getClass().getClassLoader().getResourceAsStream(WORKFLOW)) {
            definition = new DslLoader().load(yaml, "knowledge-ingest.yaml", LoadOptions.STRICT);
        }
        registry.register(definition);
        this.engine = new WorkflowEngine(registry, transport, state, timer, blobs,
                new TreeInterpreter(new JqExpressionEvaluator()), clock, facts, null,
                io.stintflow.spi.ExecutionTracer.NOOP).auditLog(audit);
    }

    /** The facts this definition emitted: {@code document-indexed} and {@code ingest-rejected}. */
    public List<CloudEvent> emitted() {
        return facts.published();
    }

    /** Starts the ingestion from the fact and waits for it to end. Redelivering the same fact starts the same instance. */
    public InstanceSnapshot ingest(CloudEvent published, Duration patience) throws Exception {
        String id = engine.startFromEvent(definition.ref(), published).toCompletableFuture().get(30, TimeUnit.SECONDS);
        current = id;
        long until = System.nanoTime() + patience.toNanos();
        InstanceSnapshot snapshot = state.load(id).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow();
        while (snapshot.status() != InstanceStatus.COMPLETED && snapshot.status() != InstanceStatus.FAILED
                && snapshot.status() != InstanceStatus.ABORTED && System.nanoTime() < until) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
            snapshot = state.load(id).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow();
        }
        return snapshot;
    }

    @Override
    public void close() {
        pool.close();
    }
}
