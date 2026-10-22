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
import io.stintflow.knowledge.Embedder;
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
    public static final String ERASE_WORKFLOW = "stint/workflows/knowledge-erase.yaml";
    public static final String REINDEX_WORKFLOW = "stint/workflows/knowledge-reindex.yaml";
    public static final String AREA_KEY_WORKFLOW = "stint/workflows/area-key-destroy.yaml";
    public static final String ANNOUNCE_WORKFLOW = "stint/workflows/knowledge-announce.yaml";
    public static final String LINKS_WORKFLOW = "stint/workflows/architecture-links.yaml";
    public static final String RESOLVE_WORKFLOW = "stint/workflows/architecture-resolve.yaml";
    public static final String PRODUCT_CATALOG_WORKFLOW = "stint/workflows/product-catalog-link.yaml";
    public static final String ARCHITECTURE_CATALOG_WORKFLOW = "stint/workflows/architecture-catalog-link.yaml";
    public static final String AREA_ERASURE_REQUESTED = "io.stintflow.knowledge.area-erasure-requested.v1";
    public static final String ERASURE_REQUESTED = "io.stintflow.knowledge.erasure-requested.v1";
    public static final String EMBEDDING_CHANGED = "io.stintflow.knowledge.embedding-changed.v1";
    public static final KnowledgeScope SCOPE = KnowledgeScope.named("examples", "product-meetings");
    /** SDD 3c.1: the second domain of the example, whose graph points at the product domain's nodes. */
    public static final KnowledgeScope ARCHITECTURE = KnowledgeScope.named("examples", "architecture");
    /** SDD 3c.3: the third domain of the example, which holds the catalog of systems, products and teams. */
    public static final KnowledgeScope CATALOG = KnowledgeScope.named("examples", "catalog");
    public static final RetentionClass BATCH = new RetentionClass("knowledge-batch", Duration.ofDays(7));
    public static final RetentionClass RESTRICTED = new RetentionClass("restricted", Duration.ofDays(30));

    final InMemoryStateStore state = new InMemoryStateStore();
    final InMemoryAuditLog audit = new InMemoryAuditLog();
    final InMemoryEventPublisher facts = new InMemoryEventPublisher();
    final CompositeKnowledgeStore store;
    /** SDD 3c.1: the architecture domain's base. */
    final CompositeKnowledgeStore architecture;
    /** SDD 3c.3: the catalog domain's base. */
    final CompositeKnowledgeStore catalog;
    final WorkflowEngine engine;
    final WorkflowDefinition definition;
    final WorkflowDefinition eraseDefinition;
    final WorkflowDefinition reindexDefinition;
    final WorkflowDefinition areaKeyDefinition;
    final WorkflowDefinition announceDefinition;
    final WorkflowDefinition linksDefinition;
    final WorkflowDefinition resolveDefinition;
    final WorkflowDefinition productCatalogDefinition;
    final WorkflowDefinition architectureCatalogDefinition;
    /** SDD 3c.2: when true the product domain's {@code resolve-pointers} does not answer (the origin is down). */
    private volatile boolean resolveDown;
    /** SDD 3c.3: when true the catalog domain's {@code catalog-lookup} does not answer. */
    private volatile boolean catalogDown;
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
        this(blobs, model, clock, timer, piiGate, null);
    }

    /**
     * @param reindexTarget the embedding model the worker of {@code knowledge-reindex} serves, or {@code null}: then there is no
     *                      reindex route (SDD 3b.5)
     */
    public KnowledgeIngestPipeline(BlobStore blobs, ChatModel model, InstantSource clock, InMemoryTimerService timer,
            boolean piiGate, Embedder reindexTarget) throws IOException {
        this(blobs, model, clock, timer, piiGate, reindexTarget, null);
    }

    /**
     * @param areaKeys the keys of the areas the worker serves, or {@code null}: then there is no {@code area-key-destroy} route
     *                 (SDD 3b.5b). The area served is the namespace of the base.
     */
    public KnowledgeIngestPipeline(BlobStore blobs, ChatModel model, InstantSource clock, InMemoryTimerService timer,
            boolean piiGate, Embedder reindexTarget, io.stintflow.spi.AreaKeys areaKeys) throws IOException {
        this.store = new CompositeKnowledgeStore(new DeterministicEmbedder(), blobs, RESTRICTED, clock);
        this.architecture = new CompositeKnowledgeStore(new DeterministicEmbedder(), blobs, RESTRICTED, clock);
        Corpus.KnowledgeBase base = Corpus.loadKnowledge().base();
        Glossary glossary = new Glossary("product", "1", base.glossary().stream()
                .map(t -> new Glossary.Term(t.id(), t.type(), t.name(), t.aliases(), null)).toList());
        ScopeAllowList scopes = new ScopeAllowList(Set.of(SCOPE));
        TaskHandlerRegistry handlers = new TaskHandlerRegistry().register("extract-knowledge", KnowledgeExtractionWorker.builder()
                .chatModel(model).provider("fake", false).policy(SensitivityPolicy.fromProperties(java.util.Map.of()))
                .glossaries(ref -> ref.equals("product@1") ? glossary : null).store(store).blobs(blobs, BATCH).build());
        KnowledgeWorkers.register(handlers, store, blobs, scopes, audit, clock);
        KnowledgeWorkers.registerErasure(handlers, store, blobs, scopes, audit, clock, reindexTarget,
                ProductMeetingPipeline.pseudonyms());
        // SDD 3c.1: the product domain announces its nodes; the architecture domain links to them, listening to the product one only
        KnowledgeWorkers.registerAnnounce(handlers, store, scopes, audit, clock);
        KnowledgeWorkers.registerReference(handlers, architecture, new ScopeAllowList(Set.of(ARCHITECTURE)),
                new io.stintflow.worker.knowledge.OriginAllowList(Set.of(SCOPE)), audit, clock);
        // SDD 3c.2: the product domain answers the pointers of the architecture domain, by its own policy and through the view it publishes
        // (SDD 3c.5: the policy is built from the contract of exposure of the base, a document)
        io.stintflow.worker.knowledge.ResolutionPolicy resolution = ProductBridge.contract().policy();
        TaskHandlerRegistry resolving = new TaskHandlerRegistry();
        KnowledgeWorkers.registerResolve(resolving, store, store, resolution, blobs, null, audit, clock);
        io.stintflow.worker.TaskHandler resolveHandler = resolving.find(KnowledgeWorkers.RESOLVE_ROUTE).orElseThrow();
        handlers.register(KnowledgeWorkers.RESOLVE_ROUTE, ctx -> resolveDown ? CompletableFuture.failedFuture(
                new io.stintflow.worker.TaskFailure(io.stintflow.spi.ErrorInfo.StandardType.COMMUNICATION.uri(), 503,
                        "The origin does not answer", null)) : resolveHandler.execute(ctx));
        // SDD 3c.3: the catalog domain answers which node of the catalog a name is; each area names its nodes and links them by pointer
        this.catalog = new CompositeKnowledgeStore(new DeterministicEmbedder(), blobs, RESTRICTED, clock);
        catalog.write(CATALOG, io.stintflow.knowledge.CatalogSeed.of(catalogGlossary(base), 1)).toCompletableFuture().join();
        TaskHandlerRegistry cataloging = new TaskHandlerRegistry();
        KnowledgeWorkers.registerCatalog(cataloging, catalog, new ScopeAllowList(Set.of(CATALOG)),
                new io.stintflow.worker.knowledge.CatalogRequesters(Set.of("examples:product-catalog-link",
                        "examples:architecture-catalog-link")), audit, clock);
        io.stintflow.worker.TaskHandler lookupHandler = cataloging.find(KnowledgeWorkers.CATALOG_LOOKUP_ROUTE).orElseThrow();
        handlers.register(KnowledgeWorkers.CATALOG_LOOKUP_ROUTE, ctx -> catalogDown ? CompletableFuture.failedFuture(
                new io.stintflow.worker.TaskFailure(io.stintflow.spi.ErrorInfo.StandardType.COMMUNICATION.uri(), 503,
                        "The catalog does not answer", null)) : lookupHandler.execute(ctx));
        io.stintflow.worker.knowledge.OriginAllowList fromCatalog = new io.stintflow.worker.knowledge.OriginAllowList(Set.of(CATALOG));
        TaskHandlerRegistry productLinks = new TaskHandlerRegistry();
        KnowledgeWorkers.registerCatalogLinks(productLinks, store, store, store, new ScopeAllowList(Set.of(SCOPE)), fromCatalog, audit, clock);
        TaskHandlerRegistry architectureLinks = new TaskHandlerRegistry();
        KnowledgeWorkers.registerCatalogLinks(architectureLinks, architecture, architecture, architecture,
                new ScopeAllowList(Set.of(ARCHITECTURE)), fromCatalog, audit, clock);
        for (String route : List.of(KnowledgeWorkers.CATALOG_MENTIONS_ROUTE, KnowledgeWorkers.CATALOG_LINK_ROUTE)) {
            io.stintflow.worker.TaskHandler forProduct = productLinks.find(route).orElseThrow();
            io.stintflow.worker.TaskHandler forArchitecture = architectureLinks.find(route).orElseThrow();
            handlers.register(route, ctx -> ARCHITECTURE.base().equals(ctx.input().path("scope").path("base").asText())
                    ? forArchitecture.execute(ctx) : forProduct.execute(ctx));
        }
        if (areaKeys != null) {
            KnowledgeWorkers.registerAreaKeys(handlers, areaKeys, Set.of(SCOPE.namespace()), audit, clock);
        }
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
        definition = load(WORKFLOW, "knowledge-ingest.yaml");
        eraseDefinition = load(ERASE_WORKFLOW, "knowledge-erase.yaml");
        reindexDefinition = load(REINDEX_WORKFLOW, "knowledge-reindex.yaml");
        areaKeyDefinition = load(AREA_KEY_WORKFLOW, "area-key-destroy.yaml");
        announceDefinition = load(ANNOUNCE_WORKFLOW, "knowledge-announce.yaml");
        linksDefinition = load(LINKS_WORKFLOW, "architecture-links.yaml");
        resolveDefinition = load(RESOLVE_WORKFLOW, "architecture-resolve.yaml");
        productCatalogDefinition = load(PRODUCT_CATALOG_WORKFLOW, "product-catalog-link.yaml");
        architectureCatalogDefinition = load(ARCHITECTURE_CATALOG_WORKFLOW, "architecture-catalog-link.yaml");
        registry.register(definition);
        registry.register(eraseDefinition);
        registry.register(reindexDefinition);
        registry.register(areaKeyDefinition);
        registry.register(announceDefinition);
        registry.register(linksDefinition);
        registry.register(resolveDefinition);
        registry.register(productCatalogDefinition);
        registry.register(architectureCatalogDefinition);
        this.engine = new WorkflowEngine(registry, transport, state, timer, blobs,
                new TreeInterpreter(new JqExpressionEvaluator()), clock, facts, null,
                io.stintflow.spi.ExecutionTracer.NOOP).auditLog(audit);
    }

    private WorkflowDefinition load(String resource, String name) throws IOException {
        try (InputStream yaml = getClass().getClassLoader().getResourceAsStream(resource)) {
            return new DslLoader().load(yaml, name, LoadOptions.STRICT);
        }
    }

    /** The facts the definitions emitted: {@code document-indexed} and {@code ingest-rejected}. */
    public List<CloudEvent> emitted() {
        return facts.published();
    }

    /** Starts the ingestion from the fact and waits for it to end. Redelivering the same fact starts the same instance. */
    public InstanceSnapshot ingest(CloudEvent published, Duration patience) throws Exception {
        return run(definition, published, patience);
    }

    /** SDD 3b.5: starts the erasure from the request and waits for it to end. */
    public InstanceSnapshot erase(CloudEvent requested, Duration patience) throws Exception {
        return run(eraseDefinition, requested, patience);
    }

    /** SDD 3b.5: starts the reindexation from the fact and waits for it to end. */
    public InstanceSnapshot reindex(CloudEvent changed, Duration patience) throws Exception {
        return run(reindexDefinition, changed, patience);
    }

    /** SDD 3c.1: starts the announcement of the nodes of an indexed document from the {@code document-indexed} fact. */
    public InstanceSnapshot announce(CloudEvent indexed, Duration patience) throws Exception {
        return run(announceDefinition, indexed, patience);
    }

    /** SDD 3c.1: starts the linking of the architecture domain from the {@code nodes-published} fact. */
    public InstanceSnapshot link(CloudEvent published, Duration patience) throws Exception {
        return run(linksDefinition, published, patience);
    }

    /** SDD 3c.2: starts the resolution of the pointers a {@code references-linked} fact carries. */
    public InstanceSnapshot resolve(CloudEvent linked, Duration patience) throws Exception {
        return run(resolveDefinition, linked, patience);
    }

    /** SDD 3c.2: makes the product domain stop answering the pointers (or answer again), for the test of an origin that is down. */
    public void resolveDown(boolean down) {
        this.resolveDown = down;
    }

    /** SDD 3c.3: the catalog of the example: the systems of the product glossary (one with another spelling), a product and a team. */
    static io.stintflow.knowledge.extraction.Glossary catalogGlossary(Corpus.KnowledgeBase base) {
        List<io.stintflow.knowledge.extraction.Glossary.Term> terms = new java.util.ArrayList<>();
        base.glossary().stream().filter(t -> t.type().equals("System")).forEach(t -> {
            List<String> aliases = new java.util.ArrayList<>(t.aliases());
            if (t.id().equals("sys-checkout")) {
                aliases.add("checkout-service");
            }
            terms.add(new io.stintflow.knowledge.extraction.Glossary.Term(t.id(), t.type(), t.name(), aliases, null));
        });
        terms.add(new io.stintflow.knowledge.extraction.Glossary.Term("prod-shop", "Product", "Online Shop"));
        terms.add(new io.stintflow.knowledge.extraction.Glossary.Term("team-payments", "Team", "Payments Team"));
        return new io.stintflow.knowledge.extraction.Glossary("catalog", "1", terms);
    }

    /** SDD 3c.3: starts the linking of the product domain's nodes to the catalog from the {@code document-indexed} fact. */
    public InstanceSnapshot catalogLinkProduct(CloudEvent indexed, Duration patience) throws Exception {
        return run(productCatalogDefinition, indexed, patience);
    }

    /** SDD 3c.3: starts the linking of the architecture domain's nodes to the catalog from the {@code references-linked} fact. */
    public InstanceSnapshot catalogLinkArchitecture(CloudEvent linked, Duration patience) throws Exception {
        return run(architectureCatalogDefinition, linked, patience);
    }

    /** SDD 3c.3: makes the catalog stop answering (or answer again), for the test of a catalog that is down. */
    public void catalogDown(boolean down) {
        this.catalogDown = down;
    }

    /** SDD 3b.5b: starts the destruction of an area's key from the request and waits for it to end. */
    public InstanceSnapshot destroyAreaKey(CloudEvent requested, Duration patience) throws Exception {
        return run(areaKeyDefinition, requested, patience);
    }

    private InstanceSnapshot run(WorkflowDefinition which, CloudEvent fact, Duration patience) throws Exception {
        String id = engine.startFromEvent(which.ref(), fact).toCompletableFuture().get(30, TimeUnit.SECONDS);
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
