package io.stintflow.example.meeting;

import java.net.URI;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import io.stintflow.example.meeting.Corpus.Meeting;
import io.stintflow.knowledge.DeterministicEmbedder;
import io.stintflow.knowledge.extraction.Glossary;
import io.stintflow.knowledge.inmemory.CompositeKnowledgeStore;
import io.stintflow.spi.AuditLog;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.ExecutionTracer;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.decision.DecisionConfig;
import io.stintflow.worker.decision.DecisionHandlers;
import io.stintflow.worker.decision.SensitivityPolicy;
import io.stintflow.worker.knowledge.ChannelAttestation;
import io.stintflow.worker.knowledge.KnowledgeWorkers;
import io.stintflow.worker.knowledge.OriginAllowList;
import io.stintflow.worker.knowledge.ResolutionPolicy;
import io.stintflow.worker.knowledge.ScopeAllowList;
import io.stintflow.worker.langchain4j.KnowledgeExtractionWorker;
import io.stintflow.worker.publishing.ConfluencePagePublisher;
import io.stintflow.worker.publishing.PublishHandler;

/**
 * The workers of the whole scenario of the milestone (SDD 3c.6): the product area's (transcription, redaction, minutes,
 * decisions, publication), the product knowledge base (extraction, write, announce) with the bridge it publishes, the
 * architecture area's references and ADR, and the conflict decision. One registry, so one worker serves the engine in the
 * process or behind a queue. The wiring is the one of the pipelines of 3b.3 and 3c.4, with the product base shared by the
 * chain; the fakes are the CI's.
 */
public final class ScenarioWorkers {

    /** The product area's base and the architecture area's. */
    public final CompositeKnowledgeStore product;
    public final CompositeKnowledgeStore architecture;
    public final TaskHandlerRegistry handlers;
    private volatile boolean originDown;

    /**
     * @param confluence a Confluence that holds the spaces {@code PRODUCT} and {@code ARCHITECTURE}
     * @param audit      where the workers and the engine record
     */
    public ScenarioWorkers(List<Meeting> meetings, ProductMeetingPipeline.Stack stack, RunMetrics metrics, BlobStore blobs,
            ExecutionTracer tracer, URI confluence, InstantSource clock, AuditLog audit) {
        this.product = new CompositeKnowledgeStore(new DeterministicEmbedder(), blobs, KnowledgeIngestPipeline.RESTRICTED, clock);
        this.architecture = new CompositeKnowledgeStore(new DeterministicEmbedder(), blobs, KnowledgeIngestPipeline.RESTRICTED, clock);
        this.handlers = ProductMeetingPipeline.handlers(meetings, stack, metrics, blobs, tracer, confluence, null, audit);

        // the product base: extraction, write, announce
        Corpus.KnowledgeBase base = Corpus.loadKnowledge().base();
        Glossary glossary = new Glossary("product", "1", base.glossary().stream()
                .map(t -> new Glossary.Term(t.id(), t.type(), t.name(), t.aliases(), null)).toList());
        ScopeAllowList productScope = new ScopeAllowList(Set.of(KnowledgeIngestPipeline.SCOPE));
        handlers.register("extract-knowledge", KnowledgeExtractionWorker.builder()
                .chatModel(new FakeKnowledgeModel(base.glossary(), Corpus.loadProductDecisions())).provider("fake", false)
                .policy(SensitivityPolicy.fromProperties(Map.of()))
                .glossaries(ref -> ref.equals("product@1") ? glossary : null).store(product)
                .blobs(blobs, KnowledgeIngestPipeline.BATCH).build());
        KnowledgeWorkers.register(handlers, product, blobs, productScope, audit, clock);
        KnowledgeWorkers.registerAnnounce(handlers, product, productScope, audit, clock);
        // the architecture area links to what the product area announced, and only to that
        KnowledgeWorkers.registerReference(handlers, architecture, new ScopeAllowList(Set.of(KnowledgeIngestPipeline.ARCHITECTURE)),
                new OriginAllowList(Set.of(KnowledgeIngestPipeline.SCOPE)), audit, clock);

        // the product area answers the pointers by the contract of exposure of its base; the channel is declared
        ResolutionPolicy policy = ProductBridge.contract().policy();
        TaskHandlerRegistry resolving = new TaskHandlerRegistry();
        KnowledgeWorkers.registerResolve(resolving, product, product, policy, blobs, null, audit, clock,
                new ChannelAttestation("scenario-channel", true));
        TaskHandler resolve = resolving.find(KnowledgeWorkers.RESOLVE_ROUTE).orElseThrow();
        handlers.register(KnowledgeWorkers.RESOLVE_ROUTE, ctx -> originDown ? CompletableFuture.failedFuture(
                new TaskFailure(io.stintflow.spi.ErrorInfo.StandardType.COMMUNICATION.uri(), 503, "The origin does not answer", null))
                : resolve.execute(ctx));

        // the ADR: the conflict decision, the pairs, the writing and the publication
        var conflict = FakeDecisions.conflict(blobs);
        DecisionHandlers.builder(DecisionConfig.fromProperties(Map.of(
                        "stint.decision.routes.decide-conflict.adapter", conflict.descriptor().id(),
                        "stint.decision.routes.decide-conflict.instructions",
                        "Does the architecture decision contradict the product decision, or make it impossible?")))
                .adapter(conflict).policy(SensitivityPolicy.fromProperties(Map.of())).usage(metrics.decisionUsage()).build()
                .registerAll(handlers);
        KnowledgeWorkers.registerAdrs(handlers, architecture, blobs, new ScopeAllowList(Set.of(ArchitectureAdrPipeline.ARCHITECTURE)),
                new OriginAllowList(Set.of(ArchitectureAdrPipeline.PRODUCT)), ArchitectureAdrPipeline.MATERIAL,
                ArchitectureAdrPipeline.DOCUMENT, audit, clock);
        handlers.register("publish-adr", new PublishAdrsHandler(new PublishHandler(ConfluencePagePublisher.builder()
                .baseUri(confluence).space(ArchitectureAdrPipeline.SPACE).email(ProductMeetingPipeline.CONFLUENCE_EMAIL)
                .apiToken(ProductMeetingPipeline.CONFLUENCE_TOKEN).build(), blobs, ArchitectureAdrPipeline.SPACE, audit, clock)));
    }

    /** Makes the product area stop answering the pointers (or answer again). */
    public void originDown(boolean down) {
        this.originDown = down;
    }
}
