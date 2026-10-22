package io.stintflow.example.meeting;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
import io.stintflow.inmemory.InMemoryStateStore;
import io.stintflow.inmemory.InMemoryTaskTransport;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.knowledge.DeterministicEmbedder;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.inmemory.CompositeKnowledgeStore;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.RetentionClass;
import io.stintflow.spi.Sensitivity;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerPool;
import io.stintflow.worker.WorkerRuntime;
import io.stintflow.worker.decision.DecisionConfig;
import io.stintflow.worker.decision.DecisionHandlers;
import io.stintflow.worker.decision.SensitivityPolicy;
import io.stintflow.worker.knowledge.Grant;
import io.stintflow.worker.knowledge.KnowledgeWorkers;
import io.stintflow.worker.knowledge.OriginAllowList;
import io.stintflow.worker.knowledge.PublishedType;
import io.stintflow.worker.knowledge.PublishedView;
import io.stintflow.worker.knowledge.ResolutionPolicy;
import io.stintflow.worker.knowledge.ScopeAllowList;
import io.stintflow.worker.publishing.ConfluencePagePublisher;
import io.stintflow.worker.publishing.FakeConfluenceServer;
import io.stintflow.worker.publishing.PublishHandler;

/**
 * The architecture domain's ADRs, wired (SDD 3c.4): the engine running {@code architecture-adr} and its child, over a product
 * domain that answers {@code resolve-pointers} by its own policy, a decision task that judges a pair, a review channel the people
 * answer through, and a local Confluence with one space, {@code ARCHITECTURE}. The product domain's base is the caller's to fill
 * ({@link #product}); the draft of the decisions is a blob it writes ({@link #draft}). Fake models, loopback only, synthetic data.
 */
public final class ArchitectureAdrPipeline implements AutoCloseable {

    public static final String SPACE = "ARCHITECTURE";
    public static final String DRAFTED = "io.stintflow.examples.architecture-decisions-drafted.v1";
    public static final String PUBLISHED = "io.stintflow.examples.adrs-published.v1";
    public static final KnowledgeScope PRODUCT = KnowledgeIngestPipeline.SCOPE;
    public static final KnowledgeScope ARCHITECTURE = KnowledgeIngestPipeline.ARCHITECTURE;
    /** The version of the product view that carries the summary, granted to {@code architecture-adr} only. */
    public static final String ADR_VIEW = "product-adr-v1";
    public static final RetentionClass MATERIAL = new RetentionClass("adr-material", Duration.ofDays(1));
    public static final RetentionClass DOCUMENT = new RetentionClass("adr-document", Duration.ofDays(30));
    public static final RetentionClass DRAFT = new RetentionClass("adr-draft", Duration.ofDays(7));

    final FilesystemBlobStore blobs;
    final ReviewChannel channel;
    final FakeConfluenceServer confluence;
    final InMemoryStateStore state = new InMemoryStateStore();
    final InMemoryAuditLog audit = new InMemoryAuditLog();
    /** The product domain's base and the architecture domain's. */
    final CompositeKnowledgeStore product;
    final CompositeKnowledgeStore architecture;
    final WorkflowEngine engine;
    final WorkflowDefinition definition;
    final InMemoryTimerService timer;
    /** The product domain's {@code resolve-pointers}, as the engine reaches it, for a test that asks as another definition. */
    final TaskHandler resolveHandler;
    private final WorkerPool pool;
    private final HttpClient http = HttpClient.newHttpClient();
    private volatile boolean originDown;
    /** The instance being run, once started. */
    volatile String current;

    public ArchitectureAdrPipeline(Path workDir, InstantSource clock, InMemoryTimerService timer) throws IOException {
        this.timer = timer;
        this.blobs = new FilesystemBlobStore(Files.createDirectories(workDir.resolve("blobs")), clock);
        this.channel = new ReviewChannel(URI.create(ProductMeetingPipeline.REVIEW_SOURCE));
        this.confluence = new FakeConfluenceServer(ProductMeetingPipeline.CONFLUENCE_EMAIL, ProductMeetingPipeline.CONFLUENCE_TOKEN,
                SPACE);
        this.product = new CompositeKnowledgeStore(new DeterministicEmbedder(), blobs, KnowledgeIngestPipeline.RESTRICTED, clock);
        this.architecture = new CompositeKnowledgeStore(new DeterministicEmbedder(), blobs, KnowledgeIngestPipeline.RESTRICTED, clock);

        // the product domain answers by its own policy: the definition that checks conflicts gets the view with the summary, the others do not
        io.stintflow.knowledge.schema.KnowledgeSchema meetings = io.stintflow.knowledge.schema.KnowledgeSchema.meetings();
        ResolutionPolicy policy = new ResolutionPolicy(
                List.of(new Grant("examples", "architecture-resolve", Set.of(PRODUCT), Set.of("Decision", "Requirement", "System"),
                                Sensitivity.CONFIDENTIAL),
                        new Grant("examples", "architecture-adr", Set.of(PRODUCT), Set.of("Decision"), Sensitivity.CONFIDENTIAL,
                                ADR_VIEW)),
                List.of(PublishedView.of("product-v1", PRODUCT, meetings, List.of(
                                PublishedType.of("Decision", true, Set.of("status"), Set.of("supersedes", "affects")),
                                PublishedType.of("Requirement", true, Set.of(), Set.of()),
                                PublishedType.of("System", true, Set.of(), Set.of())), false),
                        PublishedView.of(ADR_VIEW, PRODUCT, meetings, List.of(
                                PublishedType.of("Decision", true, Set.of("status", "summary"), Set.of())), true)));
        TaskHandlerRegistry resolving = new TaskHandlerRegistry();
        KnowledgeWorkers.registerResolve(resolving, product, product, policy, blobs, null, audit, clock);
        TaskHandler resolve = resolving.find(KnowledgeWorkers.RESOLVE_ROUTE).orElseThrow();
        this.resolveHandler = resolve;

        SensitivityPolicy sensitivity = SensitivityPolicy.fromProperties(Map.of());
        var conflict = FakeDecisions.conflict(blobs);
        TaskHandlerRegistry handlers = DecisionHandlers.builder(DecisionConfig.fromProperties(Map.of(
                        "stint.decision.routes.decide-conflict.adapter", conflict.descriptor().id(),
                        "stint.decision.routes.decide-conflict.instructions",
                        "Does the architecture decision contradict the product decision, or make it impossible?")))
                .adapter(conflict).policy(sensitivity).build().registerAll(new TaskHandlerRegistry());
        handlers.register(KnowledgeWorkers.RESOLVE_ROUTE, ctx -> originDown ? CompletableFuture.failedFuture(
                new TaskFailure(io.stintflow.spi.ErrorInfo.StandardType.COMMUNICATION.uri(), 503, "The origin does not answer", null))
                : resolve.execute(ctx));
        KnowledgeWorkers.registerAdrs(handlers, architecture, blobs, new ScopeAllowList(Set.of(ARCHITECTURE)),
                new OriginAllowList(Set.of(PRODUCT)), MATERIAL, DOCUMENT, audit, clock);
        handlers.register("publish-adr", new PublishAdrsHandler(new PublishHandler(ConfluencePagePublisher.builder()
                .baseUri(confluence.baseUri()).space(SPACE).email(ProductMeetingPipeline.CONFLUENCE_EMAIL)
                .apiToken(ProductMeetingPipeline.CONFLUENCE_TOKEN).build(), blobs, SPACE)));
        this.pool = new WorkerPool(new WorkerRuntime(handlers, blobs, io.stintflow.spi.ExecutionTracer.NOOP), 4);
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
        definition = load("architecture-adr.yaml");
        registry.register(definition);
        registry.register(load("architecture-conflict-check.yaml"));
        this.engine = new WorkflowEngine(registry, transport, state, timer, blobs, new TreeInterpreter(new JqExpressionEvaluator()),
                clock, channel, null, io.stintflow.spi.ExecutionTracer.NOOP).auditLog(audit);
        channel.answersTo(engine);
    }

    private WorkflowDefinition load(String name) throws IOException {
        try (InputStream yaml = getClass().getClassLoader().getResourceAsStream("stint/workflows/" + name)) {
            return new DslLoader().load(yaml, name, LoadOptions.STRICT);
        }
    }

    /** Makes the product domain stop answering the pointers (or answer again). */
    public void originDown(boolean down) {
        this.originDown = down;
    }

    /** Writes the draft of the decisions of a meeting, a blob; its pointer goes in the fact. */
    public URI draft(String meetingId, JsonNode decisions) {
        ObjectNode draft = Json.obj();
        draft.set("decisions", decisions);
        return blobs.put(Json.bytes(draft), "adr-draft/" + meetingId + ".json", DRAFT).toCompletableFuture().join();
    }

    /** The fact that starts the definition. */
    public static CloudEvent drafted(String meetingId, URI draftRef, String onUnavailable) {
        ObjectNode data = Json.obj().put("meetingId", meetingId).put("draftRef", draftRef.toString()).put("label", "confidential");
        if (onUnavailable != null) {
            data.put("onUnavailable", onUnavailable);
        }
        return CloudEventBuilder.v1().withId("drafted-" + meetingId + "-" + (onUnavailable == null ? "" : onUnavailable))
                .withSource(URI.create("https://architecture-meeting.example")).withType(DRAFTED).withSubject(meetingId)
                .withDataContentType("application/json").withData(Json.bytes(data)).build();
    }

    /** Every fact the definitions emitted, in order. */
    public List<CloudEvent> emitted() {
        return channel.published();
    }

    /** Starts the definition from the fact and returns the instance's id. */
    public String start(CloudEvent drafted) throws Exception {
        String id = engine.startFromEvent(definition.ref(), drafted).toCompletableFuture().get(30, TimeUnit.SECONDS);
        current = id;
        return id;
    }

    /**
     * Waits for the instance to end while a scripted reviewer answers: for each open review request {@code reviewer} says the
     * value to answer ({@code null}: leave it open). Gives the last snapshot.
     */
    public InstanceSnapshot await(String id, Duration patience, Function<JsonNode, String> reviewer) throws Exception {
        java.util.Set<String> answered = new java.util.HashSet<>();
        long until = System.nanoTime() + patience.toNanos();
        InstanceSnapshot snapshot = snapshot(id);
        while (!ended(snapshot) && System.nanoTime() < until) {
            for (JsonNode request : channel.allOpen()) {
                String reviewId = request.path("reviewId").asText();
                String value = reviewer.apply(request);
                if (value != null && answered.add(reviewId)) {
                    ObjectNode answer = Json.obj().put("reviewer", "rev-scripted").put("value", value);
                    http.send(HttpRequest.newBuilder(channel.baseUri().resolve("/reviews/" + reviewId))
                            .header(ReviewChannel.ROLE_HEADER, request.path("role").asText())
                            .POST(HttpRequest.BodyPublishers.ofByteArray(Json.bytes(answer))).build(),
                            HttpResponse.BodyHandlers.discarding());
                }
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
            snapshot = snapshot(id);
        }
        return snapshot;
    }

    InstanceSnapshot snapshot(String id) throws Exception {
        return state.load(id).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow();
    }

    private static boolean ended(InstanceSnapshot snapshot) {
        return snapshot.status() == InstanceStatus.COMPLETED || snapshot.status() == InstanceStatus.FAILED
                || snapshot.status() == InstanceStatus.ABORTED;
    }

    @Override
    public void close() {
        pool.close();
        channel.close();
        confluence.close();
    }
}
