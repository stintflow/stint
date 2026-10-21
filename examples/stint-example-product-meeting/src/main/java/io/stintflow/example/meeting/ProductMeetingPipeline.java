package io.stintflow.example.meeting;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import dev.langchain4j.model.chat.ChatModel;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.dsl.DslLoader;
import io.stintflow.dsl.LoadOptions;
import io.stintflow.example.meeting.Corpus.Meeting;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryAuditLog;
import io.stintflow.inmemory.InMemoryCorrectionLog;
import io.stintflow.inmemory.InMemoryStateStore;
import io.stintflow.inmemory.InMemoryTaskTransport;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.otel.OpenTelemetryExecutionTracer;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.ExecutionTracer;
import io.stintflow.spi.RetentionClass;
import io.stintflow.spi.StateStore;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerPool;
import io.stintflow.worker.WorkerRuntime;
import io.stintflow.worker.decision.DecisionAdapter;
import io.stintflow.worker.decision.DecisionConfig;
import io.stintflow.worker.decision.DecisionHandlers;
import io.stintflow.worker.decision.SensitivityPolicy;
import io.stintflow.worker.langchain4j.LangChain4jWorker;
import io.stintflow.worker.publishing.ConfluencePagePublisher;
import io.stintflow.worker.publishing.FakeConfluenceServer;
import io.stintflow.worker.publishing.PublishHandler;
import io.stintflow.worker.redaction.LayeredRedactor;
import io.stintflow.worker.redaction.PatternLayer;
import io.stintflow.worker.redaction.Pseudonyms;
import io.stintflow.worker.redaction.RedactionHandler;
import io.stintflow.worker.redaction.TermListLayer;
import io.stintflow.worker.transcription.Transcriber;
import io.stintflow.worker.transcription.TranscriptionHandler;

/**
 * The milestone, wired (SDD 3a.6): the local engine running {@code product-meeting}, and its workers — the
 * transcriber, the redactor (patterns and the corpus's names), the two decisions, the minutes writer and the publisher
 * to the one allowed space of a local Confluence — over a {@link Stack} that is fake in the build and local in the
 * manual run. The review channel is the engine's publisher; in this example a scripted reviewer answers through its
 * HTTP endpoint as the key says. Synthetic data, fake credentials, loopback only.
 */
public final class ProductMeetingPipeline implements AutoCloseable {

    public static final String SPACE = "PRODUCT";
    public static final String REVIEW_SOURCE = "https://review-channel.example";
    public static final RetentionClass RAW_AUDIO = new RetentionClass("raw-audio", Duration.ofDays(7));
    public static final String CONFLUENCE_EMAIL = "stint-example@example.com";
    public static final String CONFLUENCE_TOKEN = "not-a-real-token";

    /**
     * The models a run uses.
     *
     * @param audio             a meeting's audio
     * @param minutesProvider   the minutes model's provider: name, model, whether it leaves the process
     */
    public record Stack(Transcriber transcriber, DecisionAdapter docType, DecisionAdapter urgency, ChatModel minutes,
            String minutesProvider, String minutesModelName, boolean minutesRemote, Function<Meeting, byte[]> audio) {
    }

    /** One meeting's run, ids and numbers only. */
    public record MeetingRun(Meeting meeting, String instanceId, InstanceStatus status, JsonNode context,
            JsonNode docTypeDecision, Duration total, Map<String, Duration> steps, List<String> chains,
            List<String> reviewsAnswered) {
    }

    final List<Meeting> meetings;
    final RunMetrics metrics;
    final FilesystemBlobStore blobs;
    final ReviewChannel channel;
    final FakeConfluenceServer confluence;
    final InMemoryStateStore state = new InMemoryStateStore();
    final InMemoryAuditLog audit = new InMemoryAuditLog();
    /** The base the minutes are written against, once a run gives one (SDD 3b.4); none: no earlier context. */
    private final java.util.concurrent.atomic.AtomicReference<io.stintflow.knowledge.KnowledgeStore> knowledge =
            new java.util.concurrent.atomic.AtomicReference<>();
    final InMemoryCorrectionLog corrections = new InMemoryCorrectionLog();
    final WorkflowEngine engine;
    final WorkflowDefinition definition;
    final InMemoryTimerService timer;
    /** The instance of the meeting being run, once started. */
    volatile String current;
    private final WorkerPool pool;
    private final Stack stack;
    private final HttpClient http = HttpClient.newHttpClient();

    public ProductMeetingPipeline(List<Meeting> meetings, Stack stack, RunMetrics metrics, Path workDir)
            throws IOException {
        this(meetings, stack, metrics, workDir, InstantSource.system(), new InMemoryTimerService(), s -> s);
    }

    /**
     * @param clock    the engine's clock, and the blob store's (expiry)
     * @param timer    the engine's timers (a test passes one it ticks)
     * @param observed wraps the engine's state store (a test's probe)
     */
    public ProductMeetingPipeline(List<Meeting> meetings, Stack stack, RunMetrics metrics, Path workDir,
            InstantSource clock, InMemoryTimerService timer, Function<StateStore, StateStore> observed)
            throws IOException {
        this.meetings = meetings;
        this.stack = stack;
        this.metrics = metrics;
        this.timer = timer;
        this.blobs = new FilesystemBlobStore(Files.createDirectories(workDir.resolve("blobs")), clock);
        this.channel = new ReviewChannel(URI.create(REVIEW_SOURCE));
        this.confluence = new FakeConfluenceServer(CONFLUENCE_EMAIL, CONFLUENCE_TOKEN, SPACE, "HR");
        OpenTelemetryExecutionTracer tracer = new OpenTelemetryExecutionTracer(SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(metrics)).build().get("stint-example-product-meeting"));
        TaskHandlerRegistry handlers = handlers(meetings, stack, metrics, blobs, tracer, confluence.baseUri(), knowledge::get,
                audit);
        this.pool = new WorkerPool(new WorkerRuntime(handlers, blobs, tracer), 4);
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
        try (InputStream yaml = getClass().getClassLoader().getResourceAsStream("stint/workflows/product-meeting.yaml")) {
            definition = new DslLoader().load(yaml, "product-meeting.yaml", LoadOptions.STRICT);
        }
        registry.register(definition);
        this.engine = new WorkflowEngine(registry, transport, observed.apply(state), timer, blobs,
                new TreeInterpreter(new JqExpressionEvaluator()), clock, channel, null, tracer)
                .auditLog(audit).correctionLog(corrections);
        channel.answersTo(engine);
    }

    /**
     * The milestone's workers, by routing key: {@code transcribe}, {@code redactor} (patterns and the corpus's names),
     * {@code decide-doc-type} and {@code decide-urgency}, {@code structure-minutes} and {@code publish-page} (to the
     * one allowed space of the Confluence at {@code confluenceBase}, with the fake credentials). Confidential data goes
     * to local providers only.
     */
    public static TaskHandlerRegistry handlers(List<Meeting> meetings, Stack stack, RunMetrics metrics,
            BlobStore blobs, ExecutionTracer tracer, URI confluenceBase) {
        return handlers(meetings, stack, metrics, blobs, tracer, confluenceBase, null, null);
    }

    /**
     * As above, and {@code structure-minutes} reads what the base holds from earlier meetings (SDD 3b.4, 4.6): through a retriever
     * whose ceiling is the instance's level and the minutes model's provider, that leaves the meeting itself out and records each
     * read in {@code audit}. {@code knowledge} gives the base when the task runs (or {@code null} for none yet).
     */
    public static TaskHandlerRegistry handlers(List<Meeting> meetings, Stack stack, RunMetrics metrics,
            BlobStore blobs, ExecutionTracer tracer, URI confluenceBase,
            java.util.function.Supplier<io.stintflow.knowledge.KnowledgeStore> knowledge, io.stintflow.spi.AuditLog audit) {
        SensitivityPolicy policy = SensitivityPolicy.fromProperties(Map.of());
        LayeredRedactor redactor = new LayeredRedactor("example", "1", List.of(new PatternLayer(),
                new TermListLayer("person", Corpus.personMarkers(meetings))));
        LangChain4jWorker writer = LangChain4jWorker.builder().chatModel(stack.minutes())
                .provider(stack.minutesProvider(), stack.minutesModelName(), stack.minutesRemote()).policy(policy)
                .usage(metrics.llmUsage()).tracer(tracer).build();
        DecisionHandlers.Builder decisionsBuilder = DecisionHandlers.builder(DecisionConfig.fromProperties(Map.of(
                        "stint.decision.routes.decide-doc-type.adapter", stack.docType().descriptor().id(),
                        "stint.decision.routes.decide-doc-type.instructions",
                        "Which document should this meeting produce: minutes (ata), an architecture decision record "
                                + "(adr) or a runbook?",
                        "stint.decision.routes.decide-urgency.adapter", stack.urgency().descriptor().id(),
                        "stint.decision.routes.decide-urgency.instructions", "Does this meeting need action today?")))
                .adapter(stack.docType()).policy(policy).usage(metrics.decisionUsage());
        if (!stack.urgency().descriptor().id().equals(stack.docType().descriptor().id())) {
            decisionsBuilder.adapter(stack.urgency());   // in the manual run, LitJev serves both routes
        }
        return decisionsBuilder.build().registerAll(new TaskHandlerRegistry())
                .register("transcribe", new TranscriptionHandler(metrics.recording(stack.transcriber()), blobs))
                .register("redactor", new RedactionHandler(redactor, pseudonyms(), blobs).allowingStablePseudonyms())
                .register("structure-minutes", new StructureHandler(writer, blobs, knowledge == null ? null : task -> {
                    io.stintflow.knowledge.KnowledgeStore base = knowledge.get();
                    if (base == null) {
                        return null;
                    }
                    io.stintflow.spi.ModelProvider provider = new io.stintflow.spi.ModelProvider(stack.minutesProvider(),
                            stack.minutesModelName(), stack.minutesRemote());
                    var retriever = io.stintflow.worker.langchain4j.KnowledgeContentRetriever
                            .builder(base, KnowledgeIngestPipeline.SCOPE).instanceLevel(task.sensitivity()).provider(provider)
                            .approvedRemote(policy.refusal(io.stintflow.spi.Sensitivity.CONFIDENTIAL, provider).isEmpty())
                            .topK(5).hops(2).budgetTokens(1500)
                            .excludeDocuments(Set.of("meeting:" + task.input().path("meetingId").asText()));
                    return (audit == null ? retriever : retriever.audit(audit, task, InstantSource.system())).build();
                }))
                .register("publish-page", new PublishHandler(ConfluencePagePublisher.builder()
                        .baseUri(confluenceBase).space(SPACE).email(CONFLUENCE_EMAIL).apiToken(CONFLUENCE_TOKEN)
                        .build(), blobs, SPACE));
    }

    /** From now on the minutes of a meeting are written with what {@code base} holds from the earlier ones (SDD 3b.4). */
    public void useKnowledge(io.stintflow.knowledge.KnowledgeStore base) {
        knowledge.set(base);
    }

    /** The secret behind the pseudonyms of the example: the redactor's, and the one that finds a person to erase (SDD 3b.5). */
    public static Pseudonyms pseudonyms() {
        return new Pseudonyms("example-secret-not-for-real-data".getBytes(StandardCharsets.UTF_8));
    }

    /** The {@code meeting-recorded.v1} event of a meeting whose audio rests at {@code audioRef}. */
    public static CloudEvent meetingRecorded(Meeting meeting, URI audioRef, double seconds) {
        ObjectNode data = Json.obj().put("meetingId", meeting.id()).put("audioRef", audioRef.toString())
                .put("language", meeting.language()).put("durationSeconds", seconds)
                .put("recordedAt", "2026-10-13T12:00:00Z").put("label", meeting.label());
        return CloudEventBuilder.v1().withId("rec-" + meeting.id()).withSource(URI.create("https://recorder.example"))
                .withType("io.stintflow.examples.meeting-recorded.v1").withSubject(meeting.id())
                .withDataContentType("application/json").withData(Json.bytes(data)).build();
    }

    /** The audio goes to a restricted blob; the meeting starts from its event; the scripted reviewer answers. */
    public MeetingRun run(Meeting meeting, Duration patience) throws Exception {
        URI audioRef = blobs.put(stack.audio().apply(meeting), "audio/" + meeting.id() + ".wav", RAW_AUDIO)
                .toCompletableFuture().get(10, TimeUnit.SECONDS);
        Instant started = Instant.now();
        String id = engine.startFromEvent(definition.ref(), meetingRecorded(meeting, audioRef, 0))
                .toCompletableFuture().get(30, TimeUnit.SECONDS);
        current = id;
        List<String> answered = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        long until = System.nanoTime() + patience.toNanos();
        InstanceSnapshot snapshot = snapshot(id);
        while (!terminal(snapshot) && System.nanoTime() < until) {
            for (JsonNode request : channel.openFor(id)) {
                String reviewId = request.path("reviewId").asText();
                if (seen.add(reviewId)) {
                    answerAsTheKey(http, channel, meeting, request);
                    answered.add(request.path("kind").asText());
                }
            }
            metrics.sampleGpu();
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
            snapshot = snapshot(id);
        }
        Duration total = Duration.between(started, Instant.now());
        JsonNode decision = null;
        if (snapshot.decisions() != null) {
            for (var entry : snapshot.decisions().properties()) {
                if (entry.getKey().endsWith("/docType")) {
                    decision = entry.getValue();
                }
            }
        }
        return new MeetingRun(meeting, id, snapshot.status(), snapshot.context(), decision, total,
                metrics.stepLatencies(id), metrics.chainIds(id), answered);
    }

    /** The scripted reviewer: the key's document type for a decision, every item approved for the minutes. */
    public static void answerAsTheKey(HttpClient http, ReviewChannel channel, Meeting meeting, JsonNode request)
            throws Exception {
        ObjectNode answer = Json.obj().put("reviewer", "rev-scripted");
        if ("choice".equals(request.path("kind").asText())) {
            answer.put("value", meeting.expected().docType());
        } else {
            ArrayNode items = answer.putArray("items");
            request.path("items").forEach(i -> items.addObject().put("id", i.asText()).put("verdict", "approved"));
            if (request.path("items").isEmpty()) {
                answer.put("verdict", "approved");
            }
        }
        http.send(HttpRequest.newBuilder(channel.baseUri().resolve("/reviews/" + request.path("reviewId").asText()))
                .header(ReviewChannel.ROLE_HEADER, request.path("role").asText())
                .POST(HttpRequest.BodyPublishers.ofByteArray(Json.bytes(answer))).build(),
                HttpResponse.BodyHandlers.discarding());
    }

    private InstanceSnapshot snapshot(String id) throws Exception {
        return state.load(id).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow();
    }

    private static boolean terminal(InstanceSnapshot snapshot) {
        return snapshot.status() == InstanceStatus.COMPLETED || snapshot.status() == InstanceStatus.FAILED;
    }

    /** The CI's stack: everything fake, deterministic, from the corpus. */
    public static Stack fakes(List<Meeting> meetings) {
        Map<String, Transcriber.Transcript> transcripts = new java.util.HashMap<>();
        for (Meeting meeting : meetings) {
            List<Transcriber.Segment> segments = new ArrayList<>();
            for (int i = 0; i < meeting.script().size(); i++) {
                segments.add(new Transcriber.Segment(Duration.ofSeconds(4L * i), Duration.ofSeconds(4L * i + 4),
                        meeting.script().get(i).text()));
            }
            transcripts.put(io.stintflow.worker.transcription.FakeTranscriber.sha256(syntheticAudio(meeting)),
                    new Transcriber.Transcript("en", Duration.ofSeconds(4L * segments.size()), segments));
        }
        return new Stack(io.stintflow.worker.transcription.FakeTranscriber.scripted(transcripts),
                FakeDecisions.docType(), FakeDecisions.urgency(), new FakeMinutesModel(), "fake",
                FakeMinutesModel.MODEL_NAME, false, ProductMeetingPipeline::syntheticAudio);
    }

    /** Stand-in audio for the build: a few bytes per meeting, never a recording. */
    public static byte[] syntheticAudio(Meeting meeting) {
        return ("RIFF-synthetic-meeting-" + meeting.id()).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        pool.close();
        channel.close();
        confluence.close();
    }
}
