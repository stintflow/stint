package io.stintflow.example.ai;

import java.io.InputStream;
import java.io.StringWriter;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.InstantSource;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.eclipse.microprofile.config.ConfigProvider;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.cloudevents.CloudEvent;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.dsl.DslLoader;
import io.stintflow.dsl.LoadOptions;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryCorrectionLog;
import io.stintflow.inmemory.InMemoryStateStore;
import io.stintflow.inmemory.InMemoryTaskTransport;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.Sensitivity;
import io.stintflow.wire.Json;
import io.stintflow.wire.review.CorrectionExport;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerPool;
import io.stintflow.worker.WorkerRuntime;
import io.stintflow.worker.decision.DecisionConfig;
import io.stintflow.worker.decision.DecisionHandlers;
import io.stintflow.worker.decision.jev.JevDecisionAdapter;
import io.stintflow.worker.langchain4j.LangChain4jWorker;
import io.stintflow.worker.langchain4j.LlmDecisionAdapter;
import io.stintflow.worker.langchain4j.memory.InMemoryConversationStore;

/**
 * SDD 3a.3, CA4, SDD 3a.2, CA12 and SDD 3a.2b, CA5: runs {@code meeting-notes} once on the local engine and checks it.
 * Its urgency is decided by the Jev adapter's local profile, against an in-process fake of a LitJev server.
 * <p>
 * {@code triage <folder>} is the manual run of SDD 3a.2b (CA6) instead: see {@link MeetingTriage}. It needs
 * {@code stint.example.litjev.base-uri} (a real LitJev, e.g. {@code http://127.0.0.1:8000}) and takes
 * {@code stint.example.litjev.model}, {@code .server-version} and {@code .readout}.
 * <p> The worker is an ordinary Stint
 * worker — a {@link WorkerRuntime} behind a {@link WorkerPool} — whose handlers come from {@link LangChain4jWorker},
 * over {@link FakeMeetingModel} and an in-memory conversation store. Prints {@code AI-WORKER OK} and exits 0 when the
 * second step found the meeting's action items through the memory the first step committed.
 */
@QuarkusMain
public class AiWorkerMain implements QuarkusApplication {

    /** SDD 3a.4: the source the example's workflows trust for review answers (their policy lists it). */
    static final String REVIEW_SOURCE = "https://review-channel.example";

    /** A synthetic meeting: no real person, meeting or transcript. */
    static final String TRANSCRIPT = """
            Topic: quarterly planning
            Speaker A: the release slips one week.
            Speaker B: I will update the roadmap.
            Speaker A: and I will tell the support team.""";

    public static void main(String... args) {
        Quarkus.run(AiWorkerMain.class, args);
    }

    @Override
    public int run(String... args) throws Exception {
        if (args.length > 0 && args[0].equals("triage")) {
            if (args.length != 2) {
                System.out.println("usage: triage <folder of meeting files>");
                return 2;
            }
            URI baseUri = URI.create(setting("base-uri").orElseThrow(() -> new IllegalArgumentException(
                    "The triage runs against a real LitJev: set stint.example.litjev.base-uri")));
            return MeetingTriage.run(Path.of(args[1]), localJev(baseUri));
        }
        BlobStore blobs = new FilesystemBlobStore(Files.createTempDirectory("stint-ai-worker"));
        LangChain4jWorker ai = LangChain4jWorker.builder()
                .chatModel(new FakeMeetingModel())
                .conversations(new InMemoryConversationStore(), blobs)
                .build();
        // SDD 3a.2: the two decisions — the meeting's document by the LLM adapter, its urgency by the Jev adapter's
        // local profile (SDD 3a.2b) against an in-process fake of LitJev
        FakeJevEndpoint jev = new FakeJevEndpoint();
        DecisionHandlers decisions = DecisionHandlers.builder(DecisionConfig.fromProperties(Map.of(
                        "stint.decision.routes.classify-meeting.adapter", "llm",
                        "stint.decision.routes.classify-meeting.instructions",
                        "Which document should this meeting produce?",
                        "stint.decision.routes.is-urgent.adapter", "litjev",
                        "stint.decision.routes.is-urgent.instructions", "Does this meeting need action today?",
                        "stint.decision.adapters.llm.threshold", "0.8")))
                .adapter(LlmDecisionAdapter.builder().id("llm").chatModel(new FakeMeetingModel()).provider("fake")
                        .build())
                .adapter(localJev(jev.baseUri()))
                .build();
        TaskHandlerRegistry handlers = decisions.registerAll(new TaskHandlerRegistry())
                .register("summarize-meeting", ai.handler(MeetingAssistant.class, (assistant, task) ->
                        assistant.summarize("Summarize this meeting transcript:\n"
                                + task.input().path("transcript").asText())))
                .register("extract-actions", ai.handler(MeetingAssistant.class,
                        (assistant, task) -> assistant.actionItems()));

        try (jev; WorkerPool pool = new WorkerPool(new WorkerRuntime(handlers, blobs), 4);
             ReviewChannel channel = new ReviewChannel(URI.create(REVIEW_SOURCE))) {
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

            WorkflowRegistry registry = new WorkflowRegistry();
            WorkflowDefinition def = load("meeting-notes.yaml");
            WorkflowDefinition review = load("meeting-review.yaml");
            registry.register(def);
            registry.register(review);
            InMemoryStateStore states = new InMemoryStateStore();
            InMemoryCorrectionLog corrections = new InMemoryCorrectionLog();
            // SDD 3a.4: the review channel is the engine's publisher (it sees the requests) and answers through it
            WorkflowEngine engine = new WorkflowEngine(registry, transport, states, new InMemoryTimerService(), blobs,
                    new TreeInterpreter(new JqExpressionEvaluator()), InstantSource.system(), channel, null)
                    .correctionLog(corrections);
            channel.answersTo(engine);

            String id = engine.start(def.ref(), Json.obj().put("transcript", TRANSCRIPT));
            JsonNode context = engine.awaitCompletion(id, Duration.ofSeconds(30)).toCompletableFuture()
                    .get(60, TimeUnit.SECONDS);
            System.out.println("AI-WORKER result=" + context);
            JsonNode urgent = states.load(id).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow()
                    .decisions().path("/do/2/urgent");
            System.out.println("AI-WORKER urgent decision=" + urgent);

            boolean ok = "ata".equals(context.path("doc").asText())
                    && context.path("urgent").isBoolean() && !context.path("urgent").asBoolean()
                    && "litjev".equals(urgent.path("provenance").path("provider").asText())
                    && FakeJevEndpoint.MODEL.equals(urgent.path("provenance").path("model").asText())
                    && "quarterly planning".equals(context.path("summary").path("topic").asText())
                    && context.path("summary").path("speakers").asInt() == 2
                    && context.path("actions").path("items").size() == 2
                    // the request of step 2: step 1's user and AI messages, from memory, plus its own question
                    && context.path("actions").path("contextMessages").asInt() == 3;
            boolean reviewOk = reviewThroughTheChannel(engine, channel, review, corrections);
            System.out.println(ok && reviewOk ? "AI-WORKER OK" : "AI-WORKER FAILED");
            return ok && reviewOk ? 0 : 1;
        }
    }

    private static WorkflowDefinition load(String name) throws java.io.IOException {
        try (InputStream yaml = AiWorkerMain.class.getClassLoader().getResourceAsStream("stint/workflows/" + name)) {
            return new DslLoader().load(yaml, name, LoadOptions.STRICT);
        }
    }

    /**
     * SDD 3a.4: the business representative rejects one of the minutes' two items through the review channel's HTTP
     * endpoint; the flow goes back for revision, and the correction is exported as JSONL. Synthetic pointers only.
     */
    private static boolean reviewThroughTheChannel(WorkflowEngine engine, ReviewChannel channel,
            WorkflowDefinition review, InMemoryCorrectionLog corrections) throws Exception {
        String id = engine.start(review.ref(), Json.obj().put("minutesRef", "file:///synthetic/minutes-1.md")
                .set("itemIds", Json.MAPPER.createArrayNode().add("item-1").add("item-2")), Sensitivity.INTERNAL);
        HttpClient http = HttpClient.newHttpClient();
        JsonNode open = Json.MAPPER.createArrayNode();
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (open.isEmpty() && System.nanoTime() < until) {
            open = Json.read(http.send(HttpRequest.newBuilder(channel.baseUri().resolve("/reviews")).GET().build(),
                    HttpResponse.BodyHandlers.ofByteArray()).body());
        }
        System.out.println("AI-WORKER open reviews=" + open);
        String reviewId = open.path(0).path("reviewId").asText();
        ObjectNode answer = Json.obj().put("reviewer", "rev-demo").put("noteRef", "file:///synthetic/note-1.md");
        answer.putArray("items").add(Json.obj().put("id", "item-1").put("verdict", "approved"))
                .add(Json.obj().put("id", "item-2").put("verdict", "rejected"));
        int status = http.send(HttpRequest.newBuilder(channel.baseUri().resolve("/reviews/" + reviewId))
                .header(ReviewChannel.ROLE_HEADER, "business-rep")
                .POST(HttpRequest.BodyPublishers.ofByteArray(Json.bytes(answer))).build(),
                HttpResponse.BodyHandlers.discarding()).statusCode();
        JsonNode reviewed = engine.awaitCompletion(id, Duration.ofSeconds(30)).toCompletableFuture()
                .get(60, TimeUnit.SECONDS);
        System.out.println("AI-WORKER review answer status=" + status + " result=" + reviewed);
        StringWriter jsonl = new StringWriter();
        int lines = CorrectionExport.write(corrections.of(review.ref()).toCompletableFuture().get(),
                Sensitivity.INTERNAL, jsonl);
        System.out.print("AI-WORKER corrections.jsonl=" + jsonl);
        return status == 202 && reviewed.path("revise").asBoolean()
                && "[\"item-2\"]".equals(reviewed.path("rejected").toString()) && lines == 1;
    }

    /** SDD 3a.2b: the Jev adapter's local profile — no key; refused by the adapter if {@code baseUri} isn't loopback. */
    private static JevDecisionAdapter localJev(URI baseUri) {
        JevDecisionAdapter.Builder builder = JevDecisionAdapter.builder().server(JevDecisionAdapter.Server.LITJEV)
                .baseUri(baseUri);
        setting("model").ifPresent(builder::model);
        setting("server-version").ifPresent(builder::serverVersion);
        setting("readout").ifPresent(builder::readout);
        return builder.build();
    }

    private static Optional<String> setting(String name) {
        return ConfigProvider.getConfig().getOptionalValue("stint.example.litjev." + name, String.class);
    }
}
