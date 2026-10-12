package io.stintflow.example.ai;

import java.io.InputStream;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;

import io.cloudevents.CloudEvent;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.dsl.DslLoader;
import io.stintflow.dsl.LoadOptions;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryStateStore;
import io.stintflow.inmemory.InMemoryTaskTransport;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.spi.BlobStore;
import io.stintflow.wire.Json;
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
 * SDD 3a.3, CA4 and SDD 3a.2, CA12: runs {@code meeting-notes} once on the local engine and checks it. The worker is an ordinary Stint
 * worker — a {@link WorkerRuntime} behind a {@link WorkerPool} — whose handlers come from {@link LangChain4jWorker},
 * over {@link FakeMeetingModel} and an in-memory conversation store. Prints {@code AI-WORKER OK} and exits 0 when the
 * second step found the meeting's action items through the memory the first step committed.
 */
@QuarkusMain
public class AiWorkerMain implements QuarkusApplication {

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
        BlobStore blobs = new FilesystemBlobStore(Files.createTempDirectory("stint-ai-worker"));
        LangChain4jWorker ai = LangChain4jWorker.builder()
                .chatModel(new FakeMeetingModel())
                .conversations(new InMemoryConversationStore(), blobs)
                .build();
        // SDD 3a.2: the two decisions — the meeting's document by the LLM adapter, its urgency by the Jev adapter
        FakeJevEndpoint jev = new FakeJevEndpoint();
        DecisionHandlers decisions = DecisionHandlers.builder(DecisionConfig.fromProperties(Map.of(
                        "stint.decision.routes.classify-meeting.adapter", "llm",
                        "stint.decision.routes.classify-meeting.instructions",
                        "Which document should this meeting produce?",
                        "stint.decision.routes.is-urgent.adapter", "jev",
                        "stint.decision.routes.is-urgent.instructions", "Does this meeting need action today?",
                        "stint.decision.adapters.llm.threshold", "0.8")))
                .adapter(LlmDecisionAdapter.builder().id("llm").chatModel(new FakeMeetingModel()).provider("fake")
                        .build())
                .adapter(JevDecisionAdapter.builder().id("jev").baseUri(jev.baseUri()).apiKey(() -> "test").build())
                .build();
        TaskHandlerRegistry handlers = decisions.registerAll(new TaskHandlerRegistry())
                .register("summarize-meeting", ai.handler(MeetingAssistant.class, (assistant, task) ->
                        assistant.summarize("Summarize this meeting transcript:\n"
                                + task.input().path("transcript").asText())))
                .register("extract-actions", ai.handler(MeetingAssistant.class,
                        (assistant, task) -> assistant.actionItems()));

        try (jev; WorkerPool pool = new WorkerPool(new WorkerRuntime(handlers, blobs), 4)) {
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
            WorkflowDefinition def;
            try (InputStream yaml = AiWorkerMain.class.getClassLoader()
                    .getResourceAsStream("stint/workflows/meeting-notes.yaml")) {
                def = new DslLoader().load(yaml, "meeting-notes.yaml", LoadOptions.STRICT);
            }
            registry.register(def);
            WorkflowEngine engine = new WorkflowEngine(registry, transport, new InMemoryStateStore(),
                    new InMemoryTimerService(), blobs);

            String id = engine.start(def.ref(), Json.obj().put("transcript", TRANSCRIPT));
            JsonNode context = engine.awaitCompletion(id, Duration.ofSeconds(30)).toCompletableFuture()
                    .get(60, TimeUnit.SECONDS);
            System.out.println("AI-WORKER result=" + context);

            boolean ok = "ata".equals(context.path("doc").asText())
                    && context.path("urgent").isBoolean() && !context.path("urgent").asBoolean()
                    && "quarterly planning".equals(context.path("summary").path("topic").asText())
                    && context.path("summary").path("speakers").asInt() == 2
                    && context.path("actions").path("items").size() == 2
                    // the request of step 2: step 1's user and AI messages, from memory, plus its own question
                    && context.path("actions").path("contextMessages").asInt() == 3;
            System.out.println(ok ? "AI-WORKER OK" : "AI-WORKER FAILED");
            return ok ? 0 : 1;
        }
    }
}
