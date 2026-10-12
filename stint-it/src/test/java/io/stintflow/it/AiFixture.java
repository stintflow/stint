package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import com.fasterxml.jackson.databind.JsonNode;

import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;
import dev.langchain4j.model.output.FinishReason;
import dev.langchain4j.model.output.TokenUsage;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.dsl.DslLoader;
import io.stintflow.dsl.LoadOptions;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryStateStore;
import io.stintflow.inmemory.InMemoryTaskTransport;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.ExecutionTracer;
import io.stintflow.spi.TimerFireHandler;
import io.stintflow.spi.TimerRequest;
import io.stintflow.spi.TimerService;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerRuntime;
import io.stintflow.worker.langchain4j.LangChain4jWorker;
import io.stintflow.worker.langchain4j.memory.InMemoryConversationStore;

/**
 * SDD 3a.3: an engine loaded from YAML whose worker runs LLM tasks through {@link LangChain4jWorker}, over a fake
 * model — never a provider, synthetic data only. Two routing keys:
 * <ul>
 *   <li>{@code summarize}: {@link Summarizer}, with the task input's {@code text};</li>
 *   <li>{@code ask}: {@link Chat}, with the task input's {@code question}.</li>
 * </ul>
 * The clock and the timers move only when the test says so (never a sleep).
 */
final class AiFixture {

    static final Instant T0 = Instant.parse("2026-10-12T12:00:00Z");

    interface Summarizer {
        Summary summarize(@dev.langchain4j.service.UserMessage String text);
    }

    record Summary(String topic, int items) {
    }

    interface Chat {
        String reply(@dev.langchain4j.service.UserMessage String question);
    }

    final AtomicReference<Instant> now = new AtomicReference<>(T0);
    final InMemoryTimerService timers = new InMemoryTimerService(now::get);
    final BlockingQueue<TimerRequest> armed = new LinkedBlockingQueue<>();
    final InMemoryConversationStore conversations = new InMemoryConversationStore(now::get);
    final WorkflowDefinition def;
    final WorkflowEngine engine;
    final LangChain4jWorker worker;

    AiFixture(String tasks, ChatModel model) throws Exception {
        this(tasks, model, b -> b, ExecutionTracer.NOOP);
    }

    AiFixture(String tasks, ChatModel model, UnaryOperator<LangChain4jWorker.Builder> options,
            ExecutionTracer tracer) throws Exception {
        String yaml = """
                document:
                  dsl: '1.0.0'
                  namespace: ai
                  name: notes
                  version: '1.0.0'
                """ + tasks;
        def = new DslLoader().load(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)), "notes.yaml",
                LoadOptions.STRICT);
        WorkflowRegistry registry = new WorkflowRegistry();
        registry.register(def);
        BlobStore blobs = new FilesystemBlobStore(Files.createTempDirectory("stint-sdd3a3"));
        worker = options.apply(LangChain4jWorker.builder().chatModel(model).conversations(conversations)
                .clock(now::get).tracer(tracer)).build();
        TaskHandlerRegistry handlers = new TaskHandlerRegistry()
                .register("summarize", worker.handler(Summarizer.class,
                        (ai, t) -> ai.summarize(t.input().path("text").asText())))
                .register("ask", worker.handler(Chat.class, (ai, t) -> ai.reply(t.input().path("question").asText())));
        InMemoryTaskTransport transport = new InMemoryTaskTransport();
        transport.connectWorker(new WorkerRuntime(handlers, blobs, tracer)::handle);
        TimerService recording = new TimerService() {
            @Override
            public CompletionStage<String> schedule(TimerRequest req) {
                armed.add(req);
                return timers.schedule(req);
            }

            @Override
            public CompletionStage<Void> cancel(String timerId) {
                return timers.cancel(timerId);
            }

            @Override
            public void onFire(TimerFireHandler handler) {
                timers.onFire(handler);
            }

            @Override
            public Duration maxDelay() {
                return timers.maxDelay();
            }
        };
        engine = new WorkflowEngine(registry, transport, new InMemoryStateStore(), recording, blobs,
                new TreeInterpreter(new JqExpressionEvaluator()), now::get, null, null, tracer);
    }

    String start(JsonNode input) {
        return engine.start(def.ref(), input);
    }

    JsonNode awaitCompleted(String id) throws Exception {
        return engine.awaitCompletion(id, Duration.ofSeconds(10)).toCompletableFuture().get(15, TimeUnit.SECONDS);
    }

    /** Starts an instance that must fail, and returns the engine's WARNING line, with the error. */
    String startFailing(JsonNode input) throws Exception {
        try (DecisionFixture.LogCapture logs = new DecisionFixture.LogCapture()) {
            String id = start(input);
            return logs.await(line -> line.contains("Instance " + id + " failed"));
        }
    }

    /** Waits for the engine to arm a retry delay — skipping the timeout every dispatch arms ({@code timer:<cid>}). */
    TimerRequest awaitRetryTimer() throws InterruptedException {
        while (true) {
            TimerRequest timer = armed.poll(10, TimeUnit.SECONDS);
            assertThat(timer).as("a retry delay was armed").isNotNull();
            if (!timer.timerId().startsWith("timer:")) {
                return timer;
            }
        }
    }

    /** Moves the clock to {@code timer} and fires what is due. */
    void fire(TimerRequest timer) {
        now.set(timer.fireAt());
        timers.tick();
    }

    /** A model that answers by a rule and remembers what it was asked. */
    static final class RuleModel implements ChatModel {
        final List<ChatRequest> requests = new CopyOnWriteArrayList<>();
        private final Function<ChatRequest, String> rule;

        RuleModel(Function<ChatRequest, String> rule) {
            this.rule = rule;
        }

        /** Answers {@code <messages seen>:<last question>}. */
        static RuleModel echoing() {
            return new RuleModel(request -> request.messages().size() + ":" + lastUserText(request.messages()));
        }

        @Override
        public ChatResponse doChat(ChatRequest request) {
            requests.add(request);
            return ChatResponse.builder().aiMessage(AiMessage.from(rule.apply(request))).modelName("rule-model")
                    .tokenUsage(new TokenUsage(20, 10)).finishReason(FinishReason.STOP).build();
        }
    }

    static String lastUserText(List<ChatMessage> messages) {
        for (ChatMessage message : messages.reversed()) {
            if (message instanceof UserMessage user) {
                return user.singleText();
            }
        }
        return "";
    }

    static JsonNode input(String field, String value) {
        return Json.obj().put(field, value);
    }
}
