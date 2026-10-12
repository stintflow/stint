package io.stintflow.worker.langchain4j;

import static io.stintflow.worker.langchain4j.Fakes.answer;
import static io.stintflow.worker.langchain4j.Fakes.failure;
import static io.stintflow.worker.langchain4j.Fakes.run;
import static io.stintflow.worker.langchain4j.Fakes.task;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import dev.langchain4j.agent.tool.Tool;
import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.agent.tool.ToolSpecification;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ToolExecutionResultMessage;
import dev.langchain4j.model.chat.response.ChatResponse;
import dev.langchain4j.model.output.FinishReason;
import dev.langchain4j.model.output.TokenUsage;
import io.stintflow.spi.ExecutionSpan;
import io.stintflow.spi.ExecutionTracer;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.TraceAttributes;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskHandler;
import io.stintflow.worker.langchain4j.Fakes.Chat;
import io.stintflow.worker.langchain4j.Fakes.Clock;
import io.stintflow.worker.langchain4j.Fakes.Logs;
import io.stintflow.worker.langchain4j.Fakes.ScriptedModel;
import io.stintflow.worker.langchain4j.memory.InMemoryConversationStore;

/**
 * SDD 3a.3, CA9 (RF8, RF10): usage is recorded as numbers with the task's identifiers, a model call is a span with
 * identifiers only, and no prompt or response text reaches a log line, its MDC or a span — on success, on a provider
 * failure, on a validation failure, nor through a slow tool call. Only a {@link ContentPolicy} that names a sink gets
 * content.
 */
class UsageWithoutContentTest {

    /** Synthetic markers standing for a transcript in the prompt and for what the model answers. */
    static final String PROMPT_MARKER = "PROMPT-MARKER-91c2";
    static final String ANSWER_MARKER = "ANSWER-MARKER-5d08";

    private final Clock clock = new Clock();
    private final List<LlmUsage> usages = new CopyOnWriteArrayList<>();
    private final RecordingTracer tracer = new RecordingTracer();

    private LangChain4jWorker.Builder worker(ScriptedModel model) {
        return LangChain4jWorker.builder().chatModel(model).conversations(new InMemoryConversationStore()).clock(clock)
                .tracer(tracer).usage((task, usage) -> {
                    usages.add(usage);
                    UsageSink.LOG.record(task, usage);
                });
    }

    @Test
    void usage_is_numbers_and_identifiers_and_no_text_reaches_logs_or_spans() {
        ScriptedModel model = new ScriptedModel(request -> {
            clock.advance(Duration.ofMillis(1500));
            return answer("summary " + ANSWER_MARKER);
        });
        TaskHandler handler = worker(model).build().handler(Chat.class,
                (ai, t) -> ai.reply("Summarize: " + t.input().path("text").asText()));
        TaskContext ctx = traced(task("i-A", "/do/0/summarize", "c-1", 1, Json.obj().put("text", PROMPT_MARKER)));

        JsonNode output;
        try (Logs logs = new Logs()) {
            output = run(handler, ctx);

            assertThat(logs.joined()).doesNotContain(PROMPT_MARKER).doesNotContain(ANSWER_MARKER)
                    .contains("LLM usage of task /do/0/summarize in instance i-A (chain chn-i-A, attempt 1): "
                            + "model=scripted-model, calls=1, inputTokens=11, outputTokens=7, toolCalls=0, "
                            + "latencyMs=1500, finishReason=STOP")
                    .contains(TraceAttributes.INSTANCE_ID + "=i-A").contains(TraceAttributes.CHAIN_ID + "=chn-i-A");
        }
        assertThat(output.asText()).contains(ANSWER_MARKER);   // the content is the task's output, and only that
        assertThat(usages).singleElement().isEqualTo(
                new LlmUsage("scripted-model", 1, 11, 7, 0, Duration.ofMillis(1500), "STOP"));
        assertThat(tracer.spans).singleElement().satisfies(span -> {
            assertThat(span.name).isEqualTo(MeteredChatModel.SPAN_NAME);
            assertThat(span.attributes.keySet()).isSubsetOf(TraceAttributes.ALLOWED);
            assertThat(span.attributes).containsEntry(TraceAttributes.TASK_ID, "/do/0/summarize");
            assertThat(span.parent).isEqualTo(ctx.lineage());   // child of the worker's task span
            assertThat(span.closed).isTrue();
            assertThat(span.toString()).doesNotContain(PROMPT_MARKER).doesNotContain(ANSWER_MARKER);
        });
    }

    @Test
    void failures_are_recorded_without_the_text_either() {
        ScriptedModel failing = new ScriptedModel(request -> {
            throw new IllegalStateException("provider quoted: " + Fakes.lastUserText(request));
        });
        ScriptedModel unparseable = ScriptedModel.replying("not a summary " + ANSWER_MARKER);
        TaskHandler fails = worker(failing).build().handler(Chat.class, (ai, t) -> ai.reply(PROMPT_MARKER));
        TaskHandler invalid = worker(unparseable).build()
                .handler(Fakes.Summarizer.class, (ai, t) -> ai.summarize(PROMPT_MARKER));

        try (Logs logs = new Logs()) {
            assertThat(failure(fails, task("i-A", "/do/0/a", "c-1")).getMessage()).doesNotContain(PROMPT_MARKER);
            assertThat(failure(invalid, task("i-A", "/do/1/b", "c-2")).getMessage()).doesNotContain(ANSWER_MARKER);

            assertThat(logs.joined()).doesNotContain(PROMPT_MARKER).doesNotContain(ANSWER_MARKER)
                    .contains("not a known provider error: java.lang.IllegalStateException");
        }
        assertThat(usages).extracting(LlmUsage::calls).containsExactly(1, 1);   // failed calls are counted too
        assertThat(tracer.spans).hasSize(2);
        assertThat(tracer.spans.getFirst().failures).containsExactly("java.lang.IllegalStateException");
        assertThat(tracer.spans.toString()).doesNotContain(PROMPT_MARKER).doesNotContain(ANSWER_MARKER);
    }

    @Test
    void a_slow_tool_call_is_reported_by_name_and_duration_only() {
        ScriptedModel model = new ScriptedModel(request -> {
            if (request.messages().getLast() instanceof ToolExecutionResultMessage) {
                return answer("done");
            }
            ToolSpecification tool = request.toolSpecifications().getFirst();
            String parameter = tool.parameters().properties().keySet().iterator().next();
            return ChatResponse.builder().aiMessage(AiMessage.from(ToolExecutionRequest.builder().id("call-1")
                            .name(tool.name()).arguments("{\"" + parameter + "\":\"" + PROMPT_MARKER + "\"}").build()))
                    .modelName("scripted-model").tokenUsage(new TokenUsage(3, 2)).finishReason(FinishReason.TOOL_EXECUTION)
                    .build();
        });
        SlowLookup lookup = new SlowLookup(clock);
        TaskHandler handler = worker(model).toolCallLimit(Duration.ofSeconds(5)).build().service(Chat.class)
                .configure(services -> services.tools(lookup))
                .handler((ai, t) -> ai.reply("look it up"));

        try (Logs logs = new Logs()) {
            assertThat(run(handler, task("i-A", "/do/0/a", "c-1")).asText()).isEqualTo("done");

            assertThat(logs.lines).anySatisfy(line -> assertThat(line).startsWith("WARN")
                    .contains("Tool call 'lookup' took 6000 ms, over the limit of 5000 ms"));
            assertThat(logs.joined()).doesNotContain(PROMPT_MARKER).doesNotContain(SlowLookup.RESULT);
        }
        assertThat(lookup.calls).isEqualTo(1);
        assertThat(usages).singleElement().satisfies(u -> {
            assertThat(u.calls()).isEqualTo(2);
            assertThat(u.toolCalls()).isEqualTo(1);
            assertThat(u.inputTokens()).isEqualTo(14);
        });
    }

    @Test
    void only_a_content_policy_with_a_sink_receives_content() {
        List<String> recorded = new ArrayList<>();
        ContentPolicy policy = task -> Optional.of((t, messages, output) -> recorded.add(messages + " -> " + output));
        ScriptedModel model = ScriptedModel.replying(ANSWER_MARKER);

        run(worker(model).build().handler(Chat.class, (ai, t) -> ai.reply(PROMPT_MARKER)), task("i-A", "/do/0/a", "c-1"));
        assertThat(recorded).isEmpty();   // ContentPolicy.NONE, the default

        run(worker(model).contentPolicy(policy).build().handler(Chat.class, (ai, t) -> ai.reply(PROMPT_MARKER)),
                task("i-B", "/do/0/a", "c-2"));
        assertThat(recorded).singleElement().asString().contains(PROMPT_MARKER).contains(ANSWER_MARKER);
    }

    private static TaskContext traced(TaskContext ctx) {
        Lineage taskSpan = Lineage.trace("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", null);
        return new TaskContext(ctx.taskId(), ctx.instanceId(), ctx.correlationId(), ctx.attempt(), ctx.input(),
                ctx.definition(), ctx.chainId(), new Lineage(ctx.chainId(), ctx.correlationId(), null, null)
                        .withTrace(taskSpan));
    }

    /** A tool whose call takes 6 s on the test clock. */
    public static final class SlowLookup {
        static final String RESULT = "LOOKUP-RESULT-2b77";
        private final Clock clock;
        int calls;

        SlowLookup(Clock clock) {
            this.clock = clock;
        }

        @Tool("Looks a code up")
        public String lookup(String code) {
            calls++;
            clock.advance(Duration.ofSeconds(6));
            return RESULT;
        }
    }

    /** Records every span: name, attributes, parent, failures (as the tracer receives them). */
    static final class RecordingTracer implements ExecutionTracer {
        final List<Span> spans = new CopyOnWriteArrayList<>();

        @Override
        public ExecutionSpan start(String name, Map<String, String> attributes, Lineage parent, List<Lineage> links) {
            Span span = new Span(name, TraceAttributes.requireIdentifiersOnly(Map.copyOf(attributes)), parent);
            spans.add(span);
            return span;
        }
    }

    static final class Span implements ExecutionSpan {
        final String name;
        final Map<String, String> attributes;
        final Lineage parent;
        final List<String> failures = new ArrayList<>();
        boolean closed;

        Span(String name, Map<String, String> attributes, Lineage parent) {
            this.name = name;
            this.attributes = attributes;
            this.parent = parent;
        }

        @Override
        public Lineage context() {
            return Lineage.NONE;
        }

        @Override
        public void recordFailure(Throwable failure) {
            failures.add(String.valueOf(failure.getMessage()));
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public String toString() {
            return name + attributes + failures;
        }
    }
}
