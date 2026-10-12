package io.stintflow.worker.langchain4j;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.InstantSource;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import com.fasterxml.jackson.databind.JsonNode;

import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.Capability;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.request.ResponseFormat;
import dev.langchain4j.model.chat.request.ResponseFormatType;
import dev.langchain4j.model.chat.request.json.JsonObjectSchema;
import dev.langchain4j.model.chat.request.json.JsonSchema;
import dev.langchain4j.model.chat.response.ChatResponse;
import io.stintflow.spi.ExecutionTracer;
import io.stintflow.wire.Json;
import io.stintflow.wire.decision.DecisionOption;
import io.stintflow.wire.decision.DecisionRequest;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.decision.AdapterDescriptor;
import io.stintflow.worker.decision.Decision;
import io.stintflow.worker.decision.DecisionAdapter;
import io.stintflow.worker.decision.DecisionFailures;
import io.stintflow.worker.decision.DecisionTask;
import io.stintflow.worker.decision.DecisionUsage;

/**
 * A decision adapter over a LangChain4j {@link ChatModel} (SDD 3a.2, 4.4). One call per decision, no memory:
 * <ul>
 *   <li>the system message carries the route's question, the kind and the options with their descriptions; the user
 *       message carries the input, as JSON;</li>
 *   <li>the answer is {@code {"value": ..., "confidence": 0..1}} — the confidence is the model's own (8b): not
 *       calibrated, and only comparable within this adapter, model and {@link #PROMPT_VERSION};</li>
 *   <li>with {@link Capability#RESPONSE_FORMAT_JSON_SCHEMA}, the answer is constrained by a JSON Schema whose
 *       {@code value} is an enum of the options; without it, the schema is spelled out in the system message;</li>
 *   <li>an answer that is not that JSON object is a {@code validation} error; a missing confidence is not (the handler
 *       makes the decision uncertain);</li>
 *   <li>a provider failure is a standard error by the table of SDD 3a.3 ({@link ProviderErrors}); the call is a
 *       {@code stint.llm} span with identifiers only ({@link MeteredChatModel}).</li>
 * </ul>
 * Leave the provider's own retries at 0 and its request/response logging off, as for any LLM worker (SDD 3a.3).
 */
public final class LlmDecisionAdapter implements DecisionAdapter {

    /** Goes into the provenance's {@code version}: a change of the prompt shows in every decision recorded. */
    public static final String PROMPT_VERSION = "llm-decision/1";

    private final AdapterDescriptor descriptor;
    private final ChatModel chatModel;
    private final ExecutionTracer tracer;
    private final InstantSource clock;
    private final Function<Throwable, Optional<Duration>> retryAfter;
    private final Duration rateLimitRetryAfter;

    private LlmDecisionAdapter(Builder b) {
        this.chatModel = Objects.requireNonNull(b.chatModel, "chatModel is required");
        String provider = b.provider != null ? b.provider
                : chatModel.provider() == null ? null : chatModel.provider().name().toLowerCase(Locale.ROOT);
        this.descriptor = new AdapterDescriptor(Objects.requireNonNull(b.id, "id is required"), provider, b.model,
                PROMPT_VERSION, b.remote);
        this.tracer = b.tracer;
        this.clock = b.clock;
        this.retryAfter = b.retryAfter;
        this.rateLimitRetryAfter = b.rateLimitRetryAfter;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public AdapterDescriptor descriptor() {
        return descriptor;
    }

    @Override
    public CompletionStage<Decision> decide(DecisionRequest request, DecisionTask task) {
        try (TaskIdentifiers ids = TaskIdentifiers.inMdc(task.task())) {
            MeteredChatModel model = new MeteredChatModel(chatModel, tracer, task.task(), clock);
            boolean constrained = chatModel.supportedCapabilities().contains(Capability.RESPONSE_FORMAT_JSON_SCHEMA);
            ChatRequest.Builder chat = ChatRequest.builder().messages(
                    SystemMessage.from(systemMessage(request, task.instructions(), !constrained)),
                    UserMessage.from(request.input() == null ? "null" : request.input().toString()));
            if (constrained) {
                chat.responseFormat(ResponseFormat.builder().type(ResponseFormatType.JSON)
                        .jsonSchema(schema(request)).build());
            }
            ChatResponse response;
            try {
                response = model.chat(chat.build());
            } catch (RuntimeException e) {
                throw ProviderErrors.toTaskFailure(e, retryAfter, rateLimitRetryAfter);
            }
            LlmUsage used = model.usage(0);
            DecisionUsage usage = new DecisionUsage(used.calls(), used.inputTokens(), used.outputTokens(),
                    used.latency(), null);
            String text = response.aiMessage() == null ? null : response.aiMessage().text();
            return CompletableFuture.completedFuture(parse(text, response.modelName(), usage));
        } catch (TaskFailure failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    /** The answer's {@code value} and {@code confidence}; only the shape is checked here, the handler does the rest. */
    static Decision parse(String text, String model, DecisionUsage usage) {
        JsonNode answer;
        try {
            answer = Json.read(unfenced(text).getBytes(StandardCharsets.UTF_8));
        } catch (RuntimeException e) {
            throw DecisionFailures.validation("The model's answer is not JSON");
        }
        if (answer == null || !answer.isObject()) {
            throw DecisionFailures.validation("The model's answer is not a JSON object");
        }
        JsonNode confidence = answer.get("confidence");
        Double value = null;
        if (confidence != null && !confidence.isNull()) {
            if (!confidence.isNumber()) {
                throw DecisionFailures.validation("The model's confidence is not a number");
            }
            value = confidence.doubleValue();
        }
        return new Decision(answer.get("value"), value, null, model, usage);
    }

    /** Some models wrap JSON in a Markdown fence even when told not to. */
    private static String unfenced(String text) {
        if (text == null) {
            return "";
        }
        String trimmed = text.strip();
        if (trimmed.startsWith("```")) {
            int start = trimmed.indexOf('\n');
            int end = trimmed.lastIndexOf("```");
            if (start > 0 && end > start) {
                return trimmed.substring(start + 1, end).strip();
            }
        }
        return trimmed;
    }

    static String systemMessage(DecisionRequest request, String instructions, boolean spellOutSchema) {
        StringBuilder prompt = new StringBuilder("You make one decision about the input the user sends.\n")
                .append("Question: ").append(instructions).append('\n');
        switch (request.kind()) {
            case CHOICE -> prompt.append("Answer with exactly one of these option names:\n");
            case BOOLEAN -> prompt.append("Answer true or false.\n");
            case SCORE -> prompt.append("Answer with a number.\n");
        }
        for (DecisionOption option : request.options()) {
            prompt.append("- ").append(option.name());
            if (option.description() != null && !option.description().isBlank()) {
                prompt.append(": ").append(option.description());
            }
            prompt.append('\n');
        }
        prompt.append("Say how sure you are as a confidence from 0 to 1.\n");
        if (spellOutSchema) {
            String value = switch (request.kind()) {
                case CHOICE -> "one of the option names, as a string";
                case BOOLEAN -> "true or false";
                case SCORE -> "a number";
            };
            prompt.append("Answer only with a JSON object, nothing else: {\"value\": ").append(value)
                    .append(", \"confidence\": a number from 0 to 1}\n");
        }
        return prompt.toString();
    }

    static JsonSchema schema(DecisionRequest request) {
        JsonObjectSchema.Builder root = JsonObjectSchema.builder();
        switch (request.kind()) {
            case CHOICE -> root.addEnumProperty("value", request.options().stream().map(DecisionOption::name).toList());
            case BOOLEAN -> root.addBooleanProperty("value");
            case SCORE -> root.addNumberProperty("value");
        }
        root.addNumberProperty("confidence", "How sure you are, from 0 to 1");
        root.required("value", "confidence").additionalProperties(false);
        return JsonSchema.builder().name("decision").rootElement(root.build()).build();
    }

    public static final class Builder {
        private String id;
        private ChatModel chatModel;
        private String provider;
        private String model;
        private boolean remote = true;
        private ExecutionTracer tracer = ExecutionTracer.NOOP;
        private InstantSource clock = InstantSource.system();
        private Function<Throwable, Optional<Duration>> retryAfter = t -> Optional.empty();
        private Duration rateLimitRetryAfter;

        private Builder() {
        }

        /** The adapter's id: its name in the configuration and the provenance's {@code adapter}. */
        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder chatModel(ChatModel chatModel) {
            this.chatModel = chatModel;
            return this;
        }

        /** The provider's name for the provenance; default: the model's {@code ModelProvider}, in lower case. */
        public Builder provider(String provider) {
            this.provider = provider;
            return this;
        }

        /** The configured model, for the policy of SDD 3a.5; an answer's own model name takes precedence. */
        public Builder model(String model) {
            this.model = model;
            return this;
        }

        /** Whether the input leaves the process (a hosted provider); {@code false} for a model in the worker. */
        public Builder remote(boolean remote) {
            this.remote = remote;
            return this;
        }

        public Builder tracer(ExecutionTracer tracer) {
            this.tracer = Objects.requireNonNull(tracer, "tracer");
            return this;
        }

        public Builder clock(InstantSource clock) {
            this.clock = Objects.requireNonNull(clock, "clock");
            return this;
        }

        /** As {@link LangChain4jWorker.Builder#retryAfter}. */
        public Builder retryAfter(Function<Throwable, Optional<Duration>> extractor) {
            this.retryAfter = Objects.requireNonNull(extractor, "extractor");
            return this;
        }

        public Builder rateLimitRetryAfter(Duration retryAfter) {
            this.rateLimitRetryAfter = retryAfter;
            return this;
        }

        public LlmDecisionAdapter build() {
            return new LlmDecisionAdapter(this);
        }
    }
}
