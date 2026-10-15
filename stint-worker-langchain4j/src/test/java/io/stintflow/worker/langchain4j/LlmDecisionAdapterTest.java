package io.stintflow.worker.langchain4j;

import static io.stintflow.worker.langchain4j.Fakes.answer;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.exception.AuthenticationException;
import dev.langchain4j.exception.RateLimitException;
import dev.langchain4j.model.chat.Capability;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.request.json.JsonEnumSchema;
import dev.langchain4j.model.chat.request.json.JsonObjectSchema;
import dev.langchain4j.model.chat.response.ChatResponse;
import io.stintflow.spi.ErrorInfo;
import io.stintflow.wire.Json;
import io.stintflow.wire.decision.DecisionKind;
import io.stintflow.wire.decision.DecisionOption;
import io.stintflow.wire.decision.DecisionRequest;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;
import io.stintflow.worker.decision.DecisionConfig;
import io.stintflow.worker.decision.DecisionHandlers;
import io.stintflow.worker.langchain4j.Fakes.Logs;

/**
 * SDD 3a.2, 4.4: the LLM adapter over a scripted LangChain4j model — the request it builds (question, options, a JSON
 * Schema when the model supports one), how it reads the answer, its errors, and no input text in its logs.
 */
class LlmDecisionAdapterTest {

    static final String MARKER = "INPUT-MARKER-3e51";
    static final DecisionRequest MEETING = new DecisionRequest(DecisionKind.CHOICE, List.of(
            new DecisionOption("ata", "Minutes"), new DecisionOption("adr", "A decision record"),
            new DecisionOption("runbook", "Operating steps")), Json.obj().put("text", MARKER));

    /** A scripted model that may claim JSON Schema support. */
    static final class Model extends Fakes.ScriptedModel {
        private final boolean schema;

        Model(boolean schema, java.util.function.Function<ChatRequest, ChatResponse> rule) {
            super(rule);
            this.schema = schema;
        }

        @Override
        public Set<Capability> supportedCapabilities() {
            return schema ? Set.of(Capability.RESPONSE_FORMAT_JSON_SCHEMA) : Set.of();
        }
    }

    private static TaskHandler handler(Model model, String threshold) {
        LlmDecisionAdapter adapter = LlmDecisionAdapter.builder().id("llm").chatModel(model).provider("scripted")
                .rateLimitRetryAfter(Duration.ofSeconds(20)).build();
        return DecisionHandlers.builder(DecisionConfig.fromProperties(Map.of(
                        "stint.decision.routes.classify.adapter", "llm",
                        "stint.decision.routes.classify.instructions", "Which document does this meeting produce?",
                        "stint.decision.adapters.llm.threshold", threshold)))
                .adapter(adapter).build().handlers().get("classify");
    }

    private static JsonNode run(TaskHandler handler) {
        return handler.execute(DecisionTaskContexts.of(MEETING)).toCompletableFuture().join();
    }

    private static ErrorInfo error(TaskHandler handler) {
        try {
            run(handler);
        } catch (CompletionException e) {
            return ((TaskFailure) e.getCause()).toErrorInfo();
        }
        throw new AssertionError("the decision did not fail");
    }

    @Test
    void with_json_schema_support_the_answer_is_constrained_to_the_options() {
        Model model = new Model(true, r -> answer("{\"value\":\"adr\",\"confidence\":0.92}"));

        JsonNode output = run(handler(model, "0.8"));

        assertThat(output.path("value").asText()).isEqualTo("adr");
        assertThat(output.path("uncertain").asBoolean()).isFalse();
        assertThat(output.path("provenance").path("adapter").asText()).isEqualTo("llm");
        assertThat(output.path("provenance").path("provider").asText()).isEqualTo("scripted");
        assertThat(output.path("provenance").path("model").asText()).isEqualTo("scripted-model");
        assertThat(output.path("provenance").path("version").asText()).isEqualTo(LlmDecisionAdapter.PROMPT_VERSION);
        ChatRequest request = model.requests.getFirst();
        JsonObjectSchema root = (JsonObjectSchema) request.responseFormat().jsonSchema().rootElement();
        assertThat(((JsonEnumSchema) root.properties().get("value")).enumValues()).containsExactly("ata", "adr", "runbook");
        assertThat(root.required()).containsExactly("value", "confidence");
        String system = ((SystemMessage) request.messages().getFirst()).text();
        assertThat(system).contains("Question: Which document does this meeting produce?")
                .contains("- adr: A decision record").doesNotContain("Answer only with a JSON object");
        assertThat(((UserMessage) request.messages().get(1)).singleText()).contains(MARKER);
    }

    @Test
    void without_json_schema_support_the_schema_is_spelled_out_and_a_fenced_answer_is_read() {
        Model model = new Model(false, r -> answer("```json\n{\"value\":\"ata\",\"confidence\":0.55}\n```"));

        JsonNode output = run(handler(model, "0.8"));

        assertThat(output.path("value").asText()).isEqualTo("ata");
        assertThat(output.path("uncertain").asBoolean()).isTrue();   // 0.55 < 0.8
        assertThat(model.requests.getFirst().responseFormat()).isNull();
        assertThat(((SystemMessage) model.requests.getFirst().messages().getFirst()).text())
                .contains("Answer only with a JSON object");
    }

    @Test
    void a_missing_confidence_is_uncertain_and_a_malformed_answer_is_a_validation_error() {
        JsonNode missing = run(handler(new Model(true, r -> answer("{\"value\":\"ata\"}")), "0.5"));
        ErrorInfo prose = error(handler(new Model(false, r -> answer("It is clearly an ata. " + MARKER)), "0.5"));
        ErrorInfo text = error(handler(new Model(false, r -> answer("{\"value\":\"ata\",\"confidence\":\"high\"}")),
                "0.5"));
        ErrorInfo outside = error(handler(new Model(false, r -> answer("{\"value\":\"memo\",\"confidence\":0.9}")),
                "0.5"));

        assertThat(missing.path("uncertain").asBoolean()).isTrue();
        assertThat(prose.type()).isEqualTo(ErrorInfo.TYPE_VALIDATION);
        assertThat(prose.detail()).isEqualTo("The model's answer is not JSON");
        assertThat(text.detail()).isEqualTo("The model's confidence is not a number");
        assertThat(outside.detail()).isEqualTo("The decision's value is not one of the options");
    }

    @Test
    void provider_errors_map_as_in_sdd_3a3() {
        ErrorInfo limited = error(handler(new Model(true, r -> {
            throw new RateLimitException("quota for " + MARKER);
        }), "0.5"));
        ErrorInfo denied = error(handler(new Model(true, r -> {
            throw new AuthenticationException("bad key");
        }), "0.5"));

        assertThat(limited.type()).isEqualTo(ErrorInfo.TYPE_COMMUNICATION);
        assertThat(limited.status()).isEqualTo(429);
        assertThat(limited.retryAfter()).isEqualTo(Duration.ofSeconds(20));
        assertThat(limited.detail()).doesNotContain(MARKER);
        assertThat(denied.type()).isEqualTo(ErrorInfo.StandardType.AUTHENTICATION.uri());
    }

    @Test
    void neither_the_input_nor_the_answer_reaches_the_logs() {
        Model model = new Model(true, r -> answer("{\"value\":\"ata\",\"confidence\":0.9,\"why\":\"" + MARKER + "\"}"));

        try (Logs logs = new Logs()) {
            run(handler(model, "0.5"));
            error(handler(new Model(false, r -> answer("prose " + MARKER)), "0.5"));

            assertThat(logs.lines).anySatisfy(line -> assertThat(line).contains("Decision usage of task /do/0/classify")
                    .contains("adapter=llm").contains("inputTokens=11"));
            assertThat(logs.joined()).doesNotContain(MARKER);
        }
    }
}
