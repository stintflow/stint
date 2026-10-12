package io.stintflow.worker.decision.jev;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;
import io.stintflow.wire.decision.DecisionKind;
import io.stintflow.wire.decision.DecisionOption;
import io.stintflow.wire.decision.DecisionRequest;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;
import io.stintflow.worker.decision.DecisionConfig;
import io.stintflow.worker.decision.DecisionHandlers;
import io.stintflow.worker.decision.jev.FakeJevServer.Reply;

/**
 * SDD 3a.2, CA11 (4.4, 4.5): the Jev adapter against a fake server — the three kinds, the request it sends, the
 * provenance, and the errors (401, 422, 429 with {@code Retry-After}, 529, timeout). Never the real service.
 */
class JevDecisionAdapterTest {

    static final Instant NOW = Instant.parse("2026-10-12T12:00:00Z");
    static final DecisionRequest MEETING = new DecisionRequest(DecisionKind.CHOICE, List.of(
            new DecisionOption("ata", "Minutes"), new DecisionOption("adr", null),
            new DecisionOption("runbook", "Operating steps")), Json.obj().put("text", "synthetic meeting"));

    private final FakeJevServer server;

    JevDecisionAdapterTest() throws Exception {
        server = new FakeJevServer();
    }

    @AfterEach
    void stop() {
        server.close();
    }

    private JevDecisionAdapter.Builder adapter() {
        return JevDecisionAdapter.builder().id("jev").baseUri(server.baseUri()).apiKey(() -> "test")
                .clock(() -> NOW).timeout(Duration.ofSeconds(2));
    }

    private static TaskHandler handler(JevDecisionAdapter adapter, String threshold) {
        return DecisionHandlers.builder(DecisionConfig.fromProperties(Map.of(
                        "stint.decision.routes.classify.adapter", "jev",
                        "stint.decision.routes.classify.instructions", "Which document does this meeting produce?",
                        "stint.decision.adapters.jev.threshold", threshold)))
                .adapter(adapter).build().handlers().get("classify");
    }

    private static TaskContext task(DecisionRequest request) {
        return new TaskContext("/do/0/classify", "i-1", "c-1", 1, request.toJson(),
                new WorkflowRef("test", "decide", "1.0.0"), "chn-1", Lineage.NONE);
    }

    private static JsonNode run(TaskHandler handler, DecisionRequest request) {
        return handler.execute(task(request)).toCompletableFuture().join();
    }

    private static ErrorInfo error(TaskHandler handler, DecisionRequest request) {
        try {
            run(handler, request);
        } catch (CompletionException e) {
            return ((TaskFailure) e.getCause()).toErrorInfo();
        }
        throw new AssertionError("the decision did not fail");
    }

    @Test
    void a_choice_is_a_jev_choice_with_its_probabilities() {
        server.reply(Reply.ok("""
                {"model":"jev-1.13.0","answers":{"decision":{"type":"choice","choice":"adr",
                 "probabilities":{"ata":0.1,"adr":0.85,"runbook":0.05},"confidence":0.85}},
                 "usage":{"input_tokens":42,"output_tokens":3}}"""));

        JsonNode output = run(handler(adapter().build(), "0.8"), MEETING);

        assertThat(output.path("value").asText()).isEqualTo("adr");
        assertThat(output.path("confidence").asDouble()).isEqualTo(0.85);
        assertThat(output.path("uncertain").asBoolean()).isFalse();
        assertThat(output.path("provenance").path("adapter").asText()).isEqualTo("jev");
        assertThat(output.path("provenance").path("provider").asText()).isEqualTo("typesafe");
        assertThat(output.path("provenance").path("model").asText()).isEqualTo("jev-1.13.0");
        assertThat(output.path("provenance").path("version").asText()).isEqualTo(JevDecisionAdapter.VERSION);
        JsonNode sent = server.requests.getFirst();
        assertThat(sent.path("model").asText()).isEqualTo("jev-latest");
        assertThat(sent.path("state").path("text").asText()).isEqualTo("synthetic meeting");
        JsonNode question = sent.path("questions").path("decision");
        assertThat(question.path("type").asText()).isEqualTo("choice");
        assertThat(question.path("instructions").asText()).isEqualTo("Which document does this meeting produce?");
        assertThat(question.path("criteria").path("ata").asText()).isEqualTo("Minutes");
        assertThat(question.path("criteria").path("adr").asText()).isEqualTo("adr");   // no description: the name
        assertThat(server.authorizations).containsExactly("Bearer test");
    }

    @Test
    void a_tie_in_the_probabilities_is_uncertain() {
        server.reply(Reply.ok("""
                {"model":"jev-1.13.0","answers":{"decision":{"type":"choice","choice":"ata",
                 "probabilities":{"ata":0.45,"adr":0.45,"runbook":0.1},"confidence":0.9}},"usage":{}}"""));

        assertThat(run(handler(adapter().build(), "0.5"), MEETING).path("uncertain").asBoolean()).isTrue();
    }

    @Test
    void a_boolean_is_a_noul_and_a_score_uses_the_configured_levels() {
        server.reply(Reply.ok("{\"model\":\"jev-1.13.0\",\"answers\":{\"decision\":{\"type\":\"noul\",\"noul\":0.2}}}"));
        server.reply(Reply.ok("""
                {"model":"jev-1.13.0","answers":{"decision":{"type":"score","score":2.4,
                 "probabilities":{"0":0.05,"1":0.1,"2":0.3,"3":0.55},"confidence":0.55}}}"""));
        JevDecisionAdapter jev = adapter().scoreLevels(List.of("none", "low", "medium", "high")).build();
        DecisionRequest urgent = new DecisionRequest(DecisionKind.BOOLEAN, List.of(
                new DecisionOption("true", "Needs action today"), new DecisionOption("false", "Can wait")), Json.obj());
        DecisionRequest relevance = new DecisionRequest(DecisionKind.SCORE, List.of(), Json.obj());

        JsonNode no = run(handler(jev, "0.7"), urgent);
        JsonNode score = run(handler(jev, "0.7"), relevance);

        assertThat(no.path("value").asBoolean()).isFalse();
        assertThat(no.path("confidence").asDouble()).isEqualTo(0.8);
        assertThat(no.path("uncertain").asBoolean()).isFalse();
        assertThat(server.requests.get(0).path("questions").path("decision").path("type").asText()).isEqualTo("noul");
        assertThat(server.requests.get(0).path("questions").path("decision").path("criteria").path("true").asText())
                .isEqualTo("Needs action today");
        assertThat(score.path("value").asDouble()).isEqualTo(2.4);
        assertThat(score.path("uncertain").asBoolean()).isTrue();   // 0.55 < 0.7
        assertThat(server.requests.get(1).path("questions").path("decision").path("criteria"))
                .hasSize(4).first().extracting(JsonNode::asText).isEqualTo("none");
    }

    @Test
    void a_score_without_levels_is_a_configuration_error_without_a_call() {
        ErrorInfo error = error(handler(adapter().build(), "0.7"),
                new DecisionRequest(DecisionKind.SCORE, List.of(), Json.obj()));

        assertThat(error.type()).isEqualTo(ErrorInfo.StandardType.CONFIGURATION.uri());
        assertThat(server.requests).isEmpty();
        assertThatThrownBy(() -> adapter().scoreLevels(List.of("only")))
                .hasMessageContaining("Jev score levels are 2 to 10");
    }

    @Test
    void http_errors_become_standard_errors() {
        server.reply(Reply.status(401, Map.of()));
        server.reply(Reply.status(422, Map.of()));
        server.reply(Reply.status(529, Map.of()));
        TaskHandler handler = handler(adapter().build(), "0.5");

        ErrorInfo unauthorized = error(handler, MEETING);
        ErrorInfo invalid = error(handler, MEETING);
        ErrorInfo overloaded = error(handler, MEETING);

        assertThat(unauthorized.type()).isEqualTo(ErrorInfo.StandardType.AUTHENTICATION.uri());
        assertThat(unauthorized.status()).isEqualTo(401);
        assertThat(invalid.type()).isEqualTo(ErrorInfo.StandardType.CONFIGURATION.uri());
        assertThat(invalid.status()).isEqualTo(422);
        assertThat(overloaded.type()).isEqualTo(ErrorInfo.TYPE_COMMUNICATION);
        assertThat(overloaded.status()).isEqualTo(529);
    }

    @Test
    void a_rate_limit_carries_its_retry_after_in_seconds_or_as_a_date_or_the_default() {
        server.reply(Reply.status(429, Map.of("Retry-After", "30")));
        server.reply(Reply.status(429, Map.of("Retry-After", "Mon, 12 Oct 2026 12:01:30 GMT")));
        server.reply(Reply.status(429, Map.of()));
        TaskHandler handler = handler(adapter().rateLimitRetryAfter(Duration.ofSeconds(10)).build(), "0.5");

        assertThat(error(handler, MEETING).retryAfter()).isEqualTo(Duration.ofSeconds(30));
        assertThat(error(handler, MEETING).retryAfter()).isEqualTo(Duration.ofSeconds(90));
        assertThat(error(handler, MEETING)).satisfies(e -> {
            assertThat(e.status()).isEqualTo(429);
            assertThat(e.retryAfter()).isEqualTo(Duration.ofSeconds(10));
        });
    }

    @Test
    void a_slow_answer_is_a_timeout_and_an_unparseable_one_a_validation_error() {
        CountDownLatch release = new CountDownLatch(1);
        server.reply(new Reply(200, Map.of(), "{}", release));
        server.reply(Reply.ok("not json"));
        TaskHandler handler = handler(adapter().timeout(Duration.ofMillis(300)).build(), "0.5");

        try {
            assertThat(error(handler, MEETING).type()).isEqualTo(ErrorInfo.TYPE_TIMEOUT);
        } finally {
            release.countDown();
        }
        assertThat(error(handler, MEETING).type()).isEqualTo(ErrorInfo.TYPE_VALIDATION);
    }

    @Test
    void an_unreachable_service_is_a_communication_error() {
        server.close();
        assertThat(error(handler(adapter().build(), "0.5"), MEETING)).satisfies(e -> {
            assertThat(e.type()).isEqualTo(ErrorInfo.TYPE_COMMUNICATION);
            assertThat(e.status()).isEqualTo(503);
        });
    }
}
