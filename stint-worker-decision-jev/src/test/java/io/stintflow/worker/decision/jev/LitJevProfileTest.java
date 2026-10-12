package io.stintflow.worker.decision.jev;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
import io.stintflow.worker.decision.jev.JevDecisionAdapter.ConfidenceSource;
import io.stintflow.worker.decision.jev.JevDecisionAdapter.Server;

/**
 * SDD 3a.2b, CA1, CA2, CA4 and extras: the Jev adapter's local profile against a fake server answering as LitJev does
 * — Gini concentration in {@code confidence}, the checkpoint in {@code model}, no authentication. Never a real server
 * or model.
 */
class LitJevProfileTest {

    static final DecisionRequest MEETING = new DecisionRequest(DecisionKind.CHOICE, List.of(
            new DecisionOption("ata", "Minutes"), new DecisionOption("adr", "A decision record"),
            new DecisionOption("runbook", "Operating steps")), Json.obj().put("text", "synthetic meeting"));
    static final Map<String, Double> MOSTLY_ATA = ordered("ata", 0.85, "adr", 0.12, "runbook", 0.03);

    private final FakeJevServer server;

    LitJevProfileTest() throws Exception {
        server = new FakeJevServer();
    }

    @AfterEach
    void stop() {
        server.close();
    }

    private JevDecisionAdapter.Builder local() {
        return JevDecisionAdapter.builder().server(Server.LITJEV).baseUri(server.baseUri());
    }

    private static TaskHandler handler(JevDecisionAdapter adapter, String... settings) {
        Map<String, String> props = new HashMap<>(Map.of(
                "stint.decision.routes.classify.adapter", adapter.descriptor().id(),
                "stint.decision.routes.classify.instructions", "Which document does this meeting produce?"));
        for (int i = 0; i < settings.length; i += 2) {
            props.put("stint.decision.adapters." + adapter.descriptor().id() + "." + settings[i], settings[i + 1]);
        }
        return DecisionHandlers.builder(DecisionConfig.fromProperties(props)).adapter(adapter).build().handlers()
                .get("classify");
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

    private static Map<String, Double> ordered(Object... pairs) {
        Map<String, Double> map = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            map.put((String) pairs[i], (Double) pairs[i + 1]);
        }
        return map;
    }

    @Test
    void a_choice_s_confidence_is_the_highest_probability_by_default_or_the_server_s_gini_if_configured() {
        server.reply(Reply.litjev("choice", "ata", MOSTLY_ATA));
        server.reply(Reply.litjev("choice", "ata", MOSTLY_ATA));

        JsonNode byProbability = run(handler(local().build(), "threshold", "0.7"), MEETING);
        JsonNode byServer = run(handler(local().confidenceSource(ConfidenceSource.SERVER).build(),
                "threshold", "0.7"), MEETING);

        assertThat(byProbability.path("value").asText()).isEqualTo("ata");
        assertThat(byProbability.path("confidence").asDouble()).isEqualTo(0.85);
        assertThat(byProbability.path("uncertain").asBoolean()).isFalse();
        assertThat(byServer.path("value").asText()).isEqualTo("ata");
        assertThat(byServer.path("confidence").asDouble()).isCloseTo(0.6067, within(1e-4));
        assertThat(byServer.path("uncertain").asBoolean()).isTrue();   // the same answer, under 0.7 by its Gini
    }

    @Test
    void a_score_and_a_noul_in_litjev_s_format() {
        Map<String, Double> levels = ordered("0", 0.05, "1", 0.30, "2", 0.65);
        server.reply(Reply.litjev("score", 1.6, levels));
        server.reply(Reply.litjev("score", 1.6, levels));
        server.reply(Reply.litjevNoul(0.78));
        server.reply(Reply.litjevNoul(0.78));
        DecisionRequest relevance = new DecisionRequest(DecisionKind.SCORE, List.of(), MEETING.input());
        DecisionRequest urgent = new DecisionRequest(DecisionKind.BOOLEAN, List.of(), MEETING.input());
        JevDecisionAdapter.Builder scored = local().scoreLevels(List.of("low", "medium", "high"));

        JsonNode byProbability = run(handler(scored.build(), "threshold", "0.6"), relevance);
        JsonNode byServer = run(handler(scored.confidenceSource(ConfidenceSource.SERVER).build(),
                "threshold", "0.6"), relevance);
        JsonNode yes = run(handler(local().build()), urgent);
        JsonNode yesByServer = run(handler(local().confidenceSource(ConfidenceSource.SERVER).build()), urgent);

        assertThat(byProbability.path("value").asDouble()).isEqualTo(1.6);
        assertThat(byProbability.path("confidence").asDouble()).isEqualTo(0.65);
        assertThat(byProbability.path("uncertain").asBoolean()).isFalse();
        assertThat(byServer.path("confidence").asDouble()).isCloseTo(0.2725, within(1e-4));
        assertThat(byServer.path("uncertain").asBoolean()).isTrue();
        assertThat(server.requests.get(0).path("questions").path("decision").path("criteria")).hasSize(3);
        // a noul is max(p, 1 - p) whatever the source
        assertThat(yes.path("value").asBoolean()).isTrue();
        assertThat(yes.path("confidence").asDouble()).isEqualTo(0.78);
        assertThat(yesByServer.path("confidence").asDouble()).isEqualTo(0.78);
    }

    @Test
    void without_a_key_the_local_profile_starts_and_sends_no_authorization() {
        server.reply(Reply.litjev("choice", "ata", MOSTLY_ATA));
        server.reply(Reply.litjev("choice", "ata", MOSTLY_ATA));

        run(handler(local().build()), MEETING);
        run(handler(local().id("proxied").apiKey(() -> " ").build()), MEETING);

        assertThat(server.authorizations).containsExactly(null, null);
        assertThat(server.requests.getFirst().path("model").asText()).isEqualTo("litjev");
    }

    @Test
    void the_hosted_profile_still_needs_a_key() {
        assertThatThrownBy(() -> JevDecisionAdapter.builder().baseUri(server.baseUri()).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("needs an API key");
    }

    @Test
    void the_local_profile_is_local_and_names_server_model_and_readout_in_its_provenance() {
        server.reply(Reply.litjev("choice", "ata", MOSTLY_ATA));
        JevDecisionAdapter adapter = local().model("synthetic/qwen-checkpoint").serverVersion("2cd915a")
                .readout("content").build();

        JsonNode output = run(handler(adapter), MEETING);

        assertThat(adapter.descriptor().remote()).isFalse();
        assertThat(adapter.descriptor().provider()).isEqualTo("litjev");
        assertThat(adapter.descriptor().id()).isEqualTo("litjev");
        JsonNode provenance = output.path("provenance");
        assertThat(provenance.path("adapter").asText()).isEqualTo("litjev");
        assertThat(provenance.path("provider").asText()).isEqualTo("litjev");
        assertThat(provenance.path("model").asText()).isEqualTo(FakeJevServer.LITJEV_MODEL);
        assertThat(provenance.path("version").asText())
                .isEqualTo("jev-adapter/1;server=litjev@2cd915a;readout=content");
        assertThat(server.requests.getFirst().path("model").asText()).isEqualTo("synthetic/qwen-checkpoint");
        assertThat(local().build().descriptor().version()).isEqualTo("jev-adapter/1;server=litjev");
        assertThatThrownBy(() -> local().readout("content; remote")).hasMessageContaining("readout is letters");
    }

    @Test
    void the_local_profile_keeps_its_defaults_and_the_hosted_one_is_unchanged() {
        JevDecisionAdapter localDefaults = JevDecisionAdapter.builder().server(Server.LITJEV).build();
        JevDecisionAdapter hosted = JevDecisionAdapter.builder().apiKey(() -> "test").build();

        assertThat(localDefaults.descriptor()).satisfies(d -> {
            assertThat(d.id()).isEqualTo("litjev");
            assertThat(d.model()).isEqualTo("litjev");
            assertThat(d.remote()).isFalse();
        });
        assertThat(hosted.descriptor()).satisfies(d -> {
            assertThat(d.id()).isEqualTo("jev");
            assertThat(d.provider()).isEqualTo("typesafe");
            assertThat(d.model()).isEqualTo("jev-latest");
            assertThat(d.version()).isEqualTo(JevDecisionAdapter.VERSION);
            assertThat(d.remote()).isTrue();
        });
    }

    @Test
    void a_non_loopback_address_in_the_local_profile_fails_unless_allowed_and_is_then_remote() {
        URI elsewhere = URI.create("http://10.0.0.5:8000");

        assertThatThrownBy(() -> local().baseUri(elsewhere).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-loopback address (10.0.0.5)")
                .hasMessageContaining("allowRemote(true)");
        assertThat(local().baseUri(elsewhere).allowRemote(true).build().descriptor().remote()).isTrue();
        assertThat(local().allowRemote(true).build().descriptor().remote()).isFalse();   // loopback stays local
    }

    @Test
    void loopback_is_decided_by_the_host_s_text() {
        for (String local : List.of("http://127.0.0.1:8000", "http://127.1.2.3", "http://localhost:8000",
                "http://LOCALHOST", "http://[::1]:8000")) {
            assertThat(JevDecisionAdapter.isLoopback(URI.create(local))).as(local).isTrue();
        }
        for (String remote : List.of("http://10.0.0.5", "http://128.0.0.1", "http://127.0.0.256",
                "http://127.0.0.1.example.com", "http://localhost.example.com", "https://api.typesafe.ai",
                "http://[::2]")) {
            assertThat(JevDecisionAdapter.isLoopback(URI.create(remote))).as(remote).isFalse();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"null", "\"\"", "\"   \"", "{}", "[]"})
    void a_decision_without_input_is_a_validation_error_before_any_call(String input) {
        DecisionRequest empty = new DecisionRequest(DecisionKind.CHOICE, MEETING.options(),
                Json.read(input.getBytes(StandardCharsets.UTF_8)));
        JevDecisionAdapter hosted = JevDecisionAdapter.builder().id("jev").baseUri(server.baseUri())
                .apiKey(() -> "test").build();

        for (JevDecisionAdapter adapter : List.of(local().build(), hosted)) {
            ErrorInfo error = error(handler(adapter), empty);
            assertThat(error.type()).isEqualTo(ErrorInfo.TYPE_VALIDATION);
            assertThat(error.status()).isEqualTo(400);
            assertThat(error.detail()).isEqualTo("The decision has no input to decide on");
        }
        assertThat(server.requests).isEmpty();
    }

    @Test
    void a_missing_input_is_no_input_too() {
        ObjectNode request = (ObjectNode) MEETING.toJson();
        request.remove(DecisionRequest.INPUT);

        assertThat(JevDecisionAdapter.hasNoInput(DecisionRequest.fromJson(request).input())).isTrue();
        assertThat(JevDecisionAdapter.hasNoInput(Json.obj().put("text", ""))).isFalse();   // a field, even empty
        assertThat(JevDecisionAdapter.hasNoInput(Json.read("0".getBytes(StandardCharsets.UTF_8)))).isFalse();
    }

    @Test
    void probabilities_outside_0_1_are_a_validation_error_and_a_missing_distribution_is_uncertain() {
        server.reply(Reply.ok("""
                {"model":"m","answers":{"decision":{"type":"choice","choice":"ata",
                 "probabilities":{"ata":1.4,"adr":-0.4},"confidence":0.9}}}"""));
        server.reply(Reply.ok("""
                {"model":"m","answers":{"decision":{"type":"choice","choice":"ata","confidence":0.9}}}"""));
        TaskHandler handler = handler(local().build());

        assertThat(error(handler, MEETING).type()).isEqualTo(ErrorInfo.TYPE_VALIDATION);
        JsonNode noDistribution = run(handler, MEETING);
        assertThat(noDistribution.path("confidence").asDouble()).isZero();
        assertThat(noDistribution.path("uncertain").asBoolean()).isTrue();
    }

    @Test
    void the_server_s_errors_map_as_in_sdd_3a2() {
        server.reply(Reply.status(422, Map.of()));
        server.reply(Reply.status(500, Map.of()));
        TaskHandler handler = handler(local().timeout(Duration.ofSeconds(2)).build());

        ErrorInfo invalid = error(handler, MEETING);
        ErrorInfo failed = error(handler, MEETING);

        assertThat(invalid.type()).isEqualTo(ErrorInfo.StandardType.CONFIGURATION.uri());
        assertThat(invalid.status()).isEqualTo(422);
        assertThat(failed.type()).isEqualTo(ErrorInfo.TYPE_COMMUNICATION);
        assertThat(failed.status()).isEqualTo(500);
        server.close();
        assertThat(error(handler, MEETING)).satisfies(e -> {
            assertThat(e.type()).isEqualTo(ErrorInfo.TYPE_COMMUNICATION);
            assertThat(e.status()).isEqualTo(503);
            assertThat(e.detail()).contains("litjev");
        });
    }
}
