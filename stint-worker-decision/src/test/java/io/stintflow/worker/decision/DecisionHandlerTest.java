package io.stintflow.worker.decision;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

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

/**
 * SDD 3a.2, 4.2–4.7: the handler computes {@code uncertain} from the adapter's threshold, turns a malformed answer into
 * a validation error, and applies the policy, the token ceiling and the usage record — with the fake adapter.
 */
class DecisionHandlerTest {

    static final JsonNodeFactory NODES = JsonNodeFactory.instance;
    static final DecisionRequest MEETING = new DecisionRequest(DecisionKind.CHOICE, List.of(
            new DecisionOption("ata", "Minutes"), new DecisionOption("adr", "A decision record"),
            new DecisionOption("runbook", "Operating steps")), Json.obj().put("text", "synthetic meeting"));
    static final DecisionRequest URGENT = new DecisionRequest(DecisionKind.BOOLEAN, List.of(), Json.obj());

    private final List<String> usage = new CopyOnWriteArrayList<>();

    private TaskHandler handler(FakeDecisionAdapter adapter, Map<String, String> settings,
            ConfidenceCalibration calibration, DecisionPolicy policy) {
        Map<String, String> props = new HashMap<>(settings);
        props.put("stint.decision.routes.classify.adapter", adapter.descriptor().id());
        return DecisionHandlers.builder(DecisionConfig.fromProperties(props)).adapter(adapter, calibration)
                .policy(policy).usage((task, a, model, used) -> usage.add(a.id() + "/" + model + ":" + used))
                .build().handlers().get("classify");
    }

    private TaskHandler handler(FakeDecisionAdapter adapter, Map<String, String> settings) {
        return handler(adapter, settings, ConfidenceCalibration.IDENTITY, DecisionPolicy.ALLOW);
    }

    static FakeDecisionAdapter answering(Function<DecisionRequest, Decision> rule) {
        return new FakeDecisionAdapter("fake", rule);
    }

    static TaskContext task(DecisionRequest request) {
        return new TaskContext("/do/0/classify", "i-1", "c-1", 1, request.toJson(),
                new WorkflowRef("test", "decide", "1.0.0"), "chn-1", Lineage.NONE);
    }

    static JsonNode run(TaskHandler handler, DecisionRequest request) {
        return handler.execute(task(request)).toCompletableFuture().join();
    }

    static ErrorInfo error(TaskHandler handler, DecisionRequest request) {
        try {
            handler.execute(task(request)).toCompletableFuture().join();
        } catch (CompletionException e) {
            return ((TaskFailure) e.getCause()).toErrorInfo();
        }
        throw new AssertionError("the decision did not fail");
    }

    @Test
    void below_the_threshold_is_uncertain_and_at_or_above_it_is_not() {
        Map<String, String> threshold = Map.of("stint.decision.adapters.fake.threshold", "0.8");

        JsonNode below = run(handler(answering(r -> Decision.of(NODES.textNode("ata"), 0.79)), threshold), MEETING);
        JsonNode equal = run(handler(answering(r -> Decision.of(NODES.textNode("ata"), 0.8)), threshold), MEETING);
        JsonNode above = run(handler(answering(r -> Decision.of(NODES.textNode("adr"), 0.95)), threshold), MEETING);

        assertThat(below.path("uncertain").asBoolean()).isTrue();
        assertThat(below.path("value").asText()).isEqualTo("ata");
        assertThat(equal.path("uncertain").asBoolean()).isFalse();
        assertThat(above.path("uncertain").asBoolean()).isFalse();
        assertThat(above.path("confidence").asDouble()).isEqualTo(0.95);
        assertThat(above.path("provenance").path("adapter").asText()).isEqualTo("fake");
        assertThat(above.path("provenance").path("version").asText()).isEqualTo("1");
        assertThat(above.path("provenance").path("provider").isNull()).isTrue();
    }

    @Test
    void a_threshold_by_kind_prevails_over_the_adapter_s() {
        TaskHandler handler = handler(answering(r -> Decision.of(r.kind() == DecisionKind.BOOLEAN
                        ? NODES.booleanNode(true) : NODES.textNode("ata"), 0.9)),
                Map.of("stint.decision.adapters.fake.threshold", "0.8",
                        "stint.decision.adapters.fake.threshold.boolean", "0.95"));

        assertThat(run(handler, MEETING).path("uncertain").asBoolean()).isFalse();
        assertThat(run(handler, URGENT).path("uncertain").asBoolean()).isTrue();
    }

    @Test
    void no_confidence_or_a_tie_is_uncertain_not_an_error() {
        JsonNode missing = run(handler(answering(r -> Decision.of(NODES.textNode("ata"), null)), Map.of()), MEETING);
        JsonNode tie = run(handler(answering(r -> new Decision(NODES.textNode("ata"), 0.95,
                Map.of("ata", 0.45, "adr", 0.45, "runbook", 0.10), null, null)), Map.of()), MEETING);
        JsonNode clear = run(handler(answering(r -> new Decision(NODES.textNode("ata"), 0.95,
                Map.of("ata", 0.80, "adr", 0.15, "runbook", 0.05), null, null)), Map.of()), MEETING);

        assertThat(missing.path("uncertain").asBoolean()).isTrue();
        assertThat(missing.path("confidence").asDouble()).isZero();
        assertThat(tie.path("uncertain").asBoolean()).isTrue();
        assertThat(clear.path("uncertain").asBoolean()).isFalse();
    }

    @Test
    void the_calibrated_confidence_is_compared_and_recorded() {
        TaskHandler handler = handler(answering(r -> Decision.of(NODES.textNode("ata"), 0.9)), Map.of(),
                (kind, raw) -> raw * 0.5, DecisionPolicy.ALLOW);

        JsonNode output = run(handler, MEETING);

        assertThat(output.path("confidence").asDouble()).isEqualTo(0.45);
        assertThat(output.path("uncertain").asBoolean()).isTrue();
    }

    @Test
    void a_value_outside_the_options_or_of_the_wrong_type_is_a_validation_error() {
        ErrorInfo outside = error(handler(answering(r -> Decision.of(NODES.textNode("memo synthetic"), 0.99)),
                Map.of()), MEETING);
        ErrorInfo wrongType = error(handler(answering(r -> Decision.of(NODES.textNode("yes"), 0.99)), Map.of()),
                URGENT);
        ErrorInfo noValue = error(handler(answering(r -> Decision.of(null, 0.99)), Map.of()), MEETING);
        ErrorInfo badConfidence = error(handler(answering(r -> Decision.of(NODES.textNode("ata"), 85.0)), Map.of()),
                MEETING);

        assertThat(outside.type()).isEqualTo(ErrorInfo.TYPE_VALIDATION);
        assertThat(outside.status()).isEqualTo(400);
        assertThat(outside.detail()).isEqualTo("The decision's value is not one of the options")
                .doesNotContain("memo");
        assertThat(wrongType.detail()).isEqualTo("The decision's value does not fit a boolean decision");
        assertThat(noValue.detail()).isEqualTo("The decision's value does not fit a choice decision");
        assertThat(badConfidence.detail()).isEqualTo("The decision's confidence must be a number in [0,1]");
    }

    @Test
    void an_input_that_is_not_a_decision_request_is_a_validation_error() {
        TaskHandler handler = handler(answering(r -> Decision.of(NODES.textNode("ata"), 1.0)), Map.of());

        try {
            handler.execute(new TaskContext("/do/0/classify", "i-1", "c-1", 1, Json.obj().put("kind", "ranking"),
                    null)).toCompletableFuture().join();
            throw new AssertionError("expected a failure");
        } catch (CompletionException e) {
            assertThat(((TaskFailure) e.getCause()).toErrorInfo().type()).isEqualTo(ErrorInfo.TYPE_VALIDATION);
        }
    }

    @Test
    void the_policy_refuses_before_the_adapter_is_called() {
        FakeDecisionAdapter adapter = answering(r -> Decision.of(NODES.textNode("ata"), 1.0));
        TaskHandler handler = handler(adapter, Map.of(), ConfidenceCalibration.IDENTITY,
                (task, a) -> a.remote() ? Optional.empty() : Optional.of("local adapters are off for this test"));

        ErrorInfo error = error(handler, MEETING);

        assertThat(error.type()).isEqualTo(ErrorInfo.StandardType.AUTHORIZATION.uri());
        assertThat(error.status()).isEqualTo(403);
        assertThat(error.detail()).contains("adapter 'fake'").contains("local adapters are off");
        assertThat(adapter.requests).isEmpty();
    }

    @Test
    void over_the_token_ceiling_fails_before_the_adapter_is_called() {
        FakeDecisionAdapter adapter = answering(r -> Decision.of(NODES.textNode("ata"), 1.0));
        DecisionRequest big = new DecisionRequest(DecisionKind.CHOICE, MEETING.options(),
                Json.obj().put("text", "x".repeat(4000)));

        ErrorInfo error = error(handler(adapter, Map.of("stint.decision.adapters.fake.max-input-tokens", "500")), big);

        assertThat(error.type()).isEqualTo(ErrorInfo.StandardType.CONFIGURATION.uri());
        assertThat(error.status()).isEqualTo(413);
        assertThat(adapter.requests).isEmpty();
        assertThat(run(handler(adapter, Map.of("stint.decision.adapters.fake.max-input-tokens", "500")), MEETING)
                .path("value").asText()).isEqualTo("ata");
    }

    @Test
    void usage_is_recorded_with_the_estimated_cost() {
        TaskHandler handler = handler(answering(r -> new Decision(NODES.textNode("ata"), 0.9, null, "model-x",
                        new DecisionUsage(1, 1000, 10, Duration.ofMillis(5), null))),
                Map.of("stint.decision.adapters.fake.price.input-per-million", "2",
                        "stint.decision.adapters.fake.price.output-per-million", "10"));

        JsonNode output = run(handler, MEETING);

        assertThat(output.path("provenance").path("model").asText()).isEqualTo("model-x");
        assertThat(usage).singleElement().asString().startsWith("fake/model-x:DecisionUsage[calls=1, inputTokens=1000, "
                + "outputTokens=10, latency=PT0.005S, estimatedCost=").contains("0.0021");
        assertThat(new DecisionConfig.AdapterSettings(0.7, Map.of(), null, new BigDecimal("2"), new BigDecimal("10"))
                .cost(1000, 10)).isEqualByComparingTo("0.0021");
    }

    @Test
    void an_adapter_s_unexpected_failure_is_a_runtime_error_without_its_message() {
        TaskHandler handler = handler(answering(r -> {
            throw new IllegalStateException("leaked " + r.input().path("text").asText());
        }), Map.of());

        ErrorInfo error = error(handler, MEETING);

        assertThat(error.type()).isEqualTo(ErrorInfo.TYPE_RUNTIME);
        assertThat(error.detail()).isEqualTo("The decision adapter failed (IllegalStateException)");
    }

    @Test
    void a_task_failure_from_the_adapter_passes_through() {
        TaskHandler handler = handler(answering(r -> {
            throw DecisionFailures.http(429, Duration.ofSeconds(30), "fake-provider");
        }), Map.of());

        ErrorInfo error = error(handler, MEETING);

        assertThat(error.type()).isEqualTo(ErrorInfo.TYPE_COMMUNICATION);
        assertThat(error.status()).isEqualTo(429);
        assertThat(error.retryAfter()).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void http_statuses_map_as_the_table_of_sdd_3a3() {
        assertThat(DecisionFailures.http(401, null, "p").toErrorInfo().type())
                .isEqualTo(ErrorInfo.StandardType.AUTHENTICATION.uri());
        assertThat(DecisionFailures.http(403, null, "p").toErrorInfo().type())
                .isEqualTo(ErrorInfo.StandardType.AUTHORIZATION.uri());
        assertThat(DecisionFailures.http(422, null, "p").toErrorInfo().type())
                .isEqualTo(ErrorInfo.StandardType.CONFIGURATION.uri());
        assertThat(DecisionFailures.http(408, null, "p").toErrorInfo().type()).isEqualTo(ErrorInfo.TYPE_TIMEOUT);
        assertThat(DecisionFailures.http(529, null, "p").toErrorInfo()).satisfies(e -> {
            assertThat(e.type()).isEqualTo(ErrorInfo.TYPE_COMMUNICATION);
            assertThat(e.status()).isEqualTo(529);
        });
    }
}
