package io.stintflow.worker.decision;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.stintflow.wire.Json;
import io.stintflow.wire.decision.DecisionKind;

/** SDD 3a.2, CA7 (4.3): the configuration is validated when the worker starts, every problem with its key. */
class DecisionConfigTest {

    private static final FakeDecisionAdapter LLM = new FakeDecisionAdapter("llm", r -> Decision.of(Json.obj(), 1.0));

    @Test
    void routes_thresholds_ceiling_and_prices_are_read_with_their_defaults() {
        DecisionConfig config = DecisionConfig.fromProperties(Map.of(
                "stint.decision.routes.classify-meeting.adapter", "llm",
                "stint.decision.routes.classify-meeting.instructions", "Which document does this meeting produce?",
                "stint.decision.routes.is-urgent.adapter", "llm",
                "stint.decision.adapters.llm.threshold", "0.8",
                "stint.decision.adapters.llm.threshold.boolean", "0.95",
                "stint.decision.adapters.llm.max-input-tokens", "20000",
                "stint.decision.adapters.llm.price.input-per-million", "0.15",
                "quarkus.http.port", "8080"));   // not a decision key: ignored

        assertThat(config.routes().get("classify-meeting").instructions())
                .isEqualTo("Which document does this meeting produce?");
        assertThat(config.routes().get("is-urgent").instructions()).isEqualTo(DecisionConfig.DEFAULT_INSTRUCTIONS);
        DecisionConfig.AdapterSettings llm = config.adapter("llm");
        assertThat(llm.thresholdFor(DecisionKind.CHOICE)).isEqualTo(0.8);
        assertThat(llm.thresholdFor(DecisionKind.BOOLEAN)).isEqualTo(0.95);
        assertThat(llm.maxInputTokens()).isEqualTo(20000);
        assertThat(llm.cost(2_000_000, 1_000)).isEqualByComparingTo(new BigDecimal("0.30"));
        assertThat(config.adapter("other").thresholdFor(DecisionKind.SCORE)).isEqualTo(DecisionConfig.DEFAULT_THRESHOLD);
        assertThat(config.adapter("other").cost(10, 10)).isNull();
    }

    @Test
    void every_problem_is_reported_with_its_key() {
        assertThatThrownBy(() -> DecisionConfig.fromProperties(Map.of(
                "stint.decision.adapters.llm.threshold", "1.5",
                "stint.decision.adapters.llm.threshold.ranking", "0.5",
                "stint.decision.adapters.jev.threshold.choice", "high",
                "stint.decision.adapters.jev.max-input-tokens", "0",
                "stint.decision.adapters.jev.price.input-per-million", "-1",
                "stint.decision.adapters.jev.api-key", "secret",
                "stint.decision.routes.orphan.instructions", "Which?",
                "stint.decision.thresold", "0.5")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("stint.decision.adapters.llm.threshold: a threshold is a number in [0, 1], got '1.5'")
                .hasMessageContaining("stint.decision.adapters.llm.threshold.ranking: unknown decision kind 'ranking'")
                .hasMessageContaining("stint.decision.adapters.jev.threshold.choice: a threshold is a number in [0, 1]")
                .hasMessageContaining("stint.decision.adapters.jev.max-input-tokens: must be a positive integer")
                .hasMessageContaining("stint.decision.adapters.jev.price.input-per-million: a price is a non-negative")
                .hasMessageContaining("stint.decision.adapters.jev.api-key: unknown adapter key")   // never credentials
                .hasMessageContaining("stint.decision.routes.orphan.adapter: route 'orphan' has instructions but no adapter")
                .hasMessageContaining("stint.decision.thresold: unknown decision key")
                .hasMessageNotContaining("secret");
    }

    @Test
    void a_route_or_setting_naming_a_missing_adapter_fails_the_build() {
        DecisionConfig config = DecisionConfig.fromProperties(Map.of(
                "stint.decision.routes.classify.adapter", "jev",
                "stint.decision.adapters.lmm.threshold", "0.8"));

        assertThatThrownBy(() -> DecisionHandlers.builder(config).adapter(LLM).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("stint.decision.routes.classify.adapter: no adapter 'jev'")
                .hasMessageContaining("stint.decision.adapters.lmm: no adapter 'lmm'");
        assertThatThrownBy(() -> DecisionHandlers.builder(config).adapter(LLM).adapter(LLM))
                .hasMessageContaining("Two decision adapters share the id 'llm'");
    }
}
