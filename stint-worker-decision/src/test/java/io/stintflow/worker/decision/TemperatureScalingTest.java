package io.stintflow.worker.decision;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import io.stintflow.spi.Lineage;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.decision.DecisionKind;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskHandler;

/**
 * SDD 3a.2b, 4.5: temperature scaling per adapter — exact values, configured by
 * {@code stint.decision.adapters.<id>.calibration.temperature}, able to move a decision under its threshold, and
 * refused when invalid or doubled by a calibration in code.
 */
class TemperatureScalingTest {

    static final Map<String, Double> MOSTLY_ATA = Map.of("ata", 0.85, "adr", 0.12, "runbook", 0.03);

    @Test
    void a_distribution_is_rescaled_like_its_logits_and_the_confidence_is_its_highest_probability() {
        // 0.85^(1/2) / (0.85^(1/2) + 0.12^(1/2) + 0.03^(1/2)); 0.85^2 / (0.85^2 + 0.12^2 + 0.03^2)
        assertThat(new TemperatureScaling(2).calibrate(DecisionKind.CHOICE, 0.85, MOSTLY_ATA))
                .isCloseTo(0.639549, within(1e-6));
        assertThat(new TemperatureScaling(0.5).calibrate(DecisionKind.CHOICE, 0.85, MOSTLY_ATA))
                .isCloseTo(0.979263, within(1e-6));
        assertThat(new TemperatureScaling(1).calibrate(DecisionKind.CHOICE, 0.85, MOSTLY_ATA))
                .isCloseTo(0.85, within(1e-12));
    }

    @Test
    void without_a_distribution_the_confidence_is_one_of_two_outcomes() {
        assertThat(new TemperatureScaling(2).calibrate(DecisionKind.BOOLEAN, 0.8, null))
                .isCloseTo(2.0 / 3, within(1e-12));
        assertThat(new TemperatureScaling(0.5).calibrate(DecisionKind.BOOLEAN, 0.8))
                .isCloseTo(0.64 / 0.68, within(1e-12));
        assertThat(new TemperatureScaling(3).calibrate(DecisionKind.BOOLEAN, 1.0)).isEqualTo(1.0);
        assertThatThrownBy(() -> new TemperatureScaling(0)).hasMessageContaining("above 0");
    }

    @Test
    void a_configured_temperature_can_take_a_decision_under_its_threshold() {
        FakeDecisionAdapter adapter = new FakeDecisionAdapter("local",
                request -> new Decision(JsonNodeFactory.instance.textNode("ata"), 0.85, MOSTLY_ATA, null, null));
        Map<String, String> props = new HashMap<>(Map.of(
                "stint.decision.routes.classify.adapter", "local",
                "stint.decision.adapters.local.threshold", "0.7"));
        TaskHandler identity = DecisionHandlers.builder(DecisionConfig.fromProperties(props)).adapter(adapter).build()
                .handlers().get("classify");
        props.put("stint.decision.adapters.local.calibration.temperature", "2");
        TaskHandler scaled = DecisionHandlers.builder(DecisionConfig.fromProperties(props)).adapter(adapter).build()
                .handlers().get("classify");

        JsonNode before = run(identity);
        JsonNode after = run(scaled);
        JsonNode again = run(scaled);

        assertThat(before.path("confidence").asDouble()).isEqualTo(0.85);
        assertThat(before.path("uncertain").asBoolean()).isFalse();
        assertThat(after.path("confidence").asDouble()).isCloseTo(0.639549, within(1e-6));
        assertThat(after.path("uncertain").asBoolean()).isTrue();
        assertThat(again).isEqualTo(after);   // deterministic
    }

    @Test
    void an_invalid_temperature_or_one_doubled_by_a_calibration_in_code_fails_at_startup() {
        assertThatThrownBy(() -> DecisionConfig.fromProperties(Map.of(
                "stint.decision.adapters.local.calibration.temperature", "0",
                "stint.decision.adapters.other.calibration.temperature", "warm")))
                .hasMessageContaining("stint.decision.adapters.local.calibration.temperature: a temperature is a finite"
                        + " number above 0, got '0'")
                .hasMessageContaining("stint.decision.adapters.other.calibration.temperature");
        DecisionConfig config = DecisionConfig.fromProperties(Map.of(
                "stint.decision.routes.classify.adapter", "local",
                "stint.decision.adapters.local.calibration.temperature", "1.5"));
        assertThat(config.temperature("local")).contains(1.5);
        FakeDecisionAdapter adapter = new FakeDecisionAdapter("local", request -> null);
        assertThatThrownBy(() -> DecisionHandlers.builder(config).adapter(adapter, new TemperatureScaling(2)).build())
                .hasMessageContaining("stint.decision.adapters.local.calibration.temperature: adapter 'local' also has"
                        + " a calibration in code");
    }

    private static JsonNode run(TaskHandler handler) {
        return handler.execute(new TaskContext("/do/0/classify", "i-1", "c-1", 1,
                DecisionHandlerTest.MEETING.toJson(), new WorkflowRef("test", "decide", "1.0.0"), "chn-1",
                Lineage.NONE)).toCompletableFuture().join();
    }
}
