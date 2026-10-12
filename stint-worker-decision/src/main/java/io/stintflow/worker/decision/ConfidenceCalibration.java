package io.stintflow.worker.decision;

import java.util.Map;

import io.stintflow.wire.decision.DecisionKind;

/**
 * Maps an adapter's raw confidence to the one its threshold is compared with, and the one recorded (SDD 3a.2, 4.3).
 * Identity until an adapter is calibrated — e.g. by {@link TemperatureScaling}, or against the human corrections of
 * SDD 3a.4. The contract doesn't change: the result is still a confidence in [0, 1].
 */
@FunctionalInterface
public interface ConfidenceCalibration {

    double calibrate(DecisionKind kind, double raw);

    /**
     * With the decision's distribution, when the adapter has one (SDD 3a.2b, 4.5): what the handler calls. By default
     * the distribution is ignored.
     *
     * @param distribution the probability of each candidate value, or {@code null}
     */
    default double calibrate(DecisionKind kind, double raw, Map<String, Double> distribution) {
        return calibrate(kind, raw);
    }

    ConfidenceCalibration IDENTITY = (kind, raw) -> raw;
}
