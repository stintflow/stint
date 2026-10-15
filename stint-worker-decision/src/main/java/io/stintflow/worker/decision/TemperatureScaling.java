package io.stintflow.worker.decision;

import java.util.Map;

import io.stintflow.wire.decision.DecisionKind;

/**
 * Temperature scaling (SDD 3a.2b, 4.5): the distribution's log-probabilities divided by {@code t} and normalized again
 * — the same as dividing the model's logits by {@code t} — and the confidence is the highest rescaled probability.
 * Without a distribution, the raw confidence is taken as one of two outcomes ({@code raw} and {@code 1 − raw}), which is
 * exact for a boolean. {@code t = 1} is the identity, {@code t > 1} lowers the confidence and {@code t < 1} raises it.
 * The order of the candidates never changes, so neither does a tie.
 * <p>
 * Configured per adapter with {@code stint.decision.adapters.<id>.calibration.temperature}.
 */
public final class TemperatureScaling implements ConfidenceCalibration {

    private final double t;

    public TemperatureScaling(double t) {
        if (!(t > 0) || Double.isInfinite(t)) {
            throw new IllegalArgumentException("A temperature is a finite number above 0, got " + t);
        }
        this.t = t;
    }

    public double temperature() {
        return t;
    }

    @Override
    public double calibrate(DecisionKind kind, double raw) {
        return binary(raw);
    }

    @Override
    public double calibrate(DecisionKind kind, double raw, Map<String, Double> distribution) {
        if (distribution == null || distribution.size() < 2) {
            return binary(raw);
        }
        double max = 0;
        double sum = 0;
        for (double p : distribution.values()) {
            double scaled = Math.pow(p, 1 / t);
            sum += scaled;
            max = Math.max(max, scaled);
        }
        return sum > 0 ? max / sum : raw;
    }

    private double binary(double raw) {
        double chosen = Math.pow(raw, 1 / t);
        double other = Math.pow(1 - raw, 1 / t);
        return chosen + other > 0 ? chosen / (chosen + other) : raw;
    }

    @Override
    public String toString() {
        return "TemperatureScaling[t=" + t + "]";
    }
}
