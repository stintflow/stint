package io.stintflow.worker.decision;

import io.stintflow.wire.decision.DecisionKind;

/**
 * Maps an adapter's raw confidence to the one its threshold is compared with, and the one recorded (SDD 3a.2, 4.3).
 * Identity until an adapter is calibrated — e.g. against the human corrections of SDD 3a.4. The contract doesn't
 * change: the result is still a confidence in [0, 1].
 */
@FunctionalInterface
public interface ConfidenceCalibration {

    double calibrate(DecisionKind kind, double raw);

    ConfidenceCalibration IDENTITY = (kind, raw) -> raw;
}
