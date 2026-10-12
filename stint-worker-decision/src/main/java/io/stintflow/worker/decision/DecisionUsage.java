package io.stintflow.worker.decision;

import java.math.BigDecimal;
import java.time.Duration;

/**
 * What one decision used (SDD 3a.2, 4.6) — numbers only: there is no field a prompt or an answer could go into.
 *
 * @param calls         calls to the provider
 * @param inputTokens   input tokens the provider reported
 * @param outputTokens  output tokens the provider reported
 * @param latency       time spent waiting for the provider
 * @param estimatedCost tokens times the configured prices, or {@code null} without prices
 */
public record DecisionUsage(int calls, long inputTokens, long outputTokens, Duration latency, BigDecimal estimatedCost) {

    public DecisionUsage withEstimatedCost(BigDecimal cost) {
        return new DecisionUsage(calls, inputTokens, outputTokens, latency, cost);
    }
}
