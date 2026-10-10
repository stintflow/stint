package io.stintflow.spi;

/**
 * A {@code io.stintflow.timer.fire.v1} event: the timer identified by {@code timerId} (which is,
 * by SDD 1.3 convention, the exact {@link Wait#waitKey()} it guards — {@code timer:<correlationId>}
 * for a task timeout, {@code retry:<retryId>} for a retry delay) has elapsed.
 */
public record TimerFire(String timerId, String workflowInstanceId) {
}
