package io.stintflow.spi;

import java.time.Instant;

/**
 * Asks a {@link TimerService} to fire a {@code io.stintflow.timer.fire.v1} CloudEvent at {@link #fireAt()}.
 * Used primarily to bound how long the orchestrator waits for a remote task before retrying or failing.
 *
 * @param timerId            unique id, used to {@link TimerService#cancel(String)} when the result arrives first
 * @param workflowInstanceId the instance to notify
 * @param correlationId      the in-flight task this timeout guards
 * @param fireAt             absolute time the timer should fire
 */
public record TimerRequest(
        String timerId,
        String workflowInstanceId,
        String correlationId,
        Instant fireAt) {
}
