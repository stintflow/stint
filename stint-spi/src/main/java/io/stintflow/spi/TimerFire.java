package io.stintflow.spi;

import java.time.Instant;

/**
 * A {@code io.stintflow.timer.fire.v1} event: the timer identified by {@code timerId} (which is,
 * by SDD 1.3 convention, the exact {@link Wait#waitKey()} it guards — {@code timer:<correlationId>}
 * for a task timeout, {@code retry:<retryId>} for a retry delay) has elapsed.
 * <p>
 * SDD 2.9, sec. 8c: {@code armedAt} is when the connector armed the timer, carried in the timer's JSON. It lets
 * the engine tell a fire that arrived before the save of its wait (early: redelivered) from an obsolete one.
 * {@code null} for timers armed before SDD 2.9 (or by a connector that doesn't record it).
 */
public record TimerFire(String timerId, String workflowInstanceId, Instant armedAt) {

    /** A fire without {@code armedAt} (the pre-SDD-2.9 format). */
    public TimerFire(String timerId, String workflowInstanceId) {
        this(timerId, workflowInstanceId, null);
    }
}
