package io.stintflow.spi;

import java.time.Instant;

/**
 * One occurrence of a schedule, as a {@link ScheduleService} delivers it (SDD 2.4, sec. 8e).
 *
 * @param scheduledTime the occurrence it stands for, or {@code null} when the connector could not tell (e.g.
 *                      the scheduler did not substitute its placeholder) — the engine then derives it from
 *                      {@code observedAt}
 * @param observedAt    when the delivery was recorded by the channel (e.g. SQS {@code SentTimestamp})
 */
public record ScheduleFire(String scheduleName, WorkflowRef definition, Instant scheduledTime, Instant observedAt) {
}
