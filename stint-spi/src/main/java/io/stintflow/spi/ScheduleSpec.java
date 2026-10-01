package io.stintflow.spi;

import java.time.ZoneId;

/**
 * A recurring schedule the engine wants to exist (SDD 2.4, RF2): start {@code definition} at every
 * occurrence of {@code cron} in {@code zone}.
 *
 * @param name content-addressed (definition, cron, zone), so a changed schedule is a new one and the old
 *             one an orphan for an explicit prune (sec. 8a); valid as an AWS schedule name
 */
public record ScheduleSpec(String name, WorkflowRef definition, CronExpression cron, ZoneId zone) {
}
