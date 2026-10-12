package io.stintflow.core.model;

import java.time.ZoneId;
import java.util.List;

import io.stintflow.core.trigger.EventFilter;
import io.stintflow.spi.CronExpression;

/**
 * A definition's DSL 1.0 {@code schedule} (SDD 2.4, RF1): "Configures the schedule of a workflow".
 * {@code every} is compiled into {@link #cron()} (sec. 8b); {@code on} into exact-match event filters started
 * through the SDD 2.1 bindings. {@code after} is not supported.
 *
 * @param cron the recurrence, or {@code null}
 * @param zone where {@code cron} is evaluated — UTC unless the definition sets the Stint extension
 *             {@code document.metadata["stint.schedule.timezone"]} (sec. 8c)
 * @param on   DSL 1.0 {@code schedule.on} ({@code one}/{@code any}): any matching event starts an instance
 */
public record Schedule(CronExpression cron, ZoneId zone, List<EventFilter> on) {

    public static final String TIMEZONE_METADATA = "stint.schedule.timezone";

    public Schedule {
        zone = zone == null ? ZoneId.of("UTC") : zone;
        on = on == null ? List.of() : List.copyOf(on);
    }

    public static Schedule cron(CronExpression cron, ZoneId zone) {
        return new Schedule(cron, zone, List.of());
    }

    public static Schedule on(List<EventFilter> filters) {
        return new Schedule(null, null, filters);
    }
}
