package io.stintflow.spi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/** SDD 2.4, sec. 8b/8c: the portable five-field subset, its rejections, and evaluation in a time zone. */
class CronExpressionTest {

    private static final ZoneId UTC = ZoneId.of("UTC");
    private static final ZoneId SAO_PAULO = ZoneId.of("America/Sao_Paulo");
    private static final ZoneId NEW_YORK = ZoneId.of("America/New_York");

    @ParameterizedTest
    @ValueSource(strings = {"0 7 * * 1-5", "0 9 * * MON", "0 0 1 * *", "*/15 * * * *", "0 8-18/2 * JAN-MAR *",
            "30 6 * * SUN,SAT", "0 0 * * 7", "5,35 */3 1-15 * *"})
    void accepts_the_portable_subset(String expression) {
        assertThat(CronExpression.parse(expression).expression()).isEqualTo(expression.toUpperCase());
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "0 0 1 * MON|restricts both day-of-month and day-of-week",
            "0 0 * * */2|steps are not supported on the day of the week",
            "0 0 * * FRI-MON|wrapping ranges are not supported",
            "@daily|macros",
            "0 0 ? * MON|not a number or a known name",
            "0 0 L * *|not a number or a known name",
            "0 0 * * 5#3|not a number or a known name",
            "0 0 0 * * *|exactly five fields",
            "60 * * * *|outside 0-59",
            "0 24 * * *|outside 0-23"})
    void rejects_what_has_no_portable_equivalent(String expression, String why) {
        assertThatThrownBy(() -> CronExpression.parse(expression))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(why);
    }

    @Test
    void weekdays_at_seven_in_sao_paulo() {
        CronExpression weekdays = CronExpression.parse("0 7 * * 1-5");
        // Friday 2026-10-02 08:00 in São Paulo (UTC-3): the next run is Monday 07:00 local = 10:00 UTC.
        Instant friday = ZonedDateTime.of(2026, 10, 2, 8, 0, 0, 0, SAO_PAULO).toInstant();

        Instant next = weekdays.nextAfter(friday, SAO_PAULO);

        assertThat(next).isEqualTo(Instant.parse("2026-10-05T10:00:00Z"));
        assertThat(weekdays.lastAtOrBefore(friday, SAO_PAULO)).isEqualTo(Instant.parse("2026-10-02T10:00:00Z"));
        assertThat(weekdays.lastAtOrBefore(next, SAO_PAULO)).isEqualTo(next); // "at or before" includes it
    }

    @Test
    void daylight_saving_in_new_york_skipped_and_repeated_hours() {
        // 2026-03-08 02:30 doesn't exist in New York (02:00 -> 03:00): it fires at the shifted instant, 03:30 EDT.
        CronExpression twoThirty = CronExpression.parse("30 2 * * *");
        Instant beforeGap = ZonedDateTime.of(2026, 3, 8, 0, 0, 0, 0, NEW_YORK).toInstant();
        assertThat(twoThirty.nextAfter(beforeGap, NEW_YORK)).isEqualTo(Instant.parse("2026-03-08T07:30:00Z"));

        // 2026-11-01 01:30 happens twice (EDT then EST): it fires once, at the earlier offset.
        CronExpression oneThirty = CronExpression.parse("30 1 * * *");
        Instant beforeOverlap = ZonedDateTime.of(2026, 11, 1, 0, 0, 0, 0, NEW_YORK).toInstant();
        Instant first = oneThirty.nextAfter(beforeOverlap, NEW_YORK);
        assertThat(first).isEqualTo(Instant.parse("2026-11-01T05:30:00Z"));
        assertThat(oneThirty.nextAfter(first, NEW_YORK)).isEqualTo(Instant.parse("2026-11-02T06:30:00Z"));
    }

    @Test
    void every_becomes_a_cron_only_when_it_divides_the_day() {
        assertThat(CronExpression.every(Duration.ofMinutes(15)).expression()).isEqualTo("*/15 * * * *");
        assertThat(CronExpression.every(Duration.ofMinutes(1)).expression()).isEqualTo("* * * * *");
        assertThat(CronExpression.every(Duration.ofHours(1)).expression()).isEqualTo("0 * * * *");
        assertThat(CronExpression.every(Duration.ofHours(6)).expression()).isEqualTo("0 */6 * * *");
        assertThat(CronExpression.every(Duration.ofDays(1)).expression()).isEqualTo("0 0 * * *");
        assertThatThrownBy(() -> CronExpression.every(Duration.ofMinutes(7))).hasMessageContaining("no reproducible");
        assertThatThrownBy(() -> CronExpression.every(Duration.ofHours(5))).hasMessageContaining("no reproducible");
        assertThatThrownBy(() -> CronExpression.every(Duration.ofSeconds(30))).hasMessageContaining("whole number");
    }

    @Test
    void next_and_last_occurrence_in_utc() {
        CronExpression mondays = CronExpression.parse("0 9 * * MON");
        Instant wednesday = Instant.parse("2026-09-30T12:00:00Z");
        assertThat(mondays.nextAfter(wednesday, UTC)).isEqualTo(Instant.parse("2026-10-05T09:00:00Z"));
        assertThat(mondays.lastAtOrBefore(wednesday, UTC)).isEqualTo(Instant.parse("2026-09-28T09:00:00Z"));
        CronExpression firstOfMonth = CronExpression.parse("0 0 1 * *");
        assertThat(firstOfMonth.nextAfter(wednesday, UTC)).isEqualTo(Instant.parse("2026-10-01T00:00:00Z"));
    }
}
