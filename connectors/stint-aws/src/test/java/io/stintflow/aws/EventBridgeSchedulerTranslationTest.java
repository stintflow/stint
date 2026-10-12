package io.stintflow.aws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.stintflow.spi.CronExpression;
import io.stintflow.spi.ScheduleFire;
import io.stintflow.spi.WorkflowRef;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;

/**
 * SDD 2.4, unit level: the cron translation into the EventBridge Scheduler format (sec. 8b), how a schedule
 * delivery is read — with the placeholder substituted (AWS) and left as is (floci, sec. 8e) — and how a long
 * timer is named and rounded (sec. 8d).
 */
class EventBridgeSchedulerTranslationTest {

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "0 7 * * 1-5|cron(0 7 ? * 2-6 *)",
            "0 9 * * MON|cron(0 9 ? * MON *)",
            "0 0 1 * *|cron(0 0 1 * ? *)",
            "*/15 * * * *|cron(*/15 * * * ? *)",
            "0 0 * * 0|cron(0 0 ? * 1 *)",
            "0 0 * * 7|cron(0 0 ? * 1 *)",
            "0 6 * * 0,6|cron(0 6 ? * 1,7 *)",
            "0 6 * * SUN,SAT|cron(0 6 ? * SUN,SAT *)",
            "0 8-18/2 * JAN-MAR *|cron(0 8-18/2 * JAN-MAR ? *)"})
    void translates_five_fields_into_the_aws_six(String posix, String aws) {
        assertThat(AwsCron.of(CronExpression.parse(posix))).isEqualTo(aws);
    }

    @Test
    void a_delivery_with_the_substituted_scheduled_time_carries_it() {
        Message message = Message.builder()
                .body("{\"schedule\":\"stint-abc\",\"definition\":\"reports:send-report:1.0.0\","
                        + "\"scheduledTime\":\"2026-10-05T10:00:00Z\"}")
                .attributes(Map.of(MessageSystemAttributeName.SENT_TIMESTAMP, "1791194405123"))
                .build();

        ScheduleFire fire = EventBridgeSchedulerService.toFire(message, false);

        assertThat(fire.scheduleName()).isEqualTo("stint-abc");
        assertThat(fire.definition()).isEqualTo(new WorkflowRef("reports", "send-report", "1.0.0"));
        assertThat(fire.scheduledTime()).isEqualTo(Instant.parse("2026-10-05T10:00:00Z"));
        assertThat(fire.observedAt()).isEqualTo(Instant.ofEpochMilli(1791194405123L));
    }

    @Test
    void ca4_without_the_fallback_a_delivery_with_the_placeholder_left_as_is_is_rejected() {
        Message message = Message.builder()
                .body("{\"schedule\":\"stint-abc\",\"definition\":\"reports:send-report:1.0.0\","
                        + "\"scheduledTime\":\"" + EventBridgeSchedulerService.SCHEDULED_TIME_PLACEHOLDER + "\"}")
                .attributes(Map.of(MessageSystemAttributeName.SENT_TIMESTAMP, "1791194405123"))
                .build();

        // SDD 2.7, sec. 8c: on AWS this means a misconfigured Input; the poller doesn't ack it.
        assertThatThrownBy(() -> EventBridgeSchedulerService.toFire(message, false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("sent-timestamp-fallback");
    }

    @Test
    void with_the_fallback_a_delivery_with_the_placeholder_left_as_is_has_no_scheduled_time() {
        Message message = Message.builder()
                .body("{\"schedule\":\"stint-abc\",\"definition\":\"reports:send-report:1.0.0\","
                        + "\"scheduledTime\":\"" + EventBridgeSchedulerService.SCHEDULED_TIME_PLACEHOLDER + "\"}")
                .attributes(Map.of(MessageSystemAttributeName.SENT_TIMESTAMP, "1791194405123"))
                .build();

        ScheduleFire fire = EventBridgeSchedulerService.toFire(message, true);

        assertThat(fire.scheduledTime()).isNull(); // the engine derives it from observedAt
        assertThat(fire.observedAt()).isEqualTo(Instant.ofEpochMilli(1791194405123L));
    }

    @Test
    void a_long_timer_is_rounded_up_to_the_minute_and_named_from_its_id() {
        assertThat(EventBridgeSchedulerTimerService.roundUpToMinute(Instant.parse("2026-10-01T10:00:01Z")))
                .isEqualTo(Instant.parse("2026-10-01T10:01:00Z")); // never early
        assertThat(EventBridgeSchedulerTimerService.roundUpToMinute(Instant.parse("2026-10-01T10:00:00Z")))
                .isEqualTo(Instant.parse("2026-10-01T10:00:00Z"));
        assertThat(EventBridgeSchedulerTimerService.scheduleName("timer:9b1c-77"))
                .isEqualTo("timer-9b1c-77");
    }
}
