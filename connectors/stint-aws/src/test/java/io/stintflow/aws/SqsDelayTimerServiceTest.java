package io.stintflow.aws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import io.stintflow.spi.TimerFire;
import io.stintflow.wire.Json;

/** SDD 2.9: the SQS timer's delay rounding (RF1, CA2), early-fire backoff and JSON (sec. 8a/8c). */
class SqsDelayTimerServiceTest {

    private static final Instant NOW = Instant.parse("2026-10-11T12:00:00Z");

    @Test
    void delay_rounds_up_to_whole_seconds() {
        assertThat(SqsDelayTimerService.delaySeconds(NOW, NOW.plusMillis(1))).isEqualTo(1);
        assertThat(SqsDelayTimerService.delaySeconds(NOW, NOW.plusMillis(999))).isEqualTo(1);
        assertThat(SqsDelayTimerService.delaySeconds(NOW, NOW.plusSeconds(1))).isEqualTo(1);
        assertThat(SqsDelayTimerService.delaySeconds(NOW, NOW.plusMillis(1001))).isEqualTo(2);
        assertThat(SqsDelayTimerService.delaySeconds(NOW, NOW.plusSeconds(900))).isEqualTo(900);
    }

    @Test
    void a_fire_at_or_before_now_has_no_delay_and_beyond_the_sqs_max_is_refused() {
        assertThat(SqsDelayTimerService.delaySeconds(NOW, NOW)).isZero();
        assertThat(SqsDelayTimerService.delaySeconds(NOW, NOW.minusSeconds(5))).isZero();
        assertThatThrownBy(() -> SqsDelayTimerService.delaySeconds(NOW, NOW.plus(Duration.ofMillis(900_001))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void early_fires_come_back_after_2_4_8_then_16_seconds() {
        assertThat(SqsDelayTimerService.earlyFireBackoffSeconds(1)).isEqualTo(2);
        assertThat(SqsDelayTimerService.earlyFireBackoffSeconds(2)).isEqualTo(4);
        assertThat(SqsDelayTimerService.earlyFireBackoffSeconds(3)).isEqualTo(8);
        assertThat(SqsDelayTimerService.earlyFireBackoffSeconds(4)).isEqualTo(16);
        assertThat(SqsDelayTimerService.earlyFireBackoffSeconds(9)).isEqualTo(16);
    }

    @Test
    void the_timer_json_reads_with_and_without_armed_at() {
        TimerFire current = SqsDelayTimerService.fireOf(Json.read(
                "{\"timerId\":\"retry:1\",\"workflowInstanceId\":\"i-1\",\"armedAt\":\"2026-10-11T12:00:00Z\"}"
                        .getBytes()));
        TimerFire before29 = SqsDelayTimerService.fireOf(Json.read(
                "{\"timerId\":\"retry:1\",\"workflowInstanceId\":\"i-1\"}".getBytes()));

        assertThat(current).isEqualTo(new TimerFire("retry:1", "i-1", NOW));
        assertThat(before29).isEqualTo(new TimerFire("retry:1", "i-1"));
        assertThat(before29.armedAt()).isNull();
    }
}
