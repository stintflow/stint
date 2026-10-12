package io.stintflow.inmemory;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import io.stintflow.spi.TimerFire;
import io.stintflow.spi.TimerFireTooEarly;
import io.stintflow.spi.TimerRequest;

/** SDD 2.9, sec. 8a/8c: the in-memory timer keeps a fire the engine calls too early and delivers it again. */
class InMemoryTimerEarlyFireTest {

    private static final Instant T0 = Instant.parse("2026-10-11T12:00:00Z");

    @Test
    void a_too_early_fire_is_kept_and_delivered_again_on_the_next_tick_with_its_armed_at() {
        InstantSource clock = () -> T0;
        InMemoryTimerService timers = new InMemoryTimerService(clock);
        List<TimerFire> fires = new CopyOnWriteArrayList<>();
        AtomicInteger calls = new AtomicInteger();
        timers.onFire(fire -> {
            fires.add(fire);
            return calls.incrementAndGet() == 1
                    ? CompletableFuture.failedFuture(new TimerFireTooEarly(fire.timerId(), "not yet"))
                    : CompletableFuture.completedFuture(null);
        });
        timers.schedule(new TimerRequest("retry:1", "i-1", null, T0));

        timers.tick();   // too early: kept
        timers.tick();   // delivered again: acknowledged
        timers.tick();   // nothing left

        assertThat(fires).hasSize(2).allSatisfy(fire -> {
            assertThat(fire.timerId()).isEqualTo("retry:1");
            assertThat(fire.armedAt()).isEqualTo(T0);
        });
    }

    @Test
    void any_other_failure_is_not_redelivered() {
        InMemoryTimerService timers = new InMemoryTimerService(() -> T0);
        AtomicInteger calls = new AtomicInteger();
        timers.onFire(fire -> {
            calls.incrementAndGet();
            return CompletableFuture.failedFuture(new IllegalStateException("boom"));
        });
        timers.schedule(new TimerRequest("timer:1", "i-1", "c-1", T0));

        timers.tick();
        timers.tick();

        assertThat(calls).hasValue(1);
    }
}
