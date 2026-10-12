package io.stintflow.core;

import java.time.Duration;
import java.time.InstantSource;
import java.util.concurrent.CompletionStage;

import io.stintflow.spi.TimerFireHandler;
import io.stintflow.spi.TimerRequest;
import io.stintflow.spi.TimerService;

/**
 * A {@link TimerService} made of two (SDD 2.4, RF3, sec. 8d): delays up to {@code shortTimers.maxDelay()}
 * go to {@code shortTimers} (e.g. SQS {@code DelaySeconds}), longer ones to {@code longTimers} (e.g.
 * EventBridge Scheduler one-time schedules). Its {@link #maxDelay()} is the long one's, so a definition
 * registered against it may wait for hours — an SDD 2.3 {@code listen} of 48 h, say.
 * <p>
 * Both deliver fires to the same handler. {@link #cancel} asks both: cancellation is best-effort anyway, and
 * a fire whose wait is gone is a no-op (SDD 1.3).
 */
public final class DelayRoutingTimerService implements TimerService {

    private final TimerService shortTimers;
    private final TimerService longTimers;
    private final InstantSource clock;

    public DelayRoutingTimerService(TimerService shortTimers, TimerService longTimers, InstantSource clock) {
        this.shortTimers = shortTimers;
        this.longTimers = longTimers;
        this.clock = clock;
    }

    @Override
    public CompletionStage<String> schedule(TimerRequest req) {
        Duration delay = Duration.between(clock.instant(), req.fireAt());
        return delay.compareTo(shortTimers.maxDelay()) <= 0 ? shortTimers.schedule(req) : longTimers.schedule(req);
    }

    @Override
    public CompletionStage<Void> cancel(String timerId) {
        return shortTimers.cancel(timerId).thenCompose(v -> longTimers.cancel(timerId));
    }

    @Override
    public void onFire(TimerFireHandler handler) {
        shortTimers.onFire(handler);
        longTimers.onFire(handler);
    }

    @Override
    public Duration maxDelay() {
        return longTimers.maxDelay();
    }
}
