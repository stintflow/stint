package io.stintflow.spi;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

/**
 * Port: distributed timers used for task timeouts and retry delays.
 * <p>
 * Without this, a worker that is dispatched but never replies would suspend an instance forever.
 * Every remote dispatch arms a timer; the result arriving first cancels it. {@link #onFire} is how
 * the orchestrator learns a timer elapsed (SDD 1.3, RF1) — connectors that can't observe their own
 * fire (a queue nobody drains, say) simply never invoke it, which just means timeouts never trigger
 * on that connector until it's wired up.
 */
public interface TimerService {

    /** Schedule a {@code io.stintflow.timer.fire.v1} event. @return the timer id (echoes {@code req.timerId()}). */
    CompletionStage<String> schedule(TimerRequest req);

    CompletionStage<Void> cancel(String timerId);

    /** Register the handler invoked when a timer fires. */
    void onFire(TimerFireHandler handler);

    /** The maximum delay this connector can natively honor (SDD 1.3, RF11) — callers must not schedule beyond it. */
    Duration maxDelay();
}
