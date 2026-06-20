package io.stintflow.spi;

import java.util.concurrent.CompletionStage;

/**
 * Port: distributed timers used for task timeouts and delays.
 * <p>
 * Without this, a worker that is dispatched but never replies would suspend an instance forever.
 * Every remote dispatch arms a timer; the result arriving first cancels it.
 */
public interface TimerService {

    /** Schedule a {@code io.stintflow.timer.fire.v1} event. @return the timer id (echoes {@code req.timerId()}). */
    CompletionStage<String> schedule(TimerRequest req);

    CompletionStage<Void> cancel(String timerId);
}
