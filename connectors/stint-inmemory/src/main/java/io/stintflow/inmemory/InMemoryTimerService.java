package io.stintflow.inmemory;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.stintflow.spi.TimerFire;
import io.stintflow.spi.TimerFireHandler;
import io.stintflow.spi.TimerFireTooEarly;
import io.stintflow.spi.TimerRequest;
import io.stintflow.spi.TimerService;

/**
 * Records armed timers and fires them for real (SDD 1.3, RF1). Two modes:
 * <ul>
 *   <li>production ({@link #InMemoryTimerService()}): a background sweep on the system clock;</li>
 *   <li>test ({@link #InMemoryTimerService(InstantSource)}): no background thread — advance the
 *       injected clock and call {@link #tick()} yourself (SDD 1.3, sec. 8f — no sleeps).</li>
 * </ul>
 */
public final class InMemoryTimerService implements TimerService {

    private static final Duration SWEEP_INTERVAL = Duration.ofMillis(20);

    private final Map<String, TimerRequest> timers = new ConcurrentHashMap<>();
    /** SDD 2.9, sec. 8c: when each timer was armed, on this service's clock. */
    private final Map<String, Instant> armedAt = new ConcurrentHashMap<>();
    private final InstantSource clock;
    private final ScheduledExecutorService sweeper;

    private volatile TimerFireHandler handler;

    public InMemoryTimerService() {
        this(InstantSource.system(), true);
    }

    /** Test mode: no background sweep — call {@link #tick()} after advancing {@code clock}. */
    public InMemoryTimerService(InstantSource clock) {
        this(clock, false);
    }

    private InMemoryTimerService(InstantSource clock, boolean autoSweep) {
        this.clock = clock;
        if (autoSweep) {
            this.sweeper = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "stint-inmemory-timer-sweep");
                t.setDaemon(true);
                return t;
            });
            this.sweeper.scheduleWithFixedDelay(this::tick, SWEEP_INTERVAL.toMillis(), SWEEP_INTERVAL.toMillis(),
                    TimeUnit.MILLISECONDS);
        } else {
            this.sweeper = null;
        }
    }

    @Override
    public CompletionStage<String> schedule(TimerRequest req) {
        armedAt.put(req.timerId(), clock.instant());
        timers.put(req.timerId(), req);
        return CompletableFuture.completedFuture(req.timerId());
    }

    @Override
    public CompletionStage<Void> cancel(String timerId) {
        timers.remove(timerId);
        armedAt.remove(timerId);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void onFire(TimerFireHandler handler) {
        this.handler = handler;
    }

    @Override
    public Duration maxDelay() {
        return Duration.ofDays(365); // effectively unlimited for dev/test
    }

    /** Test hook: fires one specific timer immediately, regardless of the clock. Idempotent. */
    public void fire(String timerId) {
        TimerFireHandler h = handler;
        TimerRequest req = timers.remove(timerId);
        if (h != null && req != null) {
            deliver(h, req);
        }
    }

    /** Fires every timer whose {@code fireAt} is at or before the current clock instant. */
    public void tick() {
        TimerFireHandler h = handler;
        if (h == null) {
            return;
        }
        var now = clock.instant();
        for (TimerRequest req : timers.values()) {
            if (!req.fireAt().isAfter(now) && timers.remove(req.timerId(), req)) {
                deliver(h, req);
            }
        }
    }

    /**
     * SDD 2.9, sec. 8a: a fire the engine reports as {@link TimerFireTooEarly} (its wait isn't saved yet) is kept
     * and delivered again on the next tick, like the SQS connector leaves it on the queue.
     */
    private void deliver(TimerFireHandler h, TimerRequest req) {
        h.handle(new TimerFire(req.timerId(), req.workflowInstanceId(), armedAt.get(req.timerId())))
                .whenComplete((v, failure) -> {
                    if (failure != null && TimerFireTooEarly.isCause(failure)) {
                        timers.putIfAbsent(req.timerId(), req);
                    } else if (!timers.containsKey(req.timerId())) {
                        armedAt.remove(req.timerId());
                    }
                });
    }
}
