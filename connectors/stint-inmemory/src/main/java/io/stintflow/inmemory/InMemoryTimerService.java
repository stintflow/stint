package io.stintflow.inmemory;

import java.time.Duration;
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
        timers.put(req.timerId(), req);
        return CompletableFuture.completedFuture(req.timerId());
    }

    @Override
    public CompletionStage<Void> cancel(String timerId) {
        timers.remove(timerId);
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
            h.handle(new TimerFire(timerId, req.workflowInstanceId()));
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
                h.handle(new TimerFire(req.timerId(), req.workflowInstanceId()));
            }
        }
    }
}
