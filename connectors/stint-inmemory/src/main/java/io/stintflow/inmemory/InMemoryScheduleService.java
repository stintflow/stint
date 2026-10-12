package io.stintflow.inmemory;

import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.stintflow.spi.ScheduleFire;
import io.stintflow.spi.ScheduleFireHandler;
import io.stintflow.spi.ScheduleService;
import io.stintflow.spi.ScheduleSpec;

/**
 * {@link ScheduleService} for dev/test (SDD 2.4, RF4). It has no background thread: advance the injected
 * clock and call {@link #tick()}, which delivers every occurrence of every schedule in
 * {@code (last tick, now]}, in its time zone, with its exact scheduled instant. Occurrences missed while
 * nobody ticked are not caught up beyond that window (sec. 5). {@link #deliver} replays one occurrence, as a
 * duplicated delivery would.
 */
public final class InMemoryScheduleService implements ScheduleService {

    private final InstantSource clock;
    private final Map<String, Entry> schedules = new ConcurrentHashMap<>();
    private final AtomicInteger creations = new AtomicInteger();
    private volatile ScheduleFireHandler handler;

    private static final class Entry {
        final ScheduleSpec spec;
        Instant cursor;

        Entry(ScheduleSpec spec, Instant cursor) {
            this.spec = spec;
            this.cursor = cursor;
        }
    }

    public InMemoryScheduleService(InstantSource clock) {
        this.clock = clock;
    }

    @Override
    public CompletionStage<Void> ensure(ScheduleSpec spec) {
        if (schedules.putIfAbsent(spec.name(), new Entry(spec, clock.instant())) == null) {
            creations.incrementAndGet();
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<List<String>> list() {
        return CompletableFuture.completedFuture(schedules.keySet().stream().sorted().toList());
    }

    @Override
    public CompletionStage<Void> remove(String name) {
        schedules.remove(name);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void onFire(ScheduleFireHandler handler) {
        this.handler = handler;
    }

    /** How many schedules were actually created (not merely ensured again). */
    public int creations() {
        return creations.get();
    }

    /** Delivers every occurrence due by now; the stage completes once every handler stage has. */
    public CompletionStage<Void> tick() {
        Instant now = clock.instant();
        List<CompletableFuture<Void>> deliveries = new ArrayList<>();
        for (Entry entry : schedules.values()) {
            synchronized (entry) {
                Instant next = entry.spec.cron().nextAfter(entry.cursor, entry.spec.zone());
                while (!next.isAfter(now)) {
                    deliveries.add(deliver(entry.spec.name(), next).toCompletableFuture());
                    entry.cursor = next;
                    next = entry.spec.cron().nextAfter(next, entry.spec.zone());
                }
                entry.cursor = now.isAfter(entry.cursor) ? now : entry.cursor;
            }
        }
        return CompletableFuture.allOf(deliveries.toArray(CompletableFuture[]::new));
    }

    /** Delivers the occurrence of {@code name} at {@code scheduledTime} (again, if it already was). */
    public CompletionStage<Void> deliver(String name, Instant scheduledTime) {
        Entry entry = schedules.get(name);
        ScheduleFireHandler h = handler;
        if (entry == null || h == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("No schedule " + name + " or no handler"));
        }
        return h.handle(new ScheduleFire(name, entry.spec.definition(), scheduledTime, clock.instant()));
    }
}
