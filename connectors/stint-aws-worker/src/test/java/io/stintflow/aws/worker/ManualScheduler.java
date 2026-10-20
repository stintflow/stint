package io.stintflow.aws.worker;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * A {@link Scheduler} whose time moves only when a test calls {@link #advance} (SDD 3a.7): no sleep, and the binding's
 * extensions, back-off and shutdown grace happen exactly when the test says. Its {@link #clock()} is the same time.
 */
public final class ManualScheduler implements Scheduler {

    private record Task(Instant due, long order, Runnable run, boolean[] cancelled) {
    }

    private final List<Task> tasks = new ArrayList<>();
    private Instant now;
    private long order;

    public ManualScheduler(Instant start) {
        this.now = start;
    }

    public synchronized InstantSource clock() {
        return () -> {
            synchronized (this) {
                return now;
            }
        };
    }

    public synchronized Instant now() {
        return now;
    }

    @Override
    public synchronized Cancellable schedule(Runnable task, Duration delay) {
        boolean[] cancelled = {false};
        tasks.add(new Task(now.plus(delay), order++, task, cancelled));
        notifyAll();
        return () -> {
            synchronized (this) {
                cancelled[0] = true;
            }
        };
    }

    /** Tasks scheduled and not cancelled, due or not. */
    public synchronized int pending() {
        return (int) tasks.stream().filter(t -> !t.cancelled()[0]).count();
    }

    /** Waits (up to 10 s of real time) until at least {@code count} tasks are pending — without moving time. */
    public synchronized void awaitPending(int count) throws InterruptedException {
        long until = System.nanoTime() + 10_000_000_000L;
        while (pending() < count) {
            long left = until - System.nanoTime();
            if (left <= 0) {
                throw new IllegalStateException("only " + pending() + " task(s) scheduled, expected " + count);
            }
            wait(Math.max(1, left / 1_000_000));
        }
    }

    /** Moves time by {@code by}, running every task due on the way, in order — those they schedule included. */
    public void advance(Duration by) {
        Instant target;
        synchronized (this) {
            target = now.plus(by);
        }
        while (true) {
            Task next;
            synchronized (this) {
                next = tasks.stream().filter(t -> !t.due().isAfter(target))
                        .min(Comparator.comparing(Task::due).thenComparingLong(Task::order)).orElse(null);
                if (next == null) {
                    now = target;
                    return;
                }
                tasks.remove(next);
                if (next.due().isAfter(now)) {
                    now = next.due();
                }
                if (next.cancelled()[0]) {
                    continue;
                }
            }
            next.run().run();
        }
    }
}
