package io.stintflow.aws.worker;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Runs the SQS worker binding's timed work (SDD 3a.7): the visibility extensions, the receive back-off and the
 * shutdown grace. Pluggable so tests move time by hand instead of sleeping.
 */
public interface Scheduler {

    /** Runs {@code task} once, after {@code delay}. */
    Cancellable schedule(Runnable task, Duration delay);

    /** A scheduled task that may not have run yet. */
    @FunctionalInterface
    interface Cancellable {
        void cancel();
    }

    /** A scheduler on one daemon thread; {@link AutoCloseable} so its owner can stop it. */
    static Scheduler daemon(String threadName) {
        return new DaemonScheduler(threadName);
    }

    /** The default {@link Scheduler}: a single-threaded {@link ScheduledExecutorService} on a daemon thread. */
    final class DaemonScheduler implements Scheduler, AutoCloseable {
        private final ScheduledExecutorService executor;

        DaemonScheduler(String threadName) {
            executor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, threadName);
                t.setDaemon(true);
                return t;
            });
        }

        @Override
        public Cancellable schedule(Runnable task, Duration delay) {
            ScheduledFuture<?> future = executor.schedule(task, Math.max(0, delay.toNanos()), TimeUnit.NANOSECONDS);
            return () -> future.cancel(false);
        }

        @Override
        public void close() {
            executor.shutdownNow();
        }
    }
}
