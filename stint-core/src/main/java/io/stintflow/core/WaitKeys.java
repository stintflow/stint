package io.stintflow.core;

/**
 * Naming convention for typed {@link io.stintflow.spi.Wait} keys (SDD 1.2 RF2, SDD 1.3 RF4/RF10).
 * {@code event:<type>:} is reserved for SDD 2.3. By SDD 1.3 convention, a timer/retry waitKey is
 * always used verbatim as the {@code timerId} passed to {@link io.stintflow.spi.TimerService} too —
 * a fired timer's id already tells the engine which kind it is, no extra lookup needed.
 */
final class WaitKeys {

    private static final String TASK_PREFIX = "task:";
    private static final String TIMER_PREFIX = "timer:";
    private static final String RETRY_PREFIX = "retry:";

    private WaitKeys() {
    }

    static String task(String correlationId) {
        return TASK_PREFIX + correlationId;
    }

    /** The timeout guarding a task dispatch — same correlationId as its {@link #task}. */
    static String timer(String correlationId) {
        return TIMER_PREFIX + correlationId;
    }

    /** A retry backoff delay — its own id, unrelated to any in-flight task correlationId. */
    static String retry(String retryId) {
        return RETRY_PREFIX + retryId;
    }

    static boolean isRetry(String waitKey) {
        return waitKey.startsWith(RETRY_PREFIX);
    }

    /** Strips the {@code task:}/{@code timer:} prefix, recovering the correlationId. */
    static String rawId(String waitKey) {
        return waitKey.substring(waitKey.indexOf(':') + 1);
    }
}
