package io.stintflow.spi;

/**
 * SDD 2.9, sec. 8a: what a {@link TimerFireHandler} fails with when a timer fired before the wait it guards was
 * saved (SDD 2.1, RF8 arms the timer <em>before</em> that save). The connector must not acknowledge the fire: it
 * keeps it and delivers it again a little later (SQS: back to the queue with a short visibility; in memory: on the
 * next tick). Any other failure is an ordinary error.
 */
public final class TimerFireTooEarly extends RuntimeException {

    private final String timerId;

    public TimerFireTooEarly(String timerId, String message) {
        super(message, null, false, false);
        this.timerId = timerId;
    }

    public String timerId() {
        return timerId;
    }

    /** Whether {@code failure} (possibly wrapped by a completion stage) is a too-early fire. */
    public static boolean isCause(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t instanceof TimerFireTooEarly) {
                return true;
            }
        }
        return false;
    }
}
