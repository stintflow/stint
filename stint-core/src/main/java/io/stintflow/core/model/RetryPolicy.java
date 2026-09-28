package io.stintflow.core.model;

import java.time.Duration;

/**
 * {@code retry} (SDD 1.3, RF5): how many times, how far apart, and how long to keep retrying a
 * {@link TryNode}'s body after a caught error.
 *
 * @param delay        base delay before the first retry
 * @param backoff      how {@link #delayFor} grows the base delay across attempts
 * @param maxAttempts  {@code limit.attempt.count} — total dispatches allowed, including the first
 * @param maxDuration  {@code limit.duration} measured from the first attempt; {@code null} = unbounded
 * @param jitterRatio  randomizes the computed delay by up to this fraction (0.0–1.0), both directions
 */
public record RetryPolicy(Duration delay, Backoff backoff, int maxAttempts, Duration maxDuration, double jitterRatio) {

    public enum Backoff {CONSTANT, LINEAR, EXPONENTIAL}

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1: " + maxAttempts);
        }
        if (jitterRatio < 0 || jitterRatio > 1) {
            throw new IllegalArgumentException("jitterRatio must be in [0,1]: " + jitterRatio);
        }
    }

    /** Base delay (no jitter) before dispatching {@code nextAttempt} (2-based: the first retry is attempt 2). */
    public Duration delayFor(int nextAttempt) {
        int retryIndex = nextAttempt - 1; // attempt 2 -> 1st retry -> index 1
        return switch (backoff) {
            case CONSTANT -> delay;
            case LINEAR -> delay.multipliedBy(retryIndex);
            case EXPONENTIAL -> delay.multipliedBy(1L << (retryIndex - 1));
        };
    }
}
