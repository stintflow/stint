package io.stintflow.spi;

import java.time.Instant;

/**
 * Retry bookkeeping for the {@code TryNode} an instance is currently inside (SDD 1.3, RF10),
 * persisted on {@link InstanceSnapshot} so resumption can happen on a different engine.
 *
 * @param tryNodePointer      the JSON Pointer of the {@code TryNode} this state belongs to
 * @param attempt             1-based count of attempts dispatched so far
 * @param firstAttemptAt      when attempt 1 was dispatched (for {@code limit.duration})
 * @param lastError           the error from the most recent failed attempt, or {@code null} before any failure
 * @param currentCorrelationId the correlation id of the in-flight attempt, or {@code null} while waiting out a retry delay
 */
public record RetryState(String tryNodePointer, int attempt, Instant firstAttemptAt, ErrorInfo lastError,
        String currentCorrelationId) {
}
