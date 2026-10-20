package io.stintflow.aws.worker;

/**
 * What the SQS worker binding counts (SDD 3a.7, 4.7): numbers only, never a message's content. The default does
 * nothing; an adapter can feed a metrics library. Called from the binding's threads, so implementations are
 * thread-safe.
 */
public interface SqsWorkerMetrics {

    /** Messages received from the invoke queue by one receive. */
    default void received(int messages) {
    }

    /** Deliveries received and not finished yet, after each change. */
    default void inFlight(int deliveries) {
    }

    /** A delivery whose result was shipped and whose message was deleted. */
    default void acknowledged() {
    }

    /** A delivery left unacknowledged — handling, shipping or deleting failed — for the queue to redeliver. */
    default void notAcknowledged() {
    }

    /** A message's visibility was extended while its task ran. */
    default void extended() {
    }

    /** An extension of a message's visibility failed. */
    default void extensionFailed() {
    }

    /** A receive from the invoke queue failed; the loop retries after a back-off. */
    default void receiveFailed() {
    }

    /** A message that is not a task invoke, left for the queue's dead-letter queue. */
    default void unreadable() {
    }

    /** SDD 3a.9: an invoke whose reply name is invalid or not in the configured list: not run, not acknowledged. */
    default void replyRefused() {
    }

    /** A message handed back to the queue (visibility 0) unacknowledged, when the binding stopped. */
    default void released() {
    }

    SqsWorkerMetrics NONE = new SqsWorkerMetrics() {
    };
}
