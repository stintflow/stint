package io.stintflow.spi;

import java.time.Duration;

/**
 * Honest, machine-readable declaration of what a transport connector actually guarantees.
 * <p>
 * This is the antidote to "naive agnosticism": the core reads these and adapts instead of assuming
 * uniformity. For example, when {@code input} exceeds {@link #maxPayloadBytes()} the core applies the
 * claim-check pattern via a {@link BlobStore}; when {@link #dedupSupported()} is false the core
 * deduplicates by {@code correlationId + attempt}.
 *
 * @param delivery            delivery guarantee of the underlying transport
 * @param ordered             whether ordering is guaranteed
 * @param maxPayloadBytes     maximum on-wire payload (e.g. SQS = 262144). {@link Long#MAX_VALUE} = effectively unbounded
 * @param maxExecutionTime    practical cap on a single task execution (e.g. Lambda = 15m); {@code null} = unbounded
 * @param nativeDelaySupported whether the transport can natively delay delivery (e.g. SQS DelaySeconds)
 * @param dedupSupported      whether the transport deduplicates messages (e.g. SQS FIFO)
 */
public record AdapterCapabilities(
        DeliveryGuarantee delivery,
        boolean ordered,
        long maxPayloadBytes,
        Duration maxExecutionTime,
        boolean nativeDelaySupported,
        boolean dedupSupported) {

    public enum DeliveryGuarantee {AT_MOST_ONCE, AT_LEAST_ONCE, EXACTLY_ONCE}
}
