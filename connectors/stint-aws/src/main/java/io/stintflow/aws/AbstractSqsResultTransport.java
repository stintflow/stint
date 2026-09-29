package io.stintflow.aws;

import java.nio.charset.StandardCharsets;

import io.stintflow.wire.CeWire;
import io.stintflow.wire.DefaultCloudEventCodec;
import io.stintflow.spi.TaskResultHandler;
import io.stintflow.spi.TaskTransport;
import io.stintflow.spi.wire.CloudEventCodec;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * Base for AWS transports: results always come back over an SQS <em>result queue</em>, regardless of
 * how the invoke was dispatched (SQS, SNS, or EventBridge). Subclasses only implement {@code dispatch}.
 * <p>
 * This is the "one inbound, many outbound" shape that lets the dispatch side vary per AWS service
 * while result handling stays uniform.
 * <p>
 * SDD 1.3, sec. 8g/RF9: a message is deleted only after {@code handler.handle(...)} completes
 * successfully. A failure leaves it for SQS to redeliver once its visibility timeout elapses —
 * relying on the engine already being idempotent per-correlation (SDD 1.2), so at-least-once
 * redelivery is safe. Result handling here is bookkeeping-only (no remote calls), so it comfortably
 * fits inside a queue's default visibility timeout — no heartbeat/extension is implemented (RNF3).
 * The poll/ack loop itself is {@link SqsPollLoop} (extracted in SDD 2.1, shared with
 * {@link SqsDomainEventSource}).
 */
public abstract class AbstractSqsResultTransport implements TaskTransport {

    protected final CloudEventCodec codec = new DefaultCloudEventCodec();

    private SqsPollLoop loop;

    /** Subclasses provide the SQS client and the result-queue URL used for inbound results. */
    protected abstract SqsClient sqs();

    protected abstract String resultQueueUrl();

    @Override
    public synchronized void onResult(TaskResultHandler handler) {
        if (loop != null) {
            return;
        }
        loop = new SqsPollLoop(this::sqs, this::resultQueueUrl, "stint-aws-result-poller",
                m -> handler.handle(codec.toResult(CeWire.fromJson(m.body().getBytes(StandardCharsets.UTF_8)))));
        loop.start();
    }

    protected synchronized void stopPolling() {
        if (loop != null) {
            loop.stop();
        }
    }
}
