package io.stintflow.aws;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.stintflow.core.CeWire;
import io.stintflow.core.DefaultCloudEventCodec;
import io.stintflow.spi.TaskResultHandler;
import io.stintflow.spi.TaskTransport;
import io.stintflow.spi.wire.CloudEventCodec;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

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
 */
public abstract class AbstractSqsResultTransport implements TaskTransport {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractSqsResultTransport.class);

    protected final CloudEventCodec codec = new DefaultCloudEventCodec();

    private volatile boolean running;
    private ExecutorService poller;

    /** Subclasses provide the SQS client and the result-queue URL used for inbound results. */
    protected abstract SqsClient sqs();

    protected abstract String resultQueueUrl();

    @Override
    public void onResult(TaskResultHandler handler) {
        if (running) {
            return;
        }
        running = true;
        poller = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "stint-aws-result-poller");
            t.setDaemon(true);
            return t;
        });
        poller.submit(() -> pollLoop(handler));
    }

    private void pollLoop(TaskResultHandler handler) {
        while (running) {
            try {
                var resp = sqs().receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(resultQueueUrl())
                        .maxNumberOfMessages(10)
                        .waitTimeSeconds(5)
                        .build());
                for (Message m : resp.messages()) {
                    handleOne(handler, m);
                }
            } catch (Exception e) {
                LOG.warn("Result poll failed: {}", e.getMessage());
            }
        }
    }

    private void handleOne(TaskResultHandler handler, Message m) {
        try {
            var event = CeWire.fromJson(m.body().getBytes(StandardCharsets.UTF_8));
            handler.handle(codec.toResult(event)).toCompletableFuture().join();
            sqs().deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(resultQueueUrl())
                    .receiptHandle(m.receiptHandle())
                    .build());
        } catch (Exception e) {
            LOG.warn("Result handling failed, leaving message for redelivery: {}", e.getMessage());
        }
    }

    protected void stopPolling() {
        running = false;
        if (poller != null) {
            poller.shutdownNow();
        }
    }
}
