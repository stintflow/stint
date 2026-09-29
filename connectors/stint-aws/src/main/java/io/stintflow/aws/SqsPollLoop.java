package io.stintflow.aws;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

/**
 * Long-polls one SQS queue on a daemon thread and acknowledges (deletes) a message only after its
 * handler's stage completes normally (SDD 1.3, sec. 8g). Any failure — decoding included — leaves the
 * message for SQS to redeliver once its visibility timeout elapses and, past the queue's
 * {@code maxReceiveCount}, to move it to the dead-letter queue (SDD 2.1, sec. 8d). Shared by the
 * result transports and {@link SqsDomainEventSource}.
 */
final class SqsPollLoop {

    private static final Logger LOG = LoggerFactory.getLogger(SqsPollLoop.class);

    private final Supplier<SqsClient> sqs;
    private final Supplier<String> queueUrl;
    private final String threadName;
    private final Function<Message, CompletionStage<Void>> handler;

    private volatile boolean running;
    private ExecutorService poller;

    SqsPollLoop(Supplier<SqsClient> sqs, Supplier<String> queueUrl, String threadName,
            Function<Message, CompletionStage<Void>> handler) {
        this.sqs = sqs;
        this.queueUrl = queueUrl;
        this.threadName = threadName;
        this.handler = handler;
    }

    /** Idempotent: a second call while running is a no-op. */
    synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        poller = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, threadName);
            t.setDaemon(true);
            return t;
        });
        poller.submit(this::pollLoop);
    }

    synchronized void stop() {
        running = false;
        if (poller != null) {
            poller.shutdownNow();
        }
    }

    private void pollLoop() {
        while (running) {
            try {
                var resp = sqs.get().receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl.get())
                        .maxNumberOfMessages(10)
                        .waitTimeSeconds(5)
                        .build());
                for (Message m : resp.messages()) {
                    handleOne(m);
                }
            } catch (Exception e) {
                LOG.warn("Poll of {} failed: {}", queueUrl.get(), e.getMessage());
            }
        }
    }

    private void handleOne(Message m) {
        try {
            handler.apply(m).toCompletableFuture().join();
            sqs.get().deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(queueUrl.get())
                    .receiptHandle(m.receiptHandle())
                    .build());
        } catch (Exception e) {
            LOG.warn("Handling message {} from {} failed, leaving it for redelivery: {}",
                    m.messageId(), queueUrl.get(), e.getMessage());
        }
    }
}
