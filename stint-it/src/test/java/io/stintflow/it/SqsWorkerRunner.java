package io.stintflow.it;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.cloudevents.CloudEvent;
import io.stintflow.core.CeWire;
import io.stintflow.worker.WorkerRuntime;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * Prototype of the AWS Lambda/worker binding: polls the invoke queue, runs the task via
 * {@link WorkerRuntime}, and publishes the result to the result queue. In production this is the
 * Lambda handler; here it runs in-process so the IT exercises the real SQS round-trip.
 */
final class SqsWorkerRunner implements AutoCloseable {

    private final SqsClient sqs;
    private final String invokeQueueUrl;
    private final String resultQueueUrl;
    private final WorkerRuntime worker;
    private volatile boolean running = true;
    private final ExecutorService pool = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "stint-it-worker");
        t.setDaemon(true);
        return t;
    });

    SqsWorkerRunner(SqsClient sqs, String invokeQueueUrl, String resultQueueUrl, WorkerRuntime worker) {
        this.sqs = sqs;
        this.invokeQueueUrl = invokeQueueUrl;
        this.resultQueueUrl = resultQueueUrl;
        this.worker = worker;
        pool.submit(this::loop);
    }

    private void loop() {
        while (running) {
            try {
                var resp = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(invokeQueueUrl).maxNumberOfMessages(10).waitTimeSeconds(2).build());
                for (Message m : resp.messages()) {
                    CloudEvent invoke = CeWire.fromJson(m.body().getBytes(StandardCharsets.UTF_8));
                    CloudEvent result = worker.handle(invoke).toCompletableFuture().join();
                    sqs.sendMessage(SendMessageRequest.builder()
                            .queueUrl(resultQueueUrl)
                            .messageBody(new String(CeWire.toJson(result), StandardCharsets.UTF_8))
                            .build());
                    sqs.deleteMessage(DeleteMessageRequest.builder()
                            .queueUrl(invokeQueueUrl).receiptHandle(m.receiptHandle()).build());
                }
            } catch (Exception e) {
                // transient during shutdown / floci warmup; the loop retries
            }
        }
    }

    @Override
    public void close() {
        running = false;
        pool.shutdownNow();
    }
}
