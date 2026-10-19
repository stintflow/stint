package io.stintflow.aws.worker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

/**
 * An in-memory {@link SqsClient} for the binding's unit tests (SDD 3a.7): hands out the messages a test
 * {@link #offer}s, records every receive (with how many messages it asked and how many deliveries were in flight),
 * every visibility change, delete and send, and throws what a test scripts. No visibility semantics of its own — the
 * floci ITs cover SQS itself.
 */
public final class FakeSqs implements SqsClient {

    public record Receive(int maxNumberOfMessages, int visibilityTimeout, int handedOut) {
    }

    public record VisibilityChange(String receiptHandle, int visibilityTimeout) {
    }

    private final BlockingQueue<Message> queue = new LinkedBlockingQueue<>();
    private final AtomicInteger ids = new AtomicInteger();
    public final List<Receive> receives = new CopyOnWriteArrayList<>();
    public final List<VisibilityChange> visibilityChanges = new CopyOnWriteArrayList<>();
    public final List<String> deleted = new CopyOnWriteArrayList<>();
    public final List<String> sent = new CopyOnWriteArrayList<>();
    /** The queue URL of each entry of {@link #sent}, in the same order. */
    public final List<String> sentTo = new CopyOnWriteArrayList<>();
    /** Thrown by the next receive, if set; then cleared. */
    public volatile RuntimeException failNextReceive;
    /** Thrown by every visibility change while set. */
    public volatile Supplier<RuntimeException> failVisibility;
    /** Thrown by every send while set. */
    public volatile RuntimeException failSend;
    /** Runs inside every visibility change, before it is recorded — to hold a renewal in progress. */
    public volatile Runnable duringVisibilityChange;

    /** Queues a message with {@code body}; returns its receipt handle. */
    public String offer(String body) {
        int n = ids.incrementAndGet();
        String receipt = "receipt-" + n;
        queue.add(Message.builder().messageId("m-" + n).receiptHandle(receipt).body(body)
                .attributes(Map.of(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT, "1")).build());
        return receipt;
    }

    @Override
    public ReceiveMessageResponse receiveMessage(ReceiveMessageRequest request) {
        RuntimeException failure = failNextReceive;
        if (failure != null) {
            failNextReceive = null;
            throw failure;
        }
        int max = request.maxNumberOfMessages() == null ? 1 : request.maxNumberOfMessages();
        List<Message> out = new ArrayList<>();
        try {
            Message first = queue.poll(100, TimeUnit.MILLISECONDS);   // a short long-poll, so an idle loop doesn't spin
            if (first != null) {
                out.add(first);
                queue.drainTo(out, max - 1);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        receives.add(new Receive(max, request.visibilityTimeout() == null ? -1 : request.visibilityTimeout(),
                out.size()));
        return ReceiveMessageResponse.builder().messages(out).build();
    }

    @Override
    public ChangeMessageVisibilityResponse changeMessageVisibility(ChangeMessageVisibilityRequest request) {
        Supplier<RuntimeException> failure = failVisibility;
        if (failure != null) {
            throw failure.get();
        }
        Runnable during = duringVisibilityChange;
        if (during != null && request.visibilityTimeout() > 0) {
            during.run();
        }
        visibilityChanges.add(new VisibilityChange(request.receiptHandle(), request.visibilityTimeout()));
        return ChangeMessageVisibilityResponse.builder().build();
    }

    @Override
    public DeleteMessageResponse deleteMessage(DeleteMessageRequest request) {
        deleted.add(request.receiptHandle());
        return DeleteMessageResponse.builder().build();
    }

    @Override
    public SendMessageResponse sendMessage(SendMessageRequest request) {
        RuntimeException failure = failSend;
        if (failure != null) {
            throw failure;
        }
        sentTo.add(request.queueUrl());
        sent.add(request.messageBody());
        return SendMessageResponse.builder().messageId("r-" + sent.size()).build();
    }

    /** Visibility changes of one receipt, in order. */
    public List<Integer> visibilityOf(String receiptHandle) {
        return visibilityChanges.stream().filter(c -> c.receiptHandle().equals(receiptHandle))
                .map(VisibilityChange::visibilityTimeout).toList();
    }

    @Override
    public String serviceName() {
        return "sqs";
    }

    @Override
    public void close() {
    }
}
