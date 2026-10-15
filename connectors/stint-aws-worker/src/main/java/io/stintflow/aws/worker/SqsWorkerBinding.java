package io.stintflow.aws.worker;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.cloudevents.CloudEvent;
import io.stintflow.spi.wire.StintEvents;
import io.stintflow.wire.CeWire;
import io.stintflow.worker.WorkerPool;
import io.stintflow.worker.WorkerRuntime;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageNotInflightException;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiptHandleIsInvalidException;
import software.amazon.awssdk.services.sqs.model.SqsException;

/**
 * The SQS binding of a worker (SDD 3a.7): receives task invokes from a queue into a {@link WorkerPool}, ships each
 * result to the result queue and deletes the message only after its result was sent.
 * <ul>
 *   <li><b>Back-pressure</b> (4.3, 4.5): asks SQS for at most {@code min(10, pool.available())} messages, and waits
 *       for a free slot — without receiving — while the pool is full, so no message waits in memory spending its
 *       visibility.</li>
 *   <li><b>Long tasks</b> (4.4): while a task runs, its message's visibility is renewed every {@code extendEvery};
 *       renewing stops when the task ends, when its attempt's deadline ({@code stintdeadline}, SDD 3a.3c) has passed,
 *       after {@code maxExtension}, or when SQS says the message is gone.</li>
 *   <li><b>Graceful stop</b> (4.6): {@link #close()} stops receiving, waits for the deliveries in flight up to
 *       {@code shutdownGrace} — still renewing them — and hands the unfinished ones back (visibility 0),
 *       unacknowledged. Nothing is ever acknowledged without its result shipped.</li>
 * </ul>
 * Logs carry ids only — the message id, the invoke's correlationId, instance and task, the receive count — never a
 * message's body.
 */
public final class SqsWorkerBinding implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SqsWorkerBinding.class);
    static final Duration MAX_BACKOFF = Duration.ofSeconds(30);
    static final Duration FIRST_BACKOFF = Duration.ofSeconds(1);
    /** SQS keeps a message in flight at most this long after its receive. */
    static final Duration SQS_MAX_IN_FLIGHT = Duration.ofHours(12);
    /** How often a full pool's wait is interrupted to see whether the binding was stopped. */
    static final Duration RECHECK = Duration.ofMillis(500);

    private final SqsClient sqs;
    private final String invokeQueueUrl;
    private final Function<CloudEvent, CompletionStage<Void>> ship;
    private final WorkerPool pool;
    private final boolean ownsPool;
    private final Duration waitTime;
    private final Duration visibilityTimeout;
    private final Duration extendEvery;
    private final Duration maxExtension;
    private final Duration shutdownGrace;
    private final InstantSource clock;
    private final Scheduler scheduler;
    private final boolean ownsScheduler;
    private final SqsWorkerMetrics metrics;
    private final int concurrency;

    private final Set<Lease> inFlight = ConcurrentHashMap.newKeySet();
    private final CompletableFuture<Void> stopping = new CompletableFuture<>();
    private final CompletableFuture<Void> receiverDone = new CompletableFuture<>();
    private volatile boolean running;
    private boolean started;
    private boolean closed;

    private SqsWorkerBinding(Builder b) {
        this.sqs = Objects.requireNonNull(b.sqs, "sqs is required");
        this.invokeQueueUrl = Objects.requireNonNull(b.invokeQueueUrl, "invokeQueueUrl is required");
        if (b.ship != null) {
            this.ship = b.ship;
        } else {
            String resultQueueUrl = Objects.requireNonNull(b.resultQueueUrl, "resultQueueUrl or shipResult is required");
            this.ship = result -> {
                try {
                    sqs.sendMessage(r -> r.queueUrl(resultQueueUrl)
                            .messageBody(new String(CeWire.toJson(result), StandardCharsets.UTF_8)));
                    return CompletableFuture.completedFuture(null);
                } catch (RuntimeException e) {
                    return CompletableFuture.failedFuture(e);
                }
            };
        }
        if (b.pool != null) {
            this.pool = b.pool;
            this.ownsPool = false;
            this.concurrency = b.pool.available();
        } else {
            this.pool = new WorkerPool(Objects.requireNonNull(b.runtime, "runtime or pool is required"), b.concurrency);
            this.ownsPool = true;
            this.concurrency = b.concurrency;
        }
        this.waitTime = b.waitTime;
        this.visibilityTimeout = b.visibilityTimeout;
        this.extendEvery = b.extendEvery != null ? b.extendEvery : b.visibilityTimeout.dividedBy(2);
        if (extendEvery.isZero() || extendEvery.isNegative() || extendEvery.compareTo(visibilityTimeout) >= 0) {
            throw new IllegalArgumentException("extendEvery must be above 0 and below visibilityTimeout ("
                    + visibilityTimeout + "), got " + extendEvery);
        }
        this.maxExtension = b.maxExtension.compareTo(SQS_MAX_IN_FLIGHT) < 0 ? b.maxExtension : SQS_MAX_IN_FLIGHT;
        this.shutdownGrace = b.shutdownGrace;
        this.clock = b.clock;
        this.ownsScheduler = b.scheduler == null;
        this.scheduler = b.scheduler != null ? b.scheduler : Scheduler.daemon("stint-sqs-worker-timer");
        this.metrics = b.metrics;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Starts receiving, on a daemon thread. Idempotent. */
    public synchronized SqsWorkerBinding start() {
        if (started) {
            return this;
        }
        if (closed) {
            throw new IllegalStateException("The binding was closed");
        }
        started = true;
        running = true;
        Thread receiver = new Thread(this::receiveLoop, "stint-sqs-worker-receiver");
        receiver.setDaemon(true);
        receiver.start();
        LOG.info("SQS worker binding started on {}: concurrency {}, waitTime {} s, visibilityTimeout {} s, extendEvery "
                + "{} s, maxExtension {}, shutdownGrace {}", invokeQueueUrl, concurrency, waitTime.toSeconds(),
                visibilityTimeout.toSeconds(), extendEvery.toSeconds(), maxExtension, shutdownGrace);
        return this;
    }

    /** Deliveries received and not finished. */
    public int inFlight() {
        return inFlight.size();
    }

    // --- receiving ---------------------------------------------------------------------------------------------

    private void receiveLoop() {
        Duration backoff = null;
        try {
            while (running) {
                try {
                    if (!pool.awaitAvailable(RECHECK)) {
                        continue;   // full: wait for a slot without receiving
                    }
                    int max = Math.min(10, pool.available());
                    if (max < 1) {
                        continue;
                    }
                    List<Message> messages = sqs.receiveMessage(r -> r.queueUrl(invokeQueueUrl)
                            .maxNumberOfMessages(max)
                            .waitTimeSeconds((int) waitTime.toSeconds())
                            .visibilityTimeout((int) visibilityTimeout.toSeconds())
                            .messageSystemAttributeNames(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT))
                            .messages();
                    backoff = null;
                    if (!messages.isEmpty()) {
                        metrics.received(messages.size());
                    }
                    for (Message message : messages) {
                        if (running) {
                            dispatch(message);
                        } else {
                            release(message, Ids.of(message, null), "it arrived after the binding was stopped");
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (RuntimeException e) {
                    metrics.receiveFailed();
                    backoff = backoff == null ? FIRST_BACKOFF
                            : (backoff.multipliedBy(2).compareTo(MAX_BACKOFF) > 0 ? MAX_BACKOFF : backoff.multipliedBy(2));
                    LOG.warn("Receive from {} failed ({}); retrying in {} ms", invokeQueueUrl, e.getClass().getName(),
                            backoff.toMillis());
                    pause(backoff);
                }
            }
        } finally {
            receiverDone.complete(null);
        }
    }

    /** Waits {@code delay} on the scheduler, or until the binding stops. */
    private void pause(Duration delay) {
        CompletableFuture<Void> elapsed = new CompletableFuture<>();
        Scheduler.Cancellable timer = scheduler.schedule(() -> elapsed.complete(null), delay);
        try {
            CompletableFuture.anyOf(elapsed, stopping).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            // neither completes exceptionally
        } finally {
            timer.cancel();
        }
    }

    private void dispatch(Message message) throws InterruptedException {
        CloudEvent invoke;
        try {
            invoke = CeWire.fromJson(message.body().getBytes(StandardCharsets.UTF_8));
        } catch (RuntimeException e) {
            metrics.unreadable();
            LOG.warn("Message {} from {} is not a task invoke ({}): not acknowledged, left for redelivery and the "
                    + "queue's dead-letter queue", message.messageId(), invokeQueueUrl, e.getClass().getName());
            return;
        }
        Ids ids = Ids.of(message, invoke);
        if (ids.receiveCount() > 1) {
            LOG.info("{} is received again (receive count {})", ids, ids.receiveCount());
        }
        Lease lease = new Lease(message.receiptHandle(), ids, deadlineOf(invoke), clock.instant());
        inFlight.add(lease);
        metrics.inFlight(inFlight.size());
        lease.renewLater();
        CompletionStage<Void> done;
        try {
            done = pool.submit(invoke, ship, () -> acknowledge(lease));
        } catch (RuntimeException e) {
            lease.stop();
            inFlight.remove(lease);
            metrics.inFlight(inFlight.size());
            metrics.notAcknowledged();
            LOG.warn("{} could not be started ({}): not acknowledged", ids, e.getClass().getName());
            return;
        }
        lease.done = done.toCompletableFuture();
        done.whenComplete((ok, failure) -> {
            lease.stop();
            inFlight.remove(lease);
            metrics.inFlight(inFlight.size());
            if (failure != null) {
                metrics.notAcknowledged();
                LOG.warn("{} was not acknowledged ({}): left for redelivery", ids, failure.getClass().getName());
            } else {
                metrics.acknowledged();
                LOG.debug("{} was acknowledged", ids);
            }
        });
    }

    private void acknowledge(Lease lease) {
        lease.stop();   // no extension after the delete
        sqs.deleteMessage(r -> r.queueUrl(invokeQueueUrl).receiptHandle(lease.receiptHandle));
    }

    /** Hands a message back to the queue right away (visibility 0), unacknowledged. */
    private void release(Message message, Ids ids, String why) {
        release(message.receiptHandle(), ids, why);
    }

    private void release(String receiptHandle, Ids ids, String why) {
        try {
            sqs.changeMessageVisibility(r -> r.queueUrl(invokeQueueUrl).receiptHandle(receiptHandle)
                    .visibilityTimeout(0));
            metrics.released();
            LOG.warn("{} handed back to the queue, unacknowledged: {}", ids, why);
        } catch (RuntimeException e) {
            LOG.warn("{} could not be handed back ({}); it returns when its visibility ends: {}", ids,
                    e.getClass().getName(), why);
        }
    }

    /** The attempt's deadline (SDD 3a.3c), if the invoke has a readable one. */
    static Instant deadlineOf(CloudEvent invoke) {
        Object value = invoke.getExtension(StintEvents.EXT_DEADLINE);
        if (value == null) {
            return null;
        }
        try {
            return Instant.parse(value.toString());
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    // --- visibility --------------------------------------------------------------------------------------------

    /** One received message while its task runs: its visibility is renewed until {@link #stop()}. */
    final class Lease {
        final String receiptHandle;
        final Ids ids;
        final Instant deadline;
        final Instant receivedAt;
        volatile CompletableFuture<Void> done;
        private boolean stopped;
        private Scheduler.Cancellable next;

        Lease(String receiptHandle, Ids ids, Instant deadline, Instant receivedAt) {
            this.receiptHandle = receiptHandle;
            this.ids = ids;
            this.deadline = deadline;
            this.receivedAt = receivedAt;
        }

        synchronized void renewLater() {
            if (!stopped) {
                next = scheduler.schedule(this::renew, extendEvery);
            }
        }

        synchronized void stop() {
            stopped = true;
            if (next != null) {
                next.cancel();
                next = null;
            }
        }

        synchronized boolean stopped() {
            return stopped;
        }

        /**
         * Holds the lease's lock through the SQS call, so {@link #stop()} waits for a renewal in progress: no extension
         * can land after the delete of an acknowledgement or the hand-back of {@link #close()}.
         */
        private synchronized void renew() {
            if (stopped) {
                return;
            }
            Instant now = clock.instant();
            if (deadline != null && !now.isBefore(deadline)) {
                stop();
                LOG.info("{} is past its deadline: its visibility is no longer extended", ids);
                return;
            }
            if (Duration.between(receivedAt, now).compareTo(maxExtension) >= 0) {
                stop();
                LOG.warn("{} has run for maxExtension ({}): its visibility is no longer extended", ids, maxExtension);
                return;
            }
            try {
                sqs.changeMessageVisibility(r -> r.queueUrl(invokeQueueUrl).receiptHandle(receiptHandle)
                        .visibilityTimeout((int) visibilityTimeout.toSeconds()));
                metrics.extended();
                LOG.debug("{}: visibility extended by {} s", ids, visibilityTimeout.toSeconds());
            } catch (ReceiptHandleIsInvalidException | MessageNotInflightException e) {
                metrics.extensionFailed();
                stop();
                LOG.warn("{}: SQS no longer holds it for this worker ({}); its visibility is no longer extended", ids,
                        e.getClass().getSimpleName());
                return;
            } catch (SqsException e) {
                metrics.extensionFailed();
                if (gone(e)) {
                    stop();
                    LOG.warn("{}: SQS no longer holds it for this worker ({}); its visibility is no longer extended",
                            ids, e.awsErrorDetails().errorCode());
                    return;
                }
                LOG.warn("{}: extending its visibility failed ({}); trying again in {} s", ids, e.getClass().getName(),
                        extendEvery.toSeconds());
            } catch (RuntimeException e) {
                metrics.extensionFailed();
                LOG.warn("{}: extending its visibility failed ({}); trying again in {} s", ids, e.getClass().getName(),
                        extendEvery.toSeconds());
            }
            renewLater();
        }
    }

    private static boolean gone(SqsException e) {
        String code = e.awsErrorDetails() == null ? null : e.awsErrorDetails().errorCode();
        return code != null && (code.contains("ReceiptHandleIsInvalid") || code.contains("MessageNotInflight"));
    }

    // --- stopping ----------------------------------------------------------------------------------------------

    /**
     * Stops gracefully (4.6): no more receives; the deliveries in flight get {@code shutdownGrace} to finish — still
     * extended — and those that don't are handed back unacknowledged.
     */
    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            running = false;
        }
        stopping.complete(null);
        if (started) {
            try {
                receiverDone.get(waitTime.toMillis() + RECHECK.toMillis() + 10_000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException | TimeoutException e) {
                LOG.warn("The receiver of {} did not stop in time ({})", invokeQueueUrl, e.getClass().getName());
            }
        }
        List<Lease> running = List.copyOf(inFlight);
        if (!running.isEmpty()) {
            CompletableFuture<Void> grace = new CompletableFuture<>();
            Scheduler.Cancellable timer = scheduler.schedule(() -> grace.complete(null), shutdownGrace);
            CompletableFuture<?>[] dones = running.stream().map(l -> l.done)
                    .filter(Objects::nonNull).toArray(CompletableFuture[]::new);
            try {
                CompletableFuture.anyOf(CompletableFuture.allOf(dones).exceptionally(t -> null), grace).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                // the deliveries' own failures are already logged
            } finally {
                timer.cancel();
            }
        }
        int handedBack = 0;
        for (Lease lease : List.copyOf(inFlight)) {
            if (lease.done != null && lease.done.isDone()) {
                continue;
            }
            lease.stop();
            release(lease.receiptHandle, lease.ids, "it did not finish within shutdownGrace (" + shutdownGrace + ")");
            handedBack++;
        }
        if (ownsPool) {
            pool.close();
        }
        if (ownsScheduler && scheduler instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                // a daemon scheduler: nothing to lose
            }
        }
        LOG.info("SQS worker binding on {} stopped: {} delivery(ies) handed back unacknowledged", invokeQueueUrl,
                handedBack);
    }

    // --- ids -----------------------------------------------------------------------------------------------------

    /** What a log line may say about a delivery: ids only. */
    record Ids(String messageId, String correlationId, String instanceId, String taskId, int receiveCount) {

        static Ids of(Message message, CloudEvent invoke) {
            String count = message.attributes().get(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT);
            int receiveCount;
            try {
                receiveCount = count == null ? 1 : Integer.parseInt(count);
            } catch (NumberFormatException e) {
                receiveCount = 1;
            }
            return new Ids(message.messageId(), ext(invoke, StintEvents.EXT_CORRELATION_ID),
                    ext(invoke, StintEvents.EXT_WORKFLOW_INSTANCE_ID), ext(invoke, StintEvents.EXT_TASK_ID),
                    receiveCount);
        }

        private static String ext(CloudEvent invoke, String name) {
            Object value = invoke == null ? null : invoke.getExtension(name);
            return value == null ? null : value.toString();
        }

        @Override
        public String toString() {
            return "Message " + messageId + " (correlation " + correlationId + ", instance " + instanceId + ", task "
                    + taskId + ")";
        }
    }

    // --- builder -------------------------------------------------------------------------------------------------

    public static final class Builder {
        private SqsClient sqs;
        private String invokeQueueUrl;
        private String resultQueueUrl;
        private Function<CloudEvent, CompletionStage<Void>> ship;
        private WorkerRuntime runtime;
        private WorkerPool pool;
        private int concurrency = 4;
        private Duration waitTime = Duration.ofSeconds(20);
        private Duration visibilityTimeout = Duration.ofSeconds(30);
        private Duration extendEvery;
        private Duration maxExtension = Duration.ofHours(1);
        private Duration shutdownGrace = Duration.ofSeconds(30);
        private InstantSource clock = InstantSource.system();
        private Scheduler scheduler;
        private SqsWorkerMetrics metrics = SqsWorkerMetrics.NONE;

        private Builder() {
        }

        /** The SQS client — the worker builds and configures it (endpoint, credentials, HTTP client). */
        public Builder sqs(SqsClient sqs) {
            this.sqs = sqs;
            return this;
        }

        public Builder invokeQueueUrl(String url) {
            this.invokeQueueUrl = url;
            return this;
        }

        /** Where results go, as a CloudEvent JSON message. Or {@link #shipResult}. */
        public Builder resultQueueUrl(String url) {
            this.resultQueueUrl = url;
            return this;
        }

        /** Ships a result some other way; the message is deleted only after the returned stage completes. */
        public Builder shipResult(Function<CloudEvent, CompletionStage<Void>> ship) {
            this.ship = ship;
            return this;
        }

        /** The runtime the binding's own pool runs. Or {@link #pool}. */
        public Builder runtime(WorkerRuntime runtime) {
            this.runtime = runtime;
            return this;
        }

        /** An existing pool, which the binding doesn't close. Its free slots at build time are the concurrency. */
        public Builder pool(WorkerPool pool) {
            this.pool = pool;
            return this;
        }

        /** Deliveries at once, for the binding's own pool. Default 4. */
        public Builder concurrency(int concurrency) {
            if (concurrency < 1) {
                throw new IllegalArgumentException("concurrency must be at least 1");
            }
            this.concurrency = concurrency;
            return this;
        }

        /** Long polling: how long a receive waits for messages, 0 to 20 s. Default 20 s. */
        public Builder waitTime(Duration waitTime) {
            if (waitTime.isNegative() || waitTime.compareTo(Duration.ofSeconds(20)) > 0) {
                throw new IllegalArgumentException("waitTime is 0 to 20 s, got " + waitTime);
            }
            this.waitTime = waitTime;
            return this;
        }

        /** The visibility asked on each receive and set by each extension, 1 s to 12 h. Default 30 s. */
        public Builder visibilityTimeout(Duration visibilityTimeout) {
            if (visibilityTimeout.compareTo(Duration.ofSeconds(1)) < 0 || visibilityTimeout.compareTo(SQS_MAX_IN_FLIGHT) > 0) {
                throw new IllegalArgumentException("visibilityTimeout is 1 s to 12 h, got " + visibilityTimeout);
            }
            this.visibilityTimeout = visibilityTimeout;
            return this;
        }

        /** How often a running task's visibility is renewed; below the visibility. Default half of it. */
        public Builder extendEvery(Duration extendEvery) {
            this.extendEvery = Objects.requireNonNull(extendEvery, "extendEvery");
            return this;
        }

        /** The longest a task's message is held, from its receive; capped at SQS's 12 h. Default 1 h. */
        public Builder maxExtension(Duration maxExtension) {
            if (maxExtension.isZero() || maxExtension.isNegative()) {
                throw new IllegalArgumentException("maxExtension must be above 0");
            }
            this.maxExtension = maxExtension;
            return this;
        }

        /** How long {@link SqsWorkerBinding#close()} waits for the deliveries in flight. Default 30 s. */
        public Builder shutdownGrace(Duration shutdownGrace) {
            if (shutdownGrace.isNegative()) {
                throw new IllegalArgumentException("shutdownGrace must not be negative");
            }
            this.shutdownGrace = shutdownGrace;
            return this;
        }

        public Builder clock(InstantSource clock) {
            this.clock = Objects.requireNonNull(clock, "clock");
            return this;
        }

        public Builder scheduler(Scheduler scheduler) {
            this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
            return this;
        }

        public Builder metrics(SqsWorkerMetrics metrics) {
            this.metrics = Objects.requireNonNull(metrics, "metrics");
            return this;
        }

        public SqsWorkerBinding build() {
            return new SqsWorkerBinding(this);
        }
    }
}
