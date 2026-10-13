package io.stintflow.aws.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import org.jboss.logmanager.ExtLogRecord;
import org.jboss.logmanager.LogContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.stintflow.spi.Lineage;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.CeWire;
import io.stintflow.wire.DefaultCloudEventCodec;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerRuntime;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sqs.model.ReceiptHandleIsInvalidException;

/**
 * SDD 3a.7, unit level, over {@link FakeSqs} and {@link ManualScheduler}: the visibility of a running task is extended
 * until it ends, its deadline passes or {@code maxExtension} is reached; a transient extension failure is retried and
 * a lost message stops it; the pool is never asked for more than its free slots; a failed receive doesn't stop the
 * loop; an unreadable message and a failed shipment are never acknowledged; a graceful close finishes, then hands
 * back. Barriers and the manual scheduler — no sleep.
 */
class SqsWorkerBindingTest {

    static final Instant T0 = Instant.parse("2026-10-12T12:00:00Z");
    static final String SECRET = "SQS-BODY-MARKER-91f3";

    private final DefaultCloudEventCodec codec = new DefaultCloudEventCodec();
    private final FakeSqs sqs = new FakeSqs();
    private final ManualScheduler scheduler = new ManualScheduler(T0);
    private final Counting metrics = new Counting();
    private final Map<String, CountDownLatch> started = new ConcurrentHashMap<>();
    private final Map<String, CountDownLatch> release = new ConcurrentHashMap<>();
    private final AtomicInteger running = new AtomicInteger();
    private final AtomicInteger maxRunning = new AtomicInteger();
    private SqsWorkerBinding binding;

    private final TaskHandlerRegistry handlers = new TaskHandlerRegistry()
            .register("hold", ctx -> {
                int now = running.incrementAndGet();
                maxRunning.accumulateAndGet(now, Math::max);
                latch(started, ctx.correlationId()).countDown();
                try {
                    await(latch(release, ctx.correlationId()));
                } finally {
                    running.decrementAndGet();
                }
                return CompletableFuture.completedFuture(Json.obj().put("done", ctx.correlationId()));
            })
            .register("fast", ctx -> CompletableFuture.completedFuture(Json.obj().put("done", ctx.correlationId())));

    @AfterEach
    void stop() {
        release.values().forEach(CountDownLatch::countDown);
        if (binding != null) {
            CompletableFuture.runAsync(binding::close);
            scheduler.advance(Duration.ofMinutes(5));
        }
    }

    private static CountDownLatch latch(Map<String, CountDownLatch> latches, String correlationId) {
        return latches.computeIfAbsent(correlationId, k -> new CountDownLatch(1));
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("the test never released the barrier");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private String offer(String correlationId, String routingKey, Instant deadline) {
        TaskInvocation inv = new TaskInvocation("i-1", "/do/0/" + routingKey, correlationId,
                new WorkflowRef("test", "sqs", "1.0.0"), routingKey, Json.obj().put("text", SECRET), 1, Lineage.NONE,
                deadline);
        return sqs.offer(new String(CeWire.toJson(codec.toEvent(inv)), StandardCharsets.UTF_8));
    }

    private SqsWorkerBinding.Builder binding(int concurrency) {
        return SqsWorkerBinding.builder().sqs(sqs).invokeQueueUrl("invoke").resultQueueUrl("result")
                .runtime(new WorkerRuntime(handlers, null)).concurrency(concurrency)
                .waitTime(Duration.ZERO).visibilityTimeout(Duration.ofSeconds(30)).extendEvery(Duration.ofSeconds(10))
                .clock(scheduler.clock()).scheduler(scheduler).metrics(metrics);
    }

    private SqsWorkerBinding start(SqsWorkerBinding.Builder builder) {
        binding = builder.build().start();
        return binding;
    }

    private static void awaitStarted(Map<String, CountDownLatch> started, String correlationId) throws Exception {
        assertThat(latch(started, correlationId).await(10, TimeUnit.SECONDS)).as("%s started", correlationId).isTrue();
    }

    private static void awaitCondition(java.util.function.BooleanSupplier condition, String what) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > until) {
                throw new AssertionError("timed out waiting until " + what);
            }
            Thread.onSpinWait();
        }
    }

    @Test
    void a_running_task_is_extended_until_it_ends_and_never_after_its_delete() throws Exception {
        start(binding(1));
        String receipt = offer("c-1", "hold", null);
        awaitStarted(started, "c-1");
        scheduler.awaitPending(1);

        scheduler.advance(Duration.ofSeconds(10));
        scheduler.advance(Duration.ofSeconds(10));
        assertThat(sqs.visibilityOf(receipt)).containsExactly(30, 30);
        assertThat(metrics.extended).hasValue(2);

        latch(release, "c-1").countDown();
        awaitCondition(() -> sqs.deleted.contains(receipt), "c-1 is deleted");
        scheduler.advance(Duration.ofSeconds(60));

        assertThat(sqs.visibilityOf(receipt)).containsExactly(30, 30);   // nothing after the delete
        assertThat(sqs.sent).hasSize(1);
        assertThat(sqs.receives).allSatisfy(r -> assertThat(r.visibilityTimeout()).isEqualTo(30));
        awaitCondition(() -> metrics.acknowledged.get() == 1, "c-1 is counted as acknowledged");
    }

    @Test
    void extending_stops_at_the_attempt_s_deadline_and_at_max_extension() throws Exception {
        start(binding(2).maxExtension(Duration.ofSeconds(35)));
        String withDeadline = offer("c-1", "hold", T0.plusSeconds(25));
        String withoutDeadline = offer("c-2", "hold", null);
        awaitStarted(started, "c-1");
        awaitStarted(started, "c-2");
        scheduler.awaitPending(2);

        scheduler.advance(Duration.ofSeconds(60));

        assertThat(sqs.visibilityOf(withDeadline)).hasSize(2);       // at 10 and 20 s; at 30 s the deadline has passed
        assertThat(sqs.visibilityOf(withoutDeadline)).hasSize(3);    // at 10, 20 and 30 s; at 40 s past maxExtension
        assertThat(scheduler.pending()).isZero();
        assertThat(sqs.deleted).isEmpty();                           // the tasks still run: nothing acknowledged
    }

    @Test
    void a_transient_extension_failure_is_retried_and_a_lost_message_stops_extending() throws Exception {
        start(binding(1));
        String receipt = offer("c-1", "hold", null);
        awaitStarted(started, "c-1");
        scheduler.awaitPending(1);

        sqs.failVisibility = () -> SdkClientException.create("synthetic network failure");
        scheduler.advance(Duration.ofSeconds(10));
        sqs.failVisibility = null;
        scheduler.advance(Duration.ofSeconds(10));
        assertThat(sqs.visibilityOf(receipt)).containsExactly(30);   // the retry at 20 s worked
        assertThat(metrics.extensionFailed).hasValue(1);

        sqs.failVisibility = () -> ReceiptHandleIsInvalidException.builder().message("synthetic").build();
        scheduler.advance(Duration.ofSeconds(10));
        assertThat(metrics.extensionFailed).hasValue(2);
        assertThat(scheduler.pending()).isZero();                    // gone: no further extension
    }

    @Test
    void the_pool_is_never_asked_for_more_than_its_free_slots() throws Exception {
        start(binding(2));
        for (int i = 1; i <= 6; i++) {
            offer("c-" + i, "hold", null);
        }
        awaitCondition(() -> running.get() == 2, "two tasks run");

        assertThat(sqs.receives).allSatisfy(r -> assertThat(r.maxNumberOfMessages()).isBetween(1, 2));
        assertThat(sqs.receives.stream().mapToInt(FakeSqs.Receive::handedOut).sum()).isEqualTo(2);
        assertThat(binding.inFlight()).isEqualTo(2);

        for (int i = 1; i <= 6; i++) {
            awaitCondition(() -> running.get() >= 1 || sqs.deleted.size() == 6, "a task runs");
            String next = started.entrySet().stream().filter(e -> e.getValue().getCount() == 0
                    && latch(release, e.getKey()).getCount() == 1).map(Map.Entry::getKey).findFirst().orElse(null);
            if (next != null) {
                latch(release, next).countDown();
            }
            int done = i;
            awaitCondition(() -> sqs.deleted.size() >= done, done + " deleted");
        }
        assertThat(sqs.deleted).hasSize(6);
        assertThat(maxRunning).hasValue(2);                          // never more than the concurrency
    }

    @Test
    void a_failed_receive_does_not_stop_the_loop() throws Exception {
        sqs.failNextReceive = SdkClientException.create("synthetic receive failure");
        try (Logs logs = new Logs()) {
            start(binding(1));
            scheduler.awaitPending(1);                               // the back-off
            assertThat(metrics.receiveFailed).hasValue(1);
            String receipt = offer("c-1", "fast", null);

            scheduler.advance(Duration.ofSeconds(1));
            awaitCondition(() -> sqs.deleted.contains(receipt), "the next message is processed");

            assertThat(logs.lines).anySatisfy(line -> assertThat(line).startsWith("WARN")
                    .contains("Receive from invoke failed (" + SdkClientException.class.getName() + "); retrying in 1000 ms"));
        }
    }

    @Test
    void an_unreadable_message_and_a_failed_shipment_are_never_acknowledged_and_logs_carry_ids_only() throws Exception {
        try (Logs logs = new Logs()) {
            start(binding(1));
            String unreadable = sqs.offer("not a cloud event " + SECRET);
            awaitCondition(() -> metrics.unreadable.get() == 1, "the unreadable message is seen");
            sqs.failSend = new IllegalStateException("synthetic result queue failure");
            String unshipped = offer("c-2", "fast", null);
            awaitCondition(() -> metrics.notAcknowledged.get() == 1, "the failed shipment is counted");
            sqs.failSend = null;
            String fine = offer("c-3", "fast", null);
            awaitCondition(() -> sqs.deleted.contains(fine), "c-3 is deleted");

            assertThat(sqs.deleted).containsExactly(fine);
            assertThat(sqs.deleted).doesNotContain(unreadable, unshipped);
            assertThat(scheduler.pending()).isZero();                // no extension left behind
            assertThat(logs.lines).anySatisfy(line -> assertThat(line)
                    .contains("Message m-1 from invoke is not a task invoke"));
            assertThat(logs.lines).anySatisfy(line -> assertThat(line)
                    .contains("Message m-2 (correlation c-2, instance i-1, task /do/0/fast) was not acknowledged"));
            assertThat(String.join("\n", logs.lines)).doesNotContain(SECRET);
        }
    }

    @Test
    void close_stops_receiving_lets_running_tasks_finish_and_hands_back_the_late_one() throws Exception {
        start(binding(2).shutdownGrace(Duration.ofSeconds(30)));
        String finishing = offer("c-1", "hold", null);
        String late = offer("c-2", "hold", null);
        awaitStarted(started, "c-1");
        awaitStarted(started, "c-2");
        scheduler.awaitPending(2);

        CompletableFuture<Void> closing = CompletableFuture.runAsync(binding::close);
        scheduler.awaitPending(3);                                   // the two extensions and the grace
        int receivesAtClose = sqs.receives.size();
        latch(release, "c-1").countDown();
        awaitCondition(() -> sqs.deleted.contains(finishing), "c-1 finishes within the grace");
        scheduler.advance(Duration.ofSeconds(10));                   // still extended while closing
        scheduler.advance(Duration.ofSeconds(20));                   // the grace is over
        closing.get(10, TimeUnit.SECONDS);

        assertThat(sqs.visibilityOf(late)).containsExactly(30, 30, 30, 0);   // extended through the grace, handed back
        assertThat(sqs.deleted).containsExactly(finishing);          // the late one is not acknowledged
        assertThat(metrics.released).hasValue(1);
        assertThat(sqs.receives.size()).isLessThanOrEqualTo(receivesAtClose + 1);   // at most the receive in progress
        binding = null;
    }

    /** Counts what the binding reports. */
    static final class Counting implements SqsWorkerMetrics {
        final AtomicInteger acknowledged = new AtomicInteger();
        final AtomicInteger notAcknowledged = new AtomicInteger();
        final AtomicInteger extended = new AtomicInteger();
        final AtomicInteger extensionFailed = new AtomicInteger();
        final AtomicInteger receiveFailed = new AtomicInteger();
        final AtomicInteger unreadable = new AtomicInteger();
        final AtomicInteger released = new AtomicInteger();

        @Override
        public void acknowledged() {
            acknowledged.incrementAndGet();
        }

        @Override
        public void notAcknowledged() {
            notAcknowledged.incrementAndGet();
        }

        @Override
        public void extended() {
            extended.incrementAndGet();
        }

        @Override
        public void extensionFailed() {
            extensionFailed.incrementAndGet();
        }

        @Override
        public void receiveFailed() {
            receiveFailed.incrementAndGet();
        }

        @Override
        public void unreadable() {
            unreadable.incrementAndGet();
        }

        @Override
        public void released() {
            released.incrementAndGet();
        }
    }

    /** Every record logged anywhere, rendered with its parameters, MDC and exception. */
    static final class Logs extends Handler implements AutoCloseable {
        final List<String> lines = new CopyOnWriteArrayList<>();
        private final org.jboss.logmanager.Logger root = LogContext.getLogContext().getLogger("");
        private final Level previous = root.getLevel();

        Logs() {
            setLevel(Level.ALL);
            root.setLevel(Level.ALL);
            root.addHandler(this);
        }

        @Override
        public void publish(LogRecord record) {
            String message = record instanceof ExtLogRecord ext ? ext.getFormattedMessage() : record.getMessage();
            StringBuilder line = new StringBuilder(record.getLevel() + " " + record.getLoggerName() + " " + message);
            if (record.getParameters() != null) {
                line.append(" params=").append(java.util.Arrays.asList(record.getParameters()));
            }
            for (Throwable t = record.getThrown(); t != null; t = t.getCause()) {
                line.append(" thrown=").append(t);
            }
            lines.add(line.toString());
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
            root.removeHandler(this);
            root.setLevel(previous);
        }
    }
}
