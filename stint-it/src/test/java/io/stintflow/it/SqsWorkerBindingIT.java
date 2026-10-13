package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import io.stintflow.aws.worker.ManualScheduler;
import io.stintflow.aws.worker.SqsWorkerBinding;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.CeWire;
import io.stintflow.wire.DefaultCloudEventCodec;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerRuntime;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

/**
 * SDD 3a.7, CA1–CA5 over a real SQS (floci, dummy credentials {@code test}/{@code test}): the binding runs tasks side
 * by side, a crashed worker's message comes back and is processed once, a long task is not handed to another consumer
 * while its visibility is extended, the pool is never over-filled, and a graceful close finishes, then hands back.
 * Barriers and long polls — never a sleep; the waits on SQS's own clock are long polls of the second consumer.
 */
class SqsWorkerBindingIT {

    static final Instant T0 = Instant.parse("2026-10-12T12:00:00Z");
    private static GenericContainer<?> floci;
    private static SqsClient sqs;
    private static final AtomicInteger QUEUES = new AtomicInteger();

    private final DefaultCloudEventCodec codec = new DefaultCloudEventCodec();
    private final Map<String, CountDownLatch> started = new ConcurrentHashMap<>();
    private final Map<String, CountDownLatch> release = new ConcurrentHashMap<>();
    private final AtomicInteger running = new AtomicInteger();
    private final AtomicInteger maxRunning = new AtomicInteger();
    /** The countable effect: applied once per correlationId, whatever the redeliveries. */
    private final Set<String> effects = ConcurrentHashMap.newKeySet();
    private final AtomicInteger executions = new AtomicInteger();
    private final List<AutoCloseable> toClose = new CopyOnWriteArrayList<>();

    private final TaskHandlerRegistry handlers = new TaskHandlerRegistry()
            .register("hold", ctx -> {
                executions.incrementAndGet();
                maxRunning.accumulateAndGet(running.incrementAndGet(), Math::max);
                latch(started, ctx.correlationId()).countDown();
                try {
                    await(latch(release, ctx.correlationId()));
                    effects.add(ctx.correlationId());
                } finally {
                    running.decrementAndGet();
                }
                return CompletableFuture.completedFuture(Json.obj().put("done", ctx.correlationId()));
            })
            .register("fast", ctx -> {
                executions.incrementAndGet();
                effects.add(ctx.correlationId());
                return CompletableFuture.completedFuture(Json.obj().put("done", ctx.correlationId()));
            });

    @BeforeAll
    static void startFloci() {
        floci = new GenericContainer<>(DockerImageName.parse("floci/floci:latest")).withExposedPorts(4566);
        floci.start();
        sqs = client();
    }

    static SqsClient client() {
        URI endpoint = URI.create("http://" + floci.getHost() + ":" + floci.getMappedPort(4566));
        return SqsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .httpClient(UrlConnectionHttpClient.create()).build();
    }

    @AfterAll
    static void stopFloci() {
        sqs.close();
        floci.stop();
    }

    @AfterEach
    void cleanUp() throws Exception {
        release.values().forEach(CountDownLatch::countDown);
        for (AutoCloseable c : toClose) {
            c.close();
        }
    }

    static CountDownLatch latch(Map<String, CountDownLatch> latches, String correlationId) {
        return latches.computeIfAbsent(correlationId, k -> new CountDownLatch(1));
    }

    static void await(CountDownLatch latch) {
        try {
            if (!latch.await(60, TimeUnit.SECONDS)) {
                throw new IllegalStateException("the test never released the barrier");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    static String queue(int visibilitySeconds) {
        return sqs.createQueue(q -> q.queueName("binding-" + QUEUES.incrementAndGet())
                .attributes(Map.of(QueueAttributeName.VISIBILITY_TIMEOUT, String.valueOf(visibilitySeconds))))
                .queueUrl();
    }

    void send(String queueUrl, String correlationId, String routingKey) {
        TaskInvocation inv = new TaskInvocation("i-1", "/do/0/" + routingKey, correlationId,
                new WorkflowRef("it", "binding", "1.0.0"), routingKey, Json.obj(), 1, Lineage.NONE, null);
        sqs.sendMessage(r -> r.queueUrl(queueUrl)
                .messageBody(new String(CeWire.toJson(codec.toEvent(inv)), StandardCharsets.UTF_8)));
    }

    /** Correlation ids of the results in {@code resultQueue}, received (and deleted) until {@code count} came. */
    static List<String> results(String resultQueue, int count) {
        DefaultCloudEventCodec codec = new DefaultCloudEventCodec();
        List<String> ids = new CopyOnWriteArrayList<>();
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (ids.size() < count && System.nanoTime() < until) {
            for (Message m : sqs.receiveMessage(r -> r.queueUrl(resultQueue).maxNumberOfMessages(10).waitTimeSeconds(2))
                    .messages()) {
                ids.add(codec.toResult(CeWire.fromJson(m.body().getBytes(StandardCharsets.UTF_8))).correlationId());
                sqs.deleteMessage(r -> r.queueUrl(resultQueue).receiptHandle(m.receiptHandle()));
            }
        }
        return ids;
    }

    /** Messages a second consumer gets from {@code queue} within one long poll of {@code seconds}. */
    static List<Message> secondConsumer(String queue, int seconds) {
        return sqs.receiveMessage(r -> r.queueUrl(queue).maxNumberOfMessages(10).waitTimeSeconds(seconds)
                .messageSystemAttributeNames(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT)).messages();
    }

    static int messagesIn(String queue) {
        Map<QueueAttributeName, String> attributes = sqs.getQueueAttributes(r -> r.queueUrl(queue).attributeNames(
                QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE)).attributes();
        return Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES))
                + Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE));
    }

    private SqsWorkerBinding.Builder binding(SqsClient client, String invoke, String result, int concurrency) {
        return SqsWorkerBinding.builder().sqs(client).invokeQueueUrl(invoke).resultQueueUrl(result)
                .runtime(new WorkerRuntime(handlers, null)).concurrency(concurrency).waitTime(Duration.ofSeconds(1));
    }

    private SqsWorkerBinding started(SqsWorkerBinding.Builder builder) {
        SqsWorkerBinding binding = builder.build().start();
        toClose.add(binding);
        return binding;
    }

    @Test
    void ca1_a_task_held_at_a_barrier_does_not_keep_the_others_from_being_acknowledged() throws Exception {
        String invoke = queue(30);
        String result = queue(30);
        started(binding(sqs, invoke, result, 4));
        send(invoke, "c-held", "hold");
        send(invoke, "c-1", "fast");
        send(invoke, "c-2", "fast");
        send(invoke, "c-3", "fast");
        assertThat(latch(started, "c-held").await(30, TimeUnit.SECONDS)).isTrue();

        assertThat(results(result, 3)).containsExactlyInAnyOrder("c-1", "c-2", "c-3");
        assertThat(messagesIn(invoke)).isEqualTo(1);                 // only the held one is left, in flight

        latch(release, "c-held").countDown();
        assertThat(results(result, 1)).containsExactly("c-held");
        assertThat(messagesIn(invoke)).isZero();
    }

    @Test
    void ca2_a_crashed_worker_s_message_comes_back_and_its_effect_happens_once() throws Exception {
        String invoke = queue(2);
        String result = queue(30);
        DyingSqs dying = new DyingSqs(client());
        started(binding(dying, invoke, result, 1).visibilityTimeout(Duration.ofSeconds(2))
                .extendEvery(Duration.ofSeconds(1)));
        send(invoke, "c-1", "hold");
        assertThat(latch(started, "c-1").await(30, TimeUnit.SECONDS)).isTrue();

        dying.dead = true;                                           // the worker crashes mid-task: no extension, no ack
        List<Message> again = secondConsumer(invoke, 10);            // its message comes back after its visibility
        assertThat(again).singleElement().satisfies(m -> assertThat(
                m.attributes().get(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT)).isEqualTo("2"));
        sqs.changeMessageVisibility(r -> r.queueUrl(invoke).receiptHandle(again.getFirst().receiptHandle())
                .visibilityTimeout(0));                              // hand it on to the new worker

        TaskHandlerRegistry healthy = new TaskHandlerRegistry().register("hold", ctx -> {
            executions.incrementAndGet();
            effects.add(ctx.correlationId());                        // idempotent per correlationId
            return CompletableFuture.completedFuture(Json.obj().put("done", ctx.correlationId()));
        });
        SqsWorkerBinding restarted = SqsWorkerBinding.builder().sqs(sqs).invokeQueueUrl(invoke).resultQueueUrl(result)
                .runtime(new WorkerRuntime(healthy, null)).concurrency(1).waitTime(Duration.ofSeconds(1)).build()
                .start();
        toClose.add(restarted);
        assertThat(results(result, 1)).containsExactly("c-1");
        latch(release, "c-1").countDown();                          // the crashed worker's thread finishes, uselessly

        assertThat(effects).containsExactly("c-1");                  // one effect, though the task ran twice
        assertThat(executions).hasValue(2);
        assertThat(messagesIn(invoke)).isZero();
    }

    @Test
    void ca3_a_task_longer_than_the_visibility_is_not_handed_to_another_consumer_and_extension_ends_with_it()
            throws Exception {
        String invoke = queue(2);
        String result = queue(30);
        DyingSqs counted = new DyingSqs(client());
        started(binding(counted, invoke, result, 1).visibilityTimeout(Duration.ofSeconds(2))
                .extendEvery(Duration.ofSeconds(1)));
        send(invoke, "c-1", "hold");
        assertThat(latch(started, "c-1").await(30, TimeUnit.SECONDS)).isTrue();

        assertThat(secondConsumer(invoke, 7)).isEmpty();             // 7 s, over three visibilities: never handed out
        assertThat(counted.visibilityChanges.get()).isGreaterThanOrEqualTo(3);

        latch(release, "c-1").countDown();
        assertThat(results(result, 1)).containsExactly("c-1");
        int atAck = counted.visibilityChanges.get();
        assertThat(secondConsumer(invoke, 3)).isEmpty();             // 3 s more: no extension after the delete
        assertThat(counted.visibilityChanges.get()).isEqualTo(atAck);
        assertThat(messagesIn(invoke)).isZero();
    }

    @Test
    void ca4_never_more_received_and_unfinished_than_the_concurrency() throws Exception {
        String invoke = queue(30);
        String result = queue(30);
        DyingSqs counted = new DyingSqs(client());
        SqsWorkerBinding binding = started(binding(counted, invoke, result, 2));
        for (int i = 1; i <= 10; i++) {
            send(invoke, "c-" + i, "hold");
        }
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (running.get() < 2 && System.nanoTime() < until) {
            Thread.onSpinWait();
        }
        int receivesWhenFull = counted.receives.size();
        assertThat(secondConsumer(queue(30), 3)).isEmpty();           // 3 s pass on an empty queue
        assertThat(counted.receives).hasSize(receivesWhenFull);       // a full pool doesn't receive
        assertThat(binding.inFlight()).isEqualTo(2);

        for (int i = 1; i <= 10; i++) {
            latch(release, "c-" + i).countDown();
        }
        assertThat(results(result, 10)).hasSize(10);
        assertThat(maxRunning).hasValue(2);
        assertThat(counted.receives).allSatisfy(max -> assertThat(max).isBetween(1, 2));
        assertThat(messagesIn(invoke)).isZero();
    }

    @Test
    void ca5_close_lets_running_tasks_finish_and_hands_back_the_late_one() throws Exception {
        String invoke = queue(30);
        String result = queue(30);
        ManualScheduler grace = new ManualScheduler(T0);
        DyingSqs counted = new DyingSqs(client());
        SqsWorkerBinding binding = binding(counted, invoke, result, 3).scheduler(grace).clock(grace.clock())
                .shutdownGrace(Duration.ofSeconds(30)).build().start();
        for (String id : List.of("c-1", "c-2", "c-late")) {
            send(invoke, id, "hold");
        }
        for (String id : List.of("c-1", "c-2", "c-late")) {
            assertThat(latch(started, id).await(30, TimeUnit.SECONDS)).isTrue();
        }

        CompletableFuture<Void> closing = CompletableFuture.runAsync(binding::close);
        grace.awaitPending(4);                                        // three extensions and the grace
        int receivesAtClose = counted.receives.size();
        latch(release, "c-1").countDown();
        latch(release, "c-2").countDown();
        assertThat(results(result, 2)).containsExactlyInAnyOrder("c-1", "c-2");
        grace.advance(Duration.ofSeconds(30));                        // the grace is over
        closing.get(30, TimeUnit.SECONDS);

        List<Message> handedBack = secondConsumer(invoke, 5);         // visibility 0: at once, to another consumer
        assertThat(handedBack).singleElement().satisfies(m -> assertThat(
                codec.toInvocation(CeWire.fromJson(m.body().getBytes(StandardCharsets.UTF_8))).correlationId())
                .isEqualTo("c-late"));
        assertThat(counted.receives.size()).isLessThanOrEqualTo(receivesAtClose + 1);   // at most the one in progress
        assertThat(effects).containsExactlyInAnyOrder("c-1", "c-2");  // the late one never acknowledged nor applied
    }

    /** An SQS client that counts receives and visibility changes, and "dies" — every call fails — on demand. */
    static final class DyingSqs implements SqsClient {
        private final SqsClient delegate;
        volatile boolean dead;
        final List<Integer> receives = new CopyOnWriteArrayList<>();
        final AtomicInteger visibilityChanges = new AtomicInteger();

        DyingSqs(SqsClient delegate) {
            this.delegate = delegate;
        }

        private void check() {
            if (dead) {
                throw SdkClientException.create("synthetic crash: the worker is gone");
            }
        }

        @Override
        public ReceiveMessageResponse receiveMessage(ReceiveMessageRequest request) {
            check();
            receives.add(request.maxNumberOfMessages());
            return delegate.receiveMessage(request);
        }

        @Override
        public ChangeMessageVisibilityResponse changeMessageVisibility(ChangeMessageVisibilityRequest request) {
            check();
            visibilityChanges.incrementAndGet();
            return delegate.changeMessageVisibility(request);
        }

        @Override
        public DeleteMessageResponse deleteMessage(DeleteMessageRequest request) {
            check();
            return delegate.deleteMessage(request);
        }

        @Override
        public SendMessageResponse sendMessage(SendMessageRequest request) {
            check();
            return delegate.sendMessage(request);
        }

        @Override
        public String serviceName() {
            return delegate.serviceName();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
