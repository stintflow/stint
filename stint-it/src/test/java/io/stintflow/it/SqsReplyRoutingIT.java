package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import io.cloudevents.core.builder.CloudEventBuilder;
import io.stintflow.aws.RoutingSqsTaskTransport;
import io.stintflow.aws.worker.SqsWorkerBinding;
import io.stintflow.aws.worker.SqsWorkerMetrics;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.spi.wire.StintEvents;
import io.stintflow.wire.CeWire;
import io.stintflow.wire.DefaultCloudEventCodec;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerRuntime;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/**
 * SDD 3a.9, CA2, CA3 and CA8 over a real SQS (floci): two engines share <em>one</em> invoke queue and one worker, and each
 * gets only its own results in its own result queue; an invoke naming a destination outside the worker's list is not
 * run, not acknowledged and ends in the queue's dead-letter queue. Barriers and long polls — never a sleep.
 */
class SqsReplyRoutingIT {

    private static GenericContainer<?> floci;
    private static SqsClient sqs;
    private static final AtomicInteger QUEUES = new AtomicInteger();

    private final DefaultCloudEventCodec codec = new DefaultCloudEventCodec();
    private final List<AutoCloseable> toClose = new CopyOnWriteArrayList<>();
    private final AtomicInteger executions = new AtomicInteger();
    private final TaskHandlerRegistry handlers = new TaskHandlerRegistry().register("fast", ctx -> {
        executions.incrementAndGet();
        return CompletableFuture.completedFuture(Json.obj().put("done", ctx.correlationId()));
    });

    @BeforeAll
    static void startFloci() {
        floci = new GenericContainer<>(DockerImageName.parse("floci/floci:latest")).withExposedPorts(4566);
        floci.start();
        URI endpoint = URI.create("http://" + floci.getHost() + ":" + floci.getMappedPort(4566));
        sqs = SqsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
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
        for (AutoCloseable c : toClose) {
            c.close();
        }
    }

    static String queue(String prefix, Map<QueueAttributeName, String> attributes) {
        return sqs.createQueue(q -> q.queueName(prefix + "-" + QUEUES.incrementAndGet()).attributes(attributes))
                .queueUrl();
    }

    private SqsWorkerBinding worker(String invokeQueue, Map<String, String> destinations, SqsWorkerMetrics metrics) {
        SqsWorkerBinding binding = SqsWorkerBinding.builder().sqs(sqs).invokeQueueUrl(invokeQueue)
                .replyDestinations(destinations).runtime(new WorkerRuntime(handlers, null)).concurrency(4)
                .waitTime(Duration.ofSeconds(1)).visibilityTimeout(Duration.ofSeconds(30)).metrics(metrics)
                .build().start();
        toClose.add(binding);
        return binding;
    }

    private static TaskInvocation invocation(String correlationId) {
        return new TaskInvocation("i-" + correlationId, "/do/0/fast", correlationId,
                new WorkflowRef("it", "reply", "1.0.0"), "fast", Json.obj(), 1, Lineage.NONE, null);
    }

    @Test
    void two_engines_share_one_invoke_queue_and_each_gets_only_its_own_results() throws Exception {
        String shared = queue("shared-tasks", Map.of());
        String businessResults = queue("business-results", Map.of());
        String deliveryResults = queue("delivery-results", Map.of());
        worker(shared, Map.of("business-results", businessResults, "delivery-results", deliveryResults),
                new SqsWorkerMetrics() { });

        RoutingSqsTaskTransport business = RoutingSqsTaskTransport.builder().sqs(sqs).resultQueueUrl(businessResults)
                .replyTo("business-results").route("fast", shared).build();
        RoutingSqsTaskTransport delivery = RoutingSqsTaskTransport.builder().sqs(sqs).resultQueueUrl(deliveryResults)
                .replyTo("delivery-results").route("fast", shared).build();
        toClose.add(business::close);
        toClose.add(delivery::close);
        List<String> businessGot = new CopyOnWriteArrayList<>();
        List<String> deliveryGot = new CopyOnWriteArrayList<>();
        business.onResult(r -> {
            businessGot.add(r.correlationId());
            return CompletableFuture.completedFuture(null);
        });
        delivery.onResult(r -> {
            deliveryGot.add(r.correlationId());
            return CompletableFuture.completedFuture(null);
        });

        for (String id : List.of("b-1", "b-2", "b-3")) {
            business.dispatch(invocation(id)).toCompletableFuture().get(30, TimeUnit.SECONDS);
        }
        for (String id : List.of("d-1", "d-2")) {
            delivery.dispatch(invocation(id)).toCompletableFuture().get(30, TimeUnit.SECONDS);
        }
        awaitCondition(() -> businessGot.size() == 3 && deliveryGot.size() == 2, "every result arrives");

        assertThat(businessGot).containsExactlyInAnyOrder("b-1", "b-2", "b-3");
        assertThat(deliveryGot).containsExactlyInAnyOrder("d-1", "d-2");
        assertThat(executions).hasValue(5);
    }

    @Test
    void a_destination_outside_the_list_is_not_run_and_ends_in_the_dead_letter_queue() throws Exception {
        String dlq = queue("tasks-dlq", Map.of());
        String dlqArn = sqs.getQueueAttributes(r -> r.queueUrl(dlq).attributeNames(QueueAttributeName.QUEUE_ARN))
                .attributes().get(QueueAttributeName.QUEUE_ARN);
        String shared = queue("tasks", Map.of(
                QueueAttributeName.VISIBILITY_TIMEOUT, "1",
                QueueAttributeName.REDRIVE_POLICY,
                "{\"deadLetterTargetArn\":\"" + dlqArn + "\",\"maxReceiveCount\":2}"));
        String businessResults = queue("business-results", Map.of());
        String elsewhere = queue("elsewhere-results", Map.of());
        AtomicInteger refused = new AtomicInteger();
        worker(shared, Map.of("business-results", businessResults), new SqsWorkerMetrics() {
            @Override
            public void replyRefused() {
                refused.incrementAndGet();
            }
        });

        // a name that is not listed, and an address that points at a real queue: neither may be answered
        for (String name : List.of("unlisted-results", elsewhere)) {
            var event = CloudEventBuilder.from(codec.toEvent(invocation("c-" + name.hashCode())))
                    .withExtension(StintEvents.EXT_REPLY_TO, name).build();
            sqs.sendMessage(r -> r.queueUrl(shared).messageBody(new String(CeWire.toJson(event), StandardCharsets.UTF_8)));
        }

        List<Message> dead = new CopyOnWriteArrayList<>();
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        while (dead.size() < 2 && System.nanoTime() < until) {
            dead.addAll(sqs.receiveMessage(r -> r.queueUrl(dlq).maxNumberOfMessages(10).waitTimeSeconds(2)).messages());
        }

        assertThat(dead).hasSize(2);
        assertThat(refused.get()).isGreaterThanOrEqualTo(2);
        assertThat(executions).hasValue(0);
        assertThat(sqs.receiveMessage(r -> r.queueUrl(businessResults).waitTimeSeconds(1)).messages()).isEmpty();
        assertThat(sqs.receiveMessage(r -> r.queueUrl(elsewhere).waitTimeSeconds(1)).messages()).isEmpty();
    }

    private static void awaitCondition(java.util.function.BooleanSupplier condition, String what) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > until) {
                throw new AssertionError("timed out waiting until " + what);
            }
            Thread.onSpinWait();
        }
    }
}
