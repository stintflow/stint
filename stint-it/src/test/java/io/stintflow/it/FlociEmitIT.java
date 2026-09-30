package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.stintflow.aws.DynamoDbStateStore;
import io.stintflow.aws.EventBridgeEventPublisher;
import io.stintflow.aws.EventBridgeTaskTransport;
import io.stintflow.aws.S3BlobStore;
import io.stintflow.aws.S3FactBlobStore;
import io.stintflow.aws.SnsEventPublisher;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.core.model.DataFlow;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.spi.AdapterCapabilities;
import io.stintflow.spi.EventPublisher;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.OutboxEntry;
import io.stintflow.spi.SaveOutcome;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.Wait;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.spi.wire.StintEvents;
import io.stintflow.wire.CeWire;
import io.stintflow.wire.ClaimCheck;
import io.stintflow.wire.DefaultCloudEventCodec;
import io.stintflow.wire.Json;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.Target;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/**
 * SDD 2.2 on real floci infrastructure:
 * <ul>
 *   <li>CA3 — the engine's task traffic goes over EventBridge on the {@code default} bus while facts go
 *       to the {@code stint-domain} bus; a catch-all rule on the domain bus sees only the facts;</li>
 *   <li>CA2 — the first publish fails, the fact stays in the DynamoDB outbox and the sweep (queried
 *       through the {@code pending-by-age} GSI, clock advanced past the grace period) publishes it with
 *       the same id; the outbox ends empty;</li>
 *   <li>CA4 — a fact larger than EventBridge allows goes out with a {@code dataref} into the dedicated
 *       facts bucket, readable by a plain S3 client after the instance finished;</li>
 *   <li>the SNS publisher delivers a fact to a raw-delivery SQS subscription.</li>
 * </ul>
 * No sleeps: every wait is on an observable effect (a completion, a counted removal, a long-poll receive).
 */
class FlociEmitIT {

    private static final WorkflowRef REF = new WorkflowRef("billing", "invoice-order", "1.0.0");
    private static final String INVOICED = "io.acme.order.invoiced.v1";
    private static final String REPORTED = "io.acme.order.report.v1";
    private static final String SOURCE = "https://acme.example/billing";

    @Test
    void facts_go_to_the_domain_bus_survive_a_failed_publish_and_large_data_becomes_a_dataref() throws Exception {
        try (GenericContainer<?> floci = new GenericContainer<>(DockerImageName.parse("floci/floci:latest"))
                .withExposedPorts(4566)) {
            floci.start();
            URI endpoint = URI.create("http://" + floci.getHost() + ":" + floci.getMappedPort(4566));
            var creds = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));

            try (SqsClient sqs = SqsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                    .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build();
                 EventBridgeClient events = EventBridgeClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                         .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build();
                 SnsClient sns = SnsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                         .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build();
                 S3Client s3 = S3Client.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                         .forcePathStyle(true).credentialsProvider(creds)
                         .httpClient(UrlConnectionHttpClient.create()).build();
                 DynamoDbClient ddb = DynamoDbClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                         .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build()) {

                // Internal channel: task invokes on the default bus -> invokeQueue; results back on resultQueue.
                String resultQueue = sqs.createQueue(q -> q.queueName("stint-results")).queueUrl();
                String invokeQueue = sqs.createQueue(q -> q.queueName("stint-invokes")).queueUrl();
                events.putRule(r -> r.name("stint-invokes").eventPattern("{\"source\":[\"io.stintflow\"]}"));
                events.putTargets(t -> t.rule("stint-invokes").targets(Target.builder()
                        .id("invokes").arn(queueArn(sqs, invokeQueue)).inputPath("$.detail").build()));

                // Domain channel: its own bus, and a catch-all rule — whatever reaches this bus lands here.
                String domainQueue = sqs.createQueue(q -> q.queueName("stint-domain-facts")).queueUrl();
                events.createEventBus(b -> b.name("stint-domain"));
                events.putRule(r -> r.name("all-facts").eventBusName("stint-domain")
                        .eventPattern("{\"source\":[{\"prefix\":\"\"}]}"));
                events.putTargets(t -> t.rule("all-facts").eventBusName("stint-domain").targets(Target.builder()
                        .id("facts").arn(queueArn(sqs, domainQueue)).inputPath("$.detail").build()));

                s3.createBucket(b -> b.bucket("stint-blobs"));
                s3.createBucket(b -> b.bucket("stint-facts"));
                DynamoDbTestTables.create(ddb, "stint-instances", "stint-waits");
                DynamoDbTestTables.createOutbox(ddb, "stint-outbox");

                MutableClock clock = new MutableClock();
                CountingRemovals state = new CountingRemovals(
                        new DynamoDbStateStore(ddb, "stint-instances", "stint-waits", "stint-outbox", 4));
                FailFirstPublish publisher = new FailFirstPublish(new EventBridgeEventPublisher(events, "stint-domain", "default"));
                WorkflowRegistry registry = new WorkflowRegistry();
                registry.register(WorkflowBuilder.create()
                        .callRemote("price", "route-price", DataFlow.NONE)
                        .emit("announce", INVOICED, Expr.jq("{source: \"" + SOURCE + "\", type: \"" + INVOICED
                                + "\", data: {orderId: $context.orderId}}"), DataFlow.NONE)
                        .emit("report", REPORTED, Expr.jq("{source: \"" + SOURCE + "\", type: \"" + REPORTED
                                + "\", data: {orderId: $context.orderId, lines: [range(0; 12000) | {n: ., sku: \"SKU-0000000000\"}]}}"),
                                DataFlow.NONE)
                        .build(REF));
                WorkflowEngine engine = new WorkflowEngine(registry,
                        new EventBridgeTaskTransport(events, sqs, "default", resultQueue), state,
                        new InMemoryTimerService(clock), new S3BlobStore(s3, "stint-blobs"),
                        new TreeInterpreter(new JqExpressionEvaluator()), clock, publisher,
                        new S3FactBlobStore(s3, "stint-facts"));

                ObjectNode input = Json.obj();
                input.put("orderId", "A-1");
                String instanceId = engine.start(REF, input);

                // The invoke travelled on the internal channel (default bus)...
                Message invokeMessage = awaitMessages(sqs, invokeQueue, 1, Duration.ofSeconds(60)).get(0);
                DefaultCloudEventCodec codec = new DefaultCloudEventCodec();
                TaskInvocation invocation = codec.toInvocation(CeWire.fromJson(invokeMessage.body().getBytes(StandardCharsets.UTF_8)));
                sqs.sendMessage(b -> b.queueUrl(resultQueue).messageBody(new String(
                        CeWire.toJson(codec.toEvent(TaskResult.completed(invocation.correlationId(), Json.obj()), instanceId)),
                        StandardCharsets.UTF_8)));

                engine.awaitCompletion(instanceId, Duration.ofDays(1)).toCompletableFuture().get(60, TimeUnit.SECONDS);
                // The immediate publish ran: "announce" failed (injected), "report" went out and was removed.
                state.removals(1).get(60, TimeUnit.SECONDS);

                // CA2: the failed fact is still pending in the DynamoDB outbox, found through the GSI.
                List<OutboxEntry> pending = state.pendingOutbox(clock.instant().plus(Duration.ofDays(1)), 10)
                        .toCompletableFuture().get(10, TimeUnit.SECONDS);
                assertThat(pending).hasSize(1);
                String announceId = pending.get(0).eventId();
                assertThat(publisher.failedIds).containsExactly(announceId);

                clock.advance(Duration.ofSeconds(31));
                assertThat(engine.outboxRelay().sweep().toCompletableFuture().get(30, TimeUnit.SECONDS)).isEqualTo(1);
                assertThat(state.pendingOutbox(clock.instant().plus(Duration.ofDays(1)), 10)
                        .toCompletableFuture().get(10, TimeUnit.SECONDS)).isEmpty(); // CA6

                // CA3: the domain bus received exactly the two facts — no engine traffic.
                List<CloudEvent> facts = awaitMessages(sqs, domainQueue, 2, Duration.ofSeconds(60)).stream()
                        .map(m -> CeWire.fromJson(m.body().getBytes(StandardCharsets.UTF_8))).toList();
                assertThat(sqs.receiveMessage(r -> r.queueUrl(domainQueue).waitTimeSeconds(3)).messages()).isEmpty();
                assertThat(facts).extracting(CloudEvent::getType).containsExactlyInAnyOrder(INVOICED, REPORTED);
                assertThat(facts).noneSatisfy(e -> assertThat(StintEvents.isReservedType(e.getType())).isTrue());
                CloudEvent announce = facts.stream().filter(e -> e.getType().equals(INVOICED)).findFirst().orElseThrow();
                assertThat(announce.getId()).isEqualTo(announceId); // the same id as the failed attempt

                // SDD 2.5: the lineage survives EventBridge (default and domain buses), SQS and DynamoDB — the invoke,
                // both facts and the persisted snapshot share one chain; the facts' cause is the result that
                // resumed the instance (= the invoke's correlation id).
                String chainId = invocation.lineage().chainId();
                assertThat(chainId).isNotBlank();
                assertThat(facts).allSatisfy(e -> {
                    assertThat(e.getExtension(StintEvents.EXT_CHAIN_ID)).isEqualTo(chainId);
                    assertThat(e.getExtension(StintEvents.EXT_CAUSATION_ID)).isEqualTo(invocation.correlationId());
                });
                assertThat(state.load(instanceId).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow().chainId())
                        .isEqualTo(chainId);

                // CA4: the large fact carries a dataref into the facts bucket, readable after completion.
                CloudEvent report = facts.stream().filter(e -> e.getType().equals(REPORTED)).findFirst().orElseThrow();
                assertThat(report.getData()).isNull();
                assertThat(report.getExtension(ClaimCheck.POINTER_FIELD)).isNull();
                URI dataref = URI.create((String) report.getExtension("dataref"));
                assertThat(dataref.getHost()).isEqualTo("stint-facts");
                assertThat(dataref.getPath()).isEqualTo("/facts/billing/invoice-order/" + report.getId());
                JsonNode reportData = Json.read(s3.getObjectAsBytes(b -> b.bucket("stint-facts")
                        .key(dataref.getPath().substring(1))).asByteArray());
                assertThat(reportData.get("lines")).hasSize(12000);

                // SNS domain publisher: raw delivery to an SQS subscription, filterable by 'type'.
                String topicArn = sns.createTopic(t -> t.name("stint-domain-facts")).topicArn();
                String snsQueue = sqs.createQueue(q -> q.queueName("stint-domain-sns")).queueUrl();
                String subscription = sns.subscribe(s -> s.topicArn(topicArn).protocol("sqs")
                        .endpoint(queueArn(sqs, snsQueue)).returnSubscriptionArn(true)).subscriptionArn();
                sns.setSubscriptionAttributes(a -> a.subscriptionArn(subscription)
                        .attributeName("RawMessageDelivery").attributeValue("true"));
                CloudEvent snsFact = CloudEventBuilder.v1().withId("fact-sns-1").withSource(URI.create(SOURCE))
                        .withType(INVOICED).build();
                new SnsEventPublisher(sns, topicArn, "").publish(snsFact).toCompletableFuture().get(30, TimeUnit.SECONDS);
                CloudEvent viaSns = CeWire.fromJson(awaitMessages(sqs, snsQueue, 1, Duration.ofSeconds(60)).get(0)
                        .body().getBytes(StandardCharsets.UTF_8));
                assertThat(viaSns.getId()).isEqualTo("fact-sns-1");

                // Channel misconfiguration fails fast.
                assertThatThrownBy(() -> new EventBridgeEventPublisher(events, "default", "default"))
                        .isInstanceOf(IllegalStateException.class);
            }
        }
    }

    private static String queueArn(SqsClient sqs, String queueUrl) {
        return sqs.getQueueAttributes(r -> r.queueUrl(queueUrl).attributeNames(QueueAttributeName.QUEUE_ARN))
                .attributes().get(QueueAttributeName.QUEUE_ARN);
    }

    /** Long-poll receives (no sleep) until {@code count} messages arrived; deletes what it reads. */
    private static List<Message> awaitMessages(SqsClient sqs, String queueUrl, int count, Duration within) {
        List<Message> received = new ArrayList<>();
        long deadline = System.nanoTime() + within.toNanos();
        while (received.size() < count && System.nanoTime() < deadline) {
            for (Message m : sqs.receiveMessage(r -> r.queueUrl(queueUrl).maxNumberOfMessages(10).waitTimeSeconds(5))
                    .messages()) {
                received.add(m);
                sqs.deleteMessage(d -> d.queueUrl(queueUrl).receiptHandle(m.receiptHandle()));
            }
        }
        assertThat(received).as("messages on " + queueUrl).hasSizeGreaterThanOrEqualTo(count);
        return received;
    }

    /** Refuses the first publish, as if the broker were briefly unavailable. */
    private static final class FailFirstPublish implements EventPublisher {
        private final EventPublisher delegate;
        private final AtomicBoolean failed = new AtomicBoolean();
        final List<String> failedIds = new CopyOnWriteArrayList<>();

        FailFirstPublish(EventPublisher delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletionStage<Void> publish(CloudEvent event) {
            if (failed.compareAndSet(false, true)) {
                failedIds.add(event.getId());
                return CompletableFuture.failedFuture(new IllegalStateException("simulated broker outage"));
            }
            return delegate.publish(event);
        }

        @Override
        public AdapterCapabilities capabilities() {
            return delegate.capabilities();
        }
    }

    /** Lets the test wait for a number of outbox removals instead of racing the immediate publish. */
    private static final class CountingRemovals implements StateStore {
        private final StateStore delegate;
        private final AtomicInteger removed = new AtomicInteger();
        private final AtomicReference<CompletableFuture<Void>> goal = new AtomicReference<>();
        private volatile int goalCount = Integer.MAX_VALUE;

        CountingRemovals(StateStore delegate) {
            this.delegate = delegate;
        }

        CompletableFuture<Void> removals(int count) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            goal.set(future);
            goalCount = count;
            if (removed.get() >= count) {
                future.complete(null);
            }
            return future;
        }

        @Override
        public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion,
                List<Wait> addWaits, List<String> consumeWaitKeys, List<OutboxEntry> addOutbox) {
            return delegate.save(snapshot, expectedVersion, addWaits, consumeWaitKeys, addOutbox);
        }

        @Override
        public CompletionStage<Optional<InstanceSnapshot>> load(String instanceId) {
            return delegate.load(instanceId);
        }

        @Override
        public CompletionStage<Optional<Wait>> findWait(String waitKey) {
            return delegate.findWait(waitKey);
        }

        @Override
        public CompletionStage<Void> delete(String instanceId) {
            return delegate.delete(instanceId);
        }

        @Override
        public CompletionStage<List<OutboxEntry>> pendingOutbox(Instant createdAtOrBefore, int limit) {
            return delegate.pendingOutbox(createdAtOrBefore, limit);
        }

        @Override
        public CompletionStage<Void> removeOutbox(String eventId) {
            return delegate.removeOutbox(eventId).thenRun(() -> {
                if (removed.incrementAndGet() >= goalCount) {
                    CompletableFuture<Void> future = goal.get();
                    if (future != null) {
                        future.complete(null);
                    }
                }
            });
        }
    }

    private static final class MutableClock implements InstantSource {
        private final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-01-01T00:00:00Z"));

        @Override
        public Instant instant() {
            return now.get();
        }

        void advance(Duration duration) {
            now.updateAndGet(i -> i.plus(duration));
        }
    }
}
