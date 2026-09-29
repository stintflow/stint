package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.stintflow.aws.DynamoDbStateStore;
import io.stintflow.aws.SqsDomainEventSource;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.trigger.DomainEventRouter;
import io.stintflow.core.trigger.EventFilter;
import io.stintflow.core.trigger.StartReaction;
import io.stintflow.core.trigger.TriggerBinding;
import io.stintflow.core.trigger.TriggerBindings;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryTaskTransport;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.OutboxEntry;
import io.stintflow.spi.SaveOutcome;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.Wait;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.CeWire;
import io.stintflow.wire.Json;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.eventbridge.model.Target;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/**
 * SDD 2.1, CA3: real floci — EventBridge {@code PutEvents} → rule → SQS inbound queue (target
 * {@code InputPath: $.detail}) → {@link SqsDomainEventSource} → {@link DomainEventRouter} → an instance
 * created in DynamoDB. A redelivered event (again via EventBridge, and raw on the queue) creates no
 * second instance; a body that isn't a CloudEvent reaches the dead-letter queue via the redrive
 * policy (sec. 8d). No sleeps: we wait on observable effects (instance completion, conflict count,
 * a long-poll receive on the DLQ).
 */
class FlociDomainEventTriggerIT {

    private static final WorkflowRef REF = new WorkflowRef("billing", "invoice-order", "1.0.0");
    private static final String ORDER_PLACED = "io.acme.order.placed.v1";
    private static final String EB_SOURCE = "com.acme.orders";

    @Test
    void ca3_eventbridge_rule_to_sqs_starts_exactly_one_instance_and_dead_letters_garbage() throws Exception {
        try (GenericContainer<?> floci = new GenericContainer<>(DockerImageName.parse("floci/floci:latest"))
                .withExposedPorts(4566)) {
            floci.start();
            URI endpoint = URI.create("http://" + floci.getHost() + ":" + floci.getMappedPort(4566));
            var creds = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));

            try (SqsClient sqs = SqsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                    .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build();
                 EventBridgeClient events = EventBridgeClient.builder().endpointOverride(endpoint)
                         .region(Region.US_EAST_1).credentialsProvider(creds)
                         .httpClient(UrlConnectionHttpClient.create()).build();
                 DynamoDbClient ddb = DynamoDbClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                         .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build()) {

                // Inbound queue + DLQ (redrive after 2 receives, short visibility so it's observed quickly).
                String dlqUrl = sqs.createQueue(q -> q.queueName("stint-domain-events-dlq")).queueUrl();
                String dlqArn = queueArn(sqs, dlqUrl);
                String inboundUrl = sqs.createQueue(q -> q.queueName("stint-domain-events").attributes(Map.of(
                        QueueAttributeName.VISIBILITY_TIMEOUT, "2",
                        QueueAttributeName.REDRIVE_POLICY,
                        "{\"deadLetterTargetArn\":\"" + dlqArn + "\",\"maxReceiveCount\":\"2\"}"))).queueUrl();

                // EventBridge rule -> SQS target delivering only the CloudEvent ($.detail), not the envelope.
                events.putRule(r -> r.name("stint-order-placed")
                        .eventPattern("{\"source\":[\"" + EB_SOURCE + "\"],\"detail-type\":[\"" + ORDER_PLACED + "\"]}"));
                events.putTargets(t -> t.rule("stint-order-placed").targets(Target.builder()
                        .id("stint-inbound").arn(queueArn(sqs, inboundUrl)).inputPath("$.detail").build()));

                DynamoDbTestTables.create(ddb, "stint-instances", "stint-waits");
                CountingStore state = new CountingStore(new DynamoDbStateStore(ddb, "stint-instances", "stint-waits"));

                WorkflowRegistry registry = new WorkflowRegistry();
                registry.register(WorkflowBuilder.create()
                        .set("invoice", Expr.jq("{orderId: .[0].data.orderId, eventId: $workflow.input[0].id}"),
                                new DataFlow(null, null, Expr.jq(".")))
                        .build(REF));
                InMemoryTaskTransport transport = new InMemoryTaskTransport();
                WorkflowEngine engine = new WorkflowEngine(registry, transport, state, new InMemoryTimerService(),
                        new FilesystemBlobStore(Files.createTempDirectory("stint-ca3-trigger-blobs")));
                TriggerBindings bindings = new TriggerBindings(registry);
                bindings.bind(new TriggerBinding("invoice-on-order", EventFilter.ofType(ORDER_PLACED), REF));
                new DomainEventRouter(List.of(new StartReaction(bindings, engine)))
                        .subscribeTo(new SqsDomainEventSource(sqs, inboundUrl));

                // 1) CA1/CA3: an event published on EventBridge starts the bound definition.
                CloudEvent event = orderPlaced("evt-it-1", "A-1");
                putOnEventBridge(events, event);
                String instanceId = WorkflowEngine.triggeredInstanceId(REF, event);
                JsonNode result = engine.awaitCompletion(instanceId, Duration.ofSeconds(30))
                        .toCompletableFuture().get(40, TimeUnit.SECONDS);
                assertThat(result.get("orderId").asText()).isEqualTo("A-1");
                assertThat(result.get("eventId").asText()).isEqualTo("evt-it-1");

                // 2) CA2 on real infra: the same event again via EventBridge and raw on the queue.
                putOnEventBridge(events, event);
                sqs.sendMessage(b -> b.queueUrl(inboundUrl)
                        .messageBody(new String(CeWire.toJson(event), StandardCharsets.UTF_8)));
                state.conflictsReached(2).get(40, TimeUnit.SECONDS);

                assertThat(state.createdOk.get()).isEqualTo(1);
                InstanceSnapshot snap = state.load(instanceId).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
                assertThat(snap.status()).isEqualTo(InstanceStatus.COMPLETED);
                assertThat(snap.version()).isEqualTo(1);
                assertThat(snap.input().get(0).get("id").asText()).isEqualTo("evt-it-1"); // persisted in Dynamo
                assertThat(snap.startedAt()).isNotNull();

                // 3) sec. 8d: a body that isn't a CloudEvent is never acked and ends up in the DLQ.
                sqs.sendMessage(b -> b.queueUrl(inboundUrl).messageBody("this is not a cloud event"));
                assertThat(awaitMessage(sqs, dlqUrl, Duration.ofSeconds(60)).body()).isEqualTo("this is not a cloud event");
            }
        }
    }

    private static CloudEvent orderPlaced(String id, String orderId) {
        ObjectNode data = Json.obj();
        data.put("orderId", orderId);
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("https://acme.example/orders"))
                .withType(ORDER_PLACED)
                .withDataContentType("application/json")
                .withData(Json.bytes(data))
                .build();
    }

    private static void putOnEventBridge(EventBridgeClient events, CloudEvent event) {
        var response = events.putEvents(r -> r.entries(PutEventsRequestEntry.builder()
                .source(EB_SOURCE)
                .detailType(event.getType())
                .detail(new String(CeWire.toJson(event), StandardCharsets.UTF_8))
                .build()));
        assertThat(response.failedEntryCount()).isZero();
    }

    private static String queueArn(SqsClient sqs, String queueUrl) {
        return sqs.getQueueAttributes(r -> r.queueUrl(queueUrl).attributeNames(QueueAttributeName.QUEUE_ARN))
                .attributes().get(QueueAttributeName.QUEUE_ARN);
    }

    /** Long-poll receives (no sleep) until a message shows up or the deadline passes. */
    private static Message awaitMessage(SqsClient sqs, String queueUrl, Duration within) {
        long deadline = System.nanoTime() + within.toNanos();
        while (System.nanoTime() < deadline) {
            List<Message> messages = sqs.receiveMessage(r -> r.queueUrl(queueUrl).waitTimeSeconds(5)).messages();
            if (!messages.isEmpty()) {
                return messages.get(0);
            }
        }
        throw new AssertionError("No message on " + queueUrl + " within " + within);
    }

    /** Counts create saves ({@code expectedVersion == 0}) by outcome, to prove "one instance" on real Dynamo. */
    private static final class CountingStore implements StateStore {
        private final StateStore delegate;
        final AtomicInteger createdOk = new AtomicInteger();
        private final AtomicInteger conflicts = new AtomicInteger();
        private volatile CompletableFuture<Void> conflictTarget = new CompletableFuture<>();
        private volatile int conflictGoal = Integer.MAX_VALUE;

        CountingStore(StateStore delegate) {
            this.delegate = delegate;
        }

        CompletableFuture<Void> conflictsReached(int goal) {
            conflictGoal = goal;
            if (conflicts.get() >= goal) {
                conflictTarget.complete(null);
            }
            return conflictTarget;
        }

        @Override
        public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion,
                List<Wait> addWaits, List<String> consumeWaitKeys, List<OutboxEntry> addOutbox) {
            return delegate.save(snapshot, expectedVersion, addWaits, consumeWaitKeys, addOutbox).thenApply(outcome -> {
                if (expectedVersion == 0) {
                    if (outcome == SaveOutcome.OK) {
                        createdOk.incrementAndGet();
                    } else if (conflicts.incrementAndGet() >= conflictGoal) {
                        conflictTarget.complete(null);
                    }
                }
                return outcome;
            });
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
            return delegate.removeOutbox(eventId);
        }
    }
}
