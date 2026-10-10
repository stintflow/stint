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
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.stintflow.aws.DynamoDbStateStore;
import io.stintflow.aws.SqsDomainEventSource;
import io.stintflow.core.EventCorrelation;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.model.ListenNode;
import io.stintflow.core.trigger.DomainEventRouter;
import io.stintflow.core.trigger.ResumeReaction;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.spi.AdapterCapabilities;
import io.stintflow.spi.AdapterCapabilities.DeliveryGuarantee;
import io.stintflow.spi.InboxEntry;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.OutboxEntry;
import io.stintflow.spi.SaveOutcome;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.TaskResultHandler;
import io.stintflow.spi.TaskTransport;
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
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/**
 * SDD 2.3 on real floci: domain events via EventBridge → rule → SQS → {@link SqsDomainEventSource} →
 * {@link ResumeReaction} resume instances whose state lives in DynamoDB.
 * <ol>
 *   <li>{@code all}: the first event is recorded as a partial (the {@code listenState} round-trips through
 *       the instance item), the second completes the listen — output in consumption order;</li>
 *   <li>an early event is kept in the {@code stint-inbox} table and consumed when the instance reaches its
 *       {@code listen}; the save that consumes it deletes the entry in the same transaction.</li>
 * </ol>
 * No sleeps: we wait on observed saves, inbox writes and instance completion.
 */
class FlociListenIT {

    private static final WorkflowRef BOTH = new WorkflowRef("billing", "await-both", "1.0.0");
    private static final WorkflowRef WORK_THEN_LISTEN = new WorkflowRef("billing", "work-then-listen", "1.0.0");
    private static final String REVIEWED = "io.acme.review.completed.v1";
    private static final String APPROVED = "io.acme.billing.approved.v1";
    private static final String EB_SOURCE = "com.acme.review";
    private static final DataFlow EXPORT_OUTPUT = new DataFlow(null, null, Expr.jq("."));

    @Test
    void listen_resumes_from_eventbridge_and_sqs_with_state_in_dynamodb() throws Exception {
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

                String inboundUrl = sqs.createQueue(q -> q.queueName("stint-domain-events")).queueUrl();
                events.putRule(r -> r.name("stint-review")
                        .eventPattern("{\"source\":[\"" + EB_SOURCE + "\"]}"));
                events.putTargets(t -> t.rule("stint-review").targets(Target.builder()
                        .id("stint-inbound").arn(queueArn(sqs, inboundUrl)).inputPath("$.detail").build()));

                DynamoDbTestTables.create(ddb, "stint-instances", "stint-waits");
                DynamoDbTestTables.createInbox(ddb, "stint-inbox");
                ObservingStore state = new ObservingStore(new DynamoDbStateStore(ddb, "stint-instances",
                        "stint-waits", "stint-outbox", 4, "stint-inbox"));

                WorkflowRegistry registry = new WorkflowRegistry(Duration.ofMinutes(15));
                registry.register(WorkflowBuilder.create()
                        .listen("awaitBoth", ListenNode.Strategy.ALL,
                                List.of(byInstance(REVIEWED), byInstance(APPROVED)), null, null, EXPORT_OUTPUT)
                        .build(BOTH));
                registry.register(WorkflowBuilder.create()
                        .callRemote("work", "route-work", DataFlow.NONE)
                        .listen("awaitReview", ListenNode.Strategy.ONE, List.of(byInstance(REVIEWED)), null, null,
                                EXPORT_OUTPUT)
                        .build(WORK_THEN_LISTEN));
                ManualTransport transport = new ManualTransport();
                WorkflowEngine engine = new WorkflowEngine(registry, transport, state, new InMemoryTimerService(),
                        new FilesystemBlobStore(Files.createTempDirectory("stint-sdd23-it-blobs")));
                new DomainEventRouter(List.of(new ResumeReaction(engine)))
                        .subscribeTo(new SqsDomainEventSource(sqs, inboundUrl));

                // 1) all: a partial persisted in DynamoDB, then completion.
                CompletableFuture<Void> suspended = state.okSave(s -> s.definition().equals(BOTH) && s.version() == 1);
                String a = engine.start(BOTH, Json.obj());
                suspended.get(30, TimeUnit.SECONDS);
                CompletableFuture<Void> partial = state.okSave(s -> s.instanceId().equals(a) && s.version() == 2);
                putOnEventBridge(events, event(APPROVED, "apr-1", a, "approved"));
                partial.get(40, TimeUnit.SECONDS);
                InstanceSnapshot waiting = state.load(a).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
                assertThat(waiting.status()).isEqualTo(InstanceStatus.WAITING);
                assertThat(waiting.listenState().get("active").get("events")).hasSize(1); // round-tripped

                putOnEventBridge(events, event(REVIEWED, "rev-1", a, "reviewed"));
                JsonNode output = engine.awaitCompletion(a, Duration.ofSeconds(30)).toCompletableFuture()
                        .get(40, TimeUnit.SECONDS);
                assertThat(output).extracting(e -> e.get("note").asText()).containsExactly("approved", "reviewed");

                // 2) an early event: kept in stint-inbox, consumed when the instance reaches its listen.
                String b = engine.start(WORK_THEN_LISTEN, Json.obj());
                TaskInvocation work = transport.awaitDispatch();
                CompletableFuture<Void> kept = state.inboxPut();
                sqs.sendMessage(m -> m.queueUrl(inboundUrl).messageBody(
                        new String(CeWire.toJson(event(REVIEWED, "rev-early", b, "early")), StandardCharsets.UTF_8)));
                kept.get(40, TimeUnit.SECONDS);
                String key = keyFor(REVIEWED, b);
                assertThat(state.findInbox(key).toCompletableFuture().get(5, TimeUnit.SECONDS)).hasSize(1);

                transport.deliver(TaskResult.completed(work.correlationId(), Json.obj())).get(30, TimeUnit.SECONDS);
                JsonNode early = engine.awaitCompletion(b, Duration.ofSeconds(30)).toCompletableFuture()
                        .get(40, TimeUnit.SECONDS);
                assertThat(early.get(0).get("note").asText()).isEqualTo("early");
                assertThat(state.findInbox(key).toCompletableFuture().get(5, TimeUnit.SECONDS)).isEmpty();
            }
        }
    }

    private static ListenNode.Filter byInstance(String type) {
        return new ListenNode.Filter(type, Map.of(), new TreeMap<>(Map.of("instance",
                new ListenNode.Correlation(Expr.jq(".data.instanceId"), Expr.jq("$workflow.id")))));
    }

    private static String keyFor(String type, String instanceId) {
        return EventCorrelation.waitKey(type, EventCorrelation.shapeOf(byInstance(type)),
                new TreeMap<>(Map.of("instance", Json.MAPPER.getNodeFactory().textNode(instanceId))));
    }

    private static CloudEvent event(String type, String id, String instanceId, String note) {
        ObjectNode data = Json.obj();
        data.put("instanceId", instanceId);
        data.put("note", note);
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("https://acme.example/review"))
                .withType(type).withDataContentType("application/json").withData(Json.bytes(data)).build();
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

    /** Captures dispatches; results are delivered by the test. */
    private static final class ManualTransport implements TaskTransport {
        private final LinkedBlockingQueue<TaskInvocation> dispatched = new LinkedBlockingQueue<>();
        private volatile TaskResultHandler handler;

        @Override
        public CompletionStage<Void> dispatch(TaskInvocation invocation) {
            dispatched.add(invocation);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void onResult(TaskResultHandler handler) {
            this.handler = handler;
        }

        @Override
        public AdapterCapabilities capabilities() {
            return new AdapterCapabilities(DeliveryGuarantee.AT_LEAST_ONCE, true, Long.MAX_VALUE, null, true, true);
        }

        TaskInvocation awaitDispatch() throws InterruptedException {
            TaskInvocation invocation = dispatched.poll(30, TimeUnit.SECONDS);
            assertThat(invocation).isNotNull();
            return invocation;
        }

        CompletableFuture<Void> deliver(TaskResult result) {
            return handler.handle(result).toCompletableFuture();
        }
    }

    /** Lets the test wait for an observed OK save or inbox write instead of sleeping. */
    private static final class ObservingStore implements StateStore {
        private final StateStore delegate;
        private final List<Map.Entry<Predicate<InstanceSnapshot>, CompletableFuture<Void>>> saveWatches =
                new java.util.concurrent.CopyOnWriteArrayList<>();
        private volatile CompletableFuture<Void> inboxPut = new CompletableFuture<>();

        ObservingStore(StateStore delegate) {
            this.delegate = delegate;
        }

        CompletableFuture<Void> okSave(Predicate<InstanceSnapshot> which) {
            CompletableFuture<Void> seen = new CompletableFuture<>();
            saveWatches.add(Map.entry(which, seen));
            return seen;
        }

        CompletableFuture<Void> inboxPut() {
            inboxPut = new CompletableFuture<>();
            return inboxPut;
        }

        @Override
        public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion, List<Wait> addWaits,
                List<String> consumeWaitKeys, List<OutboxEntry> addOutbox) {
            return save(snapshot, expectedVersion, addWaits, consumeWaitKeys, addOutbox, List.of());
        }

        @Override
        public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion, List<Wait> addWaits,
                List<String> consumeWaitKeys, List<OutboxEntry> addOutbox, List<InboxEntry.Key> removeInbox) {
            return delegate.save(snapshot, expectedVersion, addWaits, consumeWaitKeys, addOutbox, removeInbox)
                    .thenApply(outcome -> {
                        if (outcome == SaveOutcome.OK) {
                            saveWatches.stream().filter(w -> w.getKey().test(snapshot))
                                    .forEach(w -> w.getValue().complete(null));
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

        @Override
        public CompletionStage<Void> putInbox(InboxEntry entry) {
            return delegate.putInbox(entry).thenRun(() -> inboxPut.complete(null));
        }

        @Override
        public CompletionStage<List<InboxEntry>> findInbox(String waitKey) {
            return delegate.findInbox(waitKey);
        }

        @Override
        public CompletionStage<Void> removeInbox(InboxEntry.Key key) {
            return delegate.removeInbox(key);
        }
    }
}
