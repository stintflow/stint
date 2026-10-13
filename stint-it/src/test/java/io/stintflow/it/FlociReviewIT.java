package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.JsonNode;

import io.cloudevents.CloudEvent;
import io.stintflow.aws.DynamoDbCorrectionLog;
import io.stintflow.aws.DynamoDbStateStore;
import io.stintflow.aws.EventBridgeSchedulerTimerService;
import io.stintflow.aws.SqsDelayTimerService;
import io.stintflow.core.DelayRoutingTimerService;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.dsl.DslLoader;
import io.stintflow.dsl.LoadOptions;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryAuditLog;
import io.stintflow.inmemory.InMemoryEventPublisher;
import io.stintflow.inmemory.InMemoryTaskTransport;
import io.stintflow.spi.AdapterCapabilities;
import io.stintflow.spi.CorrectionLog.Correction;
import io.stintflow.spi.EventPublisher;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.TimerFireHandler;
import io.stintflow.spi.TimerRequest;
import io.stintflow.spi.TimerService;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.spi.wire.StintEvents;
import io.stintflow.wire.Json;
import io.stintflow.wire.review.CorrectionExport;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.scheduler.SchedulerClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/**
 * SDD 3a.4 on floci (dummy {@code test}/{@code test} credentials): a review's phases on the real timers — the
 * reminder past the short limit, as a one-time EventBridge Scheduler schedule; the rest of the deadline on SQS; the
 * escalation on the Scheduler again — with the instance, its waits and the early-event inbox in DynamoDB (each phase
 * correlates under its own name, so switching phases never touches one wait key twice in a transaction). The next
 * role answers, the flow follows its choice, and the correction lands in {@code stint-corrections}. Waits are
 * barriers on the published events, no sleep.
 */
class FlociReviewIT {

    private static final String ROLE = "arn:aws:iam::000000000000:role/stint-scheduler";
    private static final String PORTAL = "https://portal.sandbox.example";
    private static final WorkflowRef REF = new WorkflowRef("delivery", "dlq", "1.0.0");

    private static final String YAML = SensitivityPolicyTest.header("dlq", """
            routes:
              internal: ['*']
            review:
              sources: [https://portal.sandbox.example]""").replace("namespace: meetings", "namespace: delivery") + """
            do:
              - decide:
                  call: review
                  with:
                    kind: choice
                    role: technician
                    material: ${ .historyRef }
                    options:
                      - resend: { description: "Resend with a note", then: resend }
                      - accept: { description: "Accept as is", then: accept }
                      - cancel: { description: "Cancel", then: end }
                    deadline: { seconds: 25 }
                    remind: { seconds: 15 }
                    escalate: { role: technical-rep, after: { seconds: 90 } }
                  export: { as: '${ $context + {chosen: .value, by: .role} }' }
              - resend:
                  set: '${ {done: "resend"} }'
                  export: { as: '${ $context + . }' }
                  then: end
              - accept:
                  set: '${ {done: "accept"} }'
                  export: { as: '${ $context + . }' }
            """;

    @Test
    void a_review_s_phases_run_on_the_scheduler_and_sqs_and_the_correction_lands_in_dynamodb() throws Exception {
        try (GenericContainer<?> floci = new GenericContainer<>(DockerImageName.parse("floci/floci:latest"))
                .withExposedPorts(4566)) {
            floci.start();
            URI endpoint = URI.create("http://" + floci.getHost() + ":" + floci.getMappedPort(4566));
            var creds = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));

            try (SqsClient sqs = SqsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                    .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build();
                 DynamoDbClient ddb = DynamoDbClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                         .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build();
                 SchedulerClient scheduler = SchedulerClient.builder().endpointOverride(endpoint)
                         .region(Region.US_EAST_1).credentialsProvider(creds)
                         .httpClient(UrlConnectionHttpClient.create()).build()) {

                String timerQueue = sqs.createQueue(q -> q.queueName("stint-timers")).queueUrl();
                DynamoDbTestTables.create(ddb, "stint-instances", "stint-waits");
                DynamoDbTestTables.createOutbox(ddb, "stint-outbox");
                DynamoDbTestTables.createInbox(ddb, "stint-inbox");
                ddb.createTable(t -> t.tableName("stint-corrections")
                        .attributeDefinitions(
                                a -> a.attributeName("definition").attributeType(ScalarAttributeType.S),
                                a -> a.attributeName("entry").attributeType(ScalarAttributeType.S))
                        .keySchema(k -> k.attributeName("definition").keyType(KeyType.HASH),
                                k -> k.attributeName("entry").keyType(KeyType.RANGE))
                        .billingMode("PAY_PER_REQUEST"));

                DelayRoutingTimerService timers = new DelayRoutingTimerService(
                        new LowerMaxDelay(new SqsDelayTimerService(sqs, timerQueue), Duration.ofSeconds(10)),
                        new EventBridgeSchedulerTimerService(scheduler, "stint-timers", ROLE,
                                queueArn(sqs, timerQueue), Duration.ofDays(365)),
                        InstantSource.system());
                WorkflowRegistry registry = new WorkflowRegistry(timers.maxDelay());
                registry.register(new DslLoader().load(new ByteArrayInputStream(YAML.getBytes(StandardCharsets.UTF_8)),
                        "dlq.yaml", LoadOptions.STRICT));
                Barrier published = new Barrier();
                DynamoDbCorrectionLog corrections = new DynamoDbCorrectionLog(ddb, "stint-corrections",
                        DynamoDbCorrectionLog.DEFAULT_TTL, InstantSource.system());
                InMemoryAuditLog audit = new InMemoryAuditLog();
                WorkflowEngine engine = new WorkflowEngine(registry, new InMemoryTaskTransport(),
                        new DynamoDbStateStore(ddb, "stint-instances", "stint-waits", "stint-outbox", 4,
                                "stint-inbox"),
                        timers, new FilesystemBlobStore(Files.createTempDirectory("stint-sdd3a4")),
                        new TreeInterpreter(new JqExpressionEvaluator()), InstantSource.system(), published, null)
                        .auditLog(audit).correctionLog(corrections);

                String id = engine.start(REF, Json.obj().put("historyRef", "s3://stint-blobs/dlq/sdd-7.json"),
                        Sensitivity.INTERNAL);

                CloudEvent request = published.next(Duration.ofSeconds(30));
                assertThat(request.getType()).isEqualTo(StintEvents.TYPE_REVIEW_REQUESTED);
                assertThat(request.getExtension(StintEvents.EXT_ROLE)).isEqualTo("technician");
                // the reminder (15 s) is past the short limit: a one-time schedule
                assertThat(scheduler.listSchedules(r -> r.groupName("stint-timers")).schedules()).isNotEmpty();

                CloudEvent reminder = published.next(Duration.ofMinutes(3));
                assertThat(reminder.getType()).isEqualTo(StintEvents.TYPE_REVIEW_REMINDER);
                CloudEvent escalated = published.next(Duration.ofMinutes(3));   // the rest (10 s): SQS
                assertThat(escalated.getType()).isEqualTo(StintEvents.TYPE_REVIEW_REQUESTED);
                assertThat(escalated.getExtension(StintEvents.EXT_ROLE)).isEqualTo("technical-rep");
                assertThat(escalated.getSubject()).isEqualTo(request.getSubject());

                engine.onDomainEvent(ReviewFlowTest.answer("ans-1", request.getSubject(), PORTAL, "technical-rep",
                        Json.obj().put("value", "resend").put("reviewer", "rev-42")), Duration.ofMinutes(5))
                        .toCompletableFuture().get(30, TimeUnit.SECONDS);

                JsonNode done = engine.awaitCompletion(id, Duration.ofMinutes(1)).toCompletableFuture()
                        .get(2, TimeUnit.MINUTES);
                assertThat(done.path("done").asText()).isEqualTo("resend");
                assertThat(done.path("by").asText()).isEqualTo("technical-rep");

                List<Correction> recorded = awaitCorrections(corrections);
                assertThat(recorded).singleElement().satisfies(c -> {
                    assertThat(c.reviewId()).isEqualTo(request.getSubject());
                    assertThat(c.human().path("value").asText()).isEqualTo("resend");
                    assertThat(c.escalated()).isTrue();
                    assertThat(c.materialRef()).isEqualTo("s3://stint-blobs/dlq/sdd-7.json");
                });
                StringWriter jsonl = new StringWriter();
                assertThat(CorrectionExport.write(recorded, Sensitivity.INTERNAL, jsonl)).isEqualTo(1);
                assertThat(audit.of(id).toCompletableFuture().get().stream().map(r -> r.outcome()).toList())
                        .containsExactly("requested", "reminded", "escalated", "answered");
            }
        }
    }

    private static List<Correction> awaitCorrections(DynamoDbCorrectionLog log) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        List<Correction> found = List.of();
        while (found.isEmpty() && System.nanoTime() < until) {
            found = log.of(REF).toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
        return found;
    }

    /** The publisher the engine relays facts to; each published event is handed to the test, in order. */
    private static final class Barrier implements EventPublisher {
        private final InMemoryEventPublisher delegate = new InMemoryEventPublisher();
        private final BlockingQueue<CloudEvent> events = new LinkedBlockingQueue<>();

        @Override
        public CompletionStage<Void> publish(CloudEvent event) {
            return delegate.publish(event).thenRun(() -> events.add(event));
        }

        @Override
        public AdapterCapabilities capabilities() {
            return delegate.capabilities();
        }

        CloudEvent next(Duration within) throws InterruptedException {
            CloudEvent event = events.poll(within.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(event).as("an event published within %s", within).isNotNull();
            return event;
        }
    }

    private static String queueArn(SqsClient sqs, String queueUrl) {
        return sqs.getQueueAttributes(r -> r.queueUrl(queueUrl).attributeNames(QueueAttributeName.QUEUE_ARN))
                .attributes().get(QueueAttributeName.QUEUE_ARN);
    }

    /** The SQS timer reporting a lower max delay, so the composite routes longer timers to the Scheduler. */
    private record LowerMaxDelay(TimerService delegate, Duration maxDelay) implements TimerService {
        @Override
        public CompletionStage<String> schedule(TimerRequest req) {
            return delegate.schedule(req);
        }

        @Override
        public CompletionStage<Void> cancel(String timerId) {
            return delegate.cancel(timerId);
        }

        @Override
        public void onFire(TimerFireHandler handler) {
            delegate.onFire(handler);
        }
    }
}
