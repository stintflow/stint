package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.aws.DynamoDbStateStore;
import io.stintflow.aws.EventBridgeSchedulerService;
import io.stintflow.aws.EventBridgeSchedulerTimerService;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.model.Schedule;
import io.stintflow.core.schedule.ScheduleReconciler;
import io.stintflow.core.schedule.ScheduledStart;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryTaskTransport;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.spi.CronExpression;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.OutboxEntry;
import io.stintflow.spi.SaveOutcome;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.TimerRequest;
import io.stintflow.spi.Wait;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.spi.wire.StintEvents;
import io.stintflow.wire.Json;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.scheduler.SchedulerClient;
import software.amazon.awssdk.services.scheduler.model.ResourceNotFoundException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/**
 * SDD 2.4 on real floci EventBridge Scheduler (no fake):
 * <ol>
 *   <li>CA2: a long timer is a one-time {@code at(...)} schedule that delivers the <em>existing</em> timer JSON
 *       {@code {timerId, workflowInstanceId}} to the timer queue, and is deleted after it fired
 *       ({@code ActionAfterCompletion=DELETE});</li>
 *   <li>CA1 + RF6: two replicas reconcile one cron schedule (one schedule in the group). The real cron fires,
 *       the schedule queue's poller hands the occurrence to {@link ScheduledStart}, and an instance is created in
 *       DynamoDB with the synthetic event. floci leaves {@code <aws.scheduler.scheduled-time>} unsubstituted, so
 *       this exercises the sec. 8e fallback: the occurrence is derived from {@code SentTimestamp}, minute-aligned.</li>
 * </ol>
 * No sleeps: long-poll receives and an observed instance creation.
 */
class FlociSchedulerIT {

    private static final WorkflowRef EVERY_MINUTE = new WorkflowRef("reports", "every-minute", "1.0.0");
    private static final String ROLE = "arn:aws:iam::000000000000:role/stint-scheduler";

    @Test
    void long_timers_and_cron_schedules_on_eventbridge_scheduler() throws Exception {
        try (GenericContainer<?> floci = new GenericContainer<>(DockerImageName.parse("floci/floci:latest"))
                .withExposedPorts(4566)) {
            floci.start();
            URI endpoint = URI.create("http://" + floci.getHost() + ":" + floci.getMappedPort(4566));
            var creds = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));

            try (SqsClient sqs = SqsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                    .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build();
                 SchedulerClient scheduler = SchedulerClient.builder().endpointOverride(endpoint)
                         .region(Region.US_EAST_1).credentialsProvider(creds)
                         .httpClient(UrlConnectionHttpClient.create()).build();
                 DynamoDbClient ddb = DynamoDbClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                         .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build()) {

                String timerQueue = sqs.createQueue(q -> q.queueName("stint-timers")).queueUrl();
                String scheduleQueue = sqs.createQueue(q -> q.queueName("stint-schedules")).queueUrl();

                // 1) CA2: a long timer, armed to fire within the next minute or two (rounded up to the minute).
                EventBridgeSchedulerTimerService longTimers = new EventBridgeSchedulerTimerService(scheduler,
                        "stint-timers", ROLE, queueArn(sqs, timerQueue), Duration.ofDays(365));
                longTimers.schedule(new TimerRequest("timer:it-long", "inst-it", "corr-it",
                        Instant.now().plusSeconds(20))).toCompletableFuture().get(30, TimeUnit.SECONDS);
                assertThat(scheduler.getSchedule(r -> r.name("timer-it-long").groupName("stint-timers"))
                        .scheduleExpression()).startsWith("at(");

                // 2) CA1 + RF6: a cron schedule reconciled by two replicas, firing for real.
                DynamoDbTestTables.create(ddb, "stint-instances", "stint-waits");
                ObservedCreate state = new ObservedCreate(new DynamoDbStateStore(ddb, "stint-instances", "stint-waits"));
                WorkflowRegistry registry = new WorkflowRegistry();
                registry.register(WorkflowBuilder.create()
                        .set("trigger", Expr.jq("{id: $workflow.input[0].id, type: $workflow.input[0].type}"),
                                new DataFlow(null, null, Expr.jq(".")))
                        .schedule(Schedule.cron(CronExpression.parse("* * * * *"), ZoneId.of("America/Sao_Paulo")))
                        .build(EVERY_MINUTE));
                WorkflowEngine engine = new WorkflowEngine(registry, new InMemoryTaskTransport(), state,
                        new InMemoryTimerService(InstantSource.system()),
                        new FilesystemBlobStore(Files.createTempDirectory("stint-sdd24-it")));
                EventBridgeSchedulerService replicaA = new EventBridgeSchedulerService(scheduler, sqs,
                        "stint-schedules", ROLE, queueArn(sqs, scheduleQueue), scheduleQueue);
                EventBridgeSchedulerService replicaB = new EventBridgeSchedulerService(scheduler, sqs,
                        "stint-schedules", ROLE, queueArn(sqs, scheduleQueue), scheduleQueue);
                new ScheduleReconciler(registry, replicaA).reconcile().toCompletableFuture().get(30, TimeUnit.SECONDS);
                new ScheduleReconciler(registry, replicaB).reconcile().toCompletableFuture().get(30, TimeUnit.SECONDS);
                assertThat(replicaA.list().toCompletableFuture().get(30, TimeUnit.SECONDS))
                        .containsExactly(registry.scheduleSpecs().get(0).name()); // one schedule, not two
                replicaA.onFire(new ScheduledStart(registry, engine));

                // The timer JSON arrives on the timer queue, unchanged, and its schedule is gone afterwards.
                Message timerFire = awaitMessage(sqs, timerQueue, Duration.ofMinutes(3));
                JsonNode body = Json.read(timerFire.body().getBytes());
                assertThat(body.get("timerId").asText()).isEqualTo("timer:it-long");
                assertThat(body.get("workflowInstanceId").asText()).isEqualTo("inst-it");
                assertThatThrownBy(() -> scheduler.getSchedule(r -> r.name("timer-it-long").groupName("stint-timers")))
                        .isInstanceOf(ResourceNotFoundException.class);

                // The cron occurrence started an instance from the synthetic event (fallback: minute-aligned).
                String instanceId = state.firstCreate.get(3, TimeUnit.MINUTES);
                JsonNode trigger = engine.awaitCompletion(instanceId, Duration.ofSeconds(30)).toCompletableFuture()
                        .get(40, TimeUnit.SECONDS);
                assertThat(trigger.get("type").asText()).isEqualTo(StintEvents.TYPE_SCHEDULE_FIRED);
                String occurrence = trigger.get("id").asText().substring(trigger.get("id").asText().indexOf('@') + 1);
                assertThat(Instant.parse(occurrence).getEpochSecond() % 60).isZero();
                InstanceSnapshot snap = state.load(instanceId).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
                assertThat(snap.status()).isEqualTo(InstanceStatus.COMPLETED);
                assertThat(snap.chainId()).startsWith("chn-");
            }
        }
    }

    private static String queueArn(SqsClient sqs, String queueUrl) {
        return sqs.getQueueAttributes(r -> r.queueUrl(queueUrl).attributeNames(QueueAttributeName.QUEUE_ARN))
                .attributes().get(QueueAttributeName.QUEUE_ARN);
    }

    /** Long-poll receives (no sleep) until a message shows up or the deadline passes. */
    private static Message awaitMessage(SqsClient sqs, String queueUrl, Duration within) {
        long deadline = System.nanoTime() + within.toNanos();
        while (System.nanoTime() < deadline) {
            List<Message> messages = sqs.receiveMessage(r -> r.queueUrl(queueUrl).waitTimeSeconds(10)).messages();
            if (!messages.isEmpty()) {
                return messages.get(0);
            }
        }
        throw new AssertionError("No message on " + queueUrl + " within " + within);
    }

    /** Completes {@link #firstCreate} with the id of the first instance created. */
    private static final class ObservedCreate implements StateStore {
        private final StateStore delegate;
        final CompletableFuture<String> firstCreate = new CompletableFuture<>();

        ObservedCreate(StateStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion, List<Wait> addWaits,
                List<String> consumeWaitKeys, List<OutboxEntry> addOutbox) {
            return delegate.save(snapshot, expectedVersion, addWaits, consumeWaitKeys, addOutbox).thenApply(outcome -> {
                if (expectedVersion == 0 && outcome == SaveOutcome.OK) {
                    firstCreate.complete(snapshot.instanceId());
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
