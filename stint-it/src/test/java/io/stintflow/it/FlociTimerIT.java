package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import io.stintflow.aws.DynamoDbStateStore;
import io.stintflow.aws.EventBridgeSchedulerTimerService;
import io.stintflow.aws.SqsDelayTimerService;
import io.stintflow.aws.SqsTaskTransport;
import io.stintflow.core.DelayRoutingTimerService;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.model.FlowDirective;
import io.stintflow.core.model.ListenNode;
import io.stintflow.core.model.RetryPolicy;
import io.stintflow.core.model.TryNode;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.TimerFireHandler;
import io.stintflow.spi.TimerRequest;
import io.stintflow.spi.TimerService;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.CeWire;
import io.stintflow.wire.DefaultCloudEventCodec;
import io.stintflow.wire.Json;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.scheduler.SchedulerClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/**
 * SDD 2.7, CA3: the engine's timers on real infrastructure — no {@code InMemoryTimerService}. One engine,
 * {@link DelayRoutingTimerService} over {@link SqsDelayTimerService} (short) and {@link EventBridgeSchedulerTimerService}
 * (long), state in DynamoDB, tasks over SQS:
 * <ol>
 *   <li>a task timeout fires through SQS {@code DelaySeconds} and fails the instance;</li>
 *   <li>a timed-out attempt inside {@code try} is retried after a delay that is also an SQS timer; attempt 2 is
 *       answered and the instance completes;</li>
 *   <li>a long timer goes through an EventBridge Scheduler one-time schedule. The schedule delivers the timer JSON
 *       to the same timer queue, the SQS poller fires it into the engine, and a {@code listen} timeout is caught.
 *       Only here the short connector reports a 10 s max delay instead of SQS's 15 min, so the long path runs in
 *       about two minutes instead of fifteen.</li>
 * </ol>
 * No sleeps: completions, long-poll receives and observed dispatches.
 */
class FlociTimerIT {

    private static final String ROLE = "arn:aws:iam::000000000000:role/stint-scheduler";
    private static final WorkflowRef TIMES_OUT = new WorkflowRef("timers", "times-out", "1.0.0");
    private static final WorkflowRef RETRIES = new WorkflowRef("timers", "retries", "1.0.0");
    private static final WorkflowRef WAITS_LONG = new WorkflowRef("timers", "waits-long", "1.0.0");

    @Test
    void task_timeout_retry_delay_and_a_long_timer_fire_through_real_sqs_and_scheduler() throws Exception {
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

                String invokeQueue = sqs.createQueue(q -> q.queueName("timer-it-invoke")).queueUrl();
                String resultQueue = sqs.createQueue(q -> q.queueName("timer-it-result")).queueUrl();
                String timerQueue = sqs.createQueue(q -> q.queueName("stint-timers")).queueUrl();
                DynamoDbTestTables.create(ddb, "stint-instances", "stint-waits");
                DynamoDbTestTables.createInbox(ddb, "stint-inbox"); // a listen checks the early-event inbox (SDD 2.3)
                StateStore state = new DynamoDbStateStore(ddb, "stint-instances", "stint-waits");

                SqsDelayTimerService sqsTimers = new SqsDelayTimerService(sqs, timerQueue);
                EventBridgeSchedulerTimerService schedulerTimers = new EventBridgeSchedulerTimerService(scheduler,
                        "stint-timers", ROLE, queueArn(sqs, timerQueue), Duration.ofDays(365));
                DelayRoutingTimerService timers = new DelayRoutingTimerService(
                        new LowerMaxDelay(sqsTimers, Duration.ofSeconds(10)), schedulerTimers, InstantSource.system());

                WorkflowRegistry registry = new WorkflowRegistry(timers.maxDelay());
                registry.register(WorkflowBuilder.create()
                        .callRemote("work", "route-silent", DataFlow.NONE, FlowDirective.CONTINUE, Duration.ofSeconds(5))
                        .build(TIMES_OUT));
                registry.register(WorkflowBuilder.create()
                        .tryRemote("guarded", DataFlow.NONE, "work", "route-flaky", DataFlow.NONE, Duration.ofSeconds(3),
                                WorkflowBuilder.catchAnyWithRetry(new RetryPolicy(Duration.ofSeconds(2),
                                        RetryPolicy.Backoff.CONSTANT, 2, null, 0.0), null, FlowDirective.CONTINUE),
                                FlowDirective.CONTINUE)
                        .build(RETRIES));
                registry.register(WorkflowBuilder.create()
                        .tryListen("guarded", DataFlow.NONE, "awaitReview", ListenNode.Strategy.ONE,
                                List.of(new ListenNode.Filter("io.acme.review.completed.v1", Map.of(), new TreeMap<>(
                                        Map.of("instance", new ListenNode.Correlation(Expr.jq(".data.instanceId"),
                                                Expr.jq("$workflow.id")))))),
                                null, Duration.ofSeconds(60), DataFlow.NONE,
                                new TryNode.Catch(null, null, null, null, Expr.jq("{timedOut: true}"), FlowDirective.CONTINUE),
                                FlowDirective.CONTINUE)
                        .set("outcome", Expr.jq("."), new DataFlow(null, null, Expr.jq(".")))
                        .build(WAITS_LONG));

                WorkflowEngine engine = new WorkflowEngine(registry, new SqsTaskTransport(sqs, invokeQueue, resultQueue),
                        state, timers, new FilesystemBlobStore(Files.createTempDirectory("stint-sdd27-timers")),
                        new TreeInterpreter(new JqExpressionEvaluator()), InstantSource.system(), null, null);

                String timesOut = engine.start(TIMES_OUT, Json.obj());
                String retries = engine.start(RETRIES, Json.obj());
                String waitsLong = engine.start(WAITS_LONG, Json.obj());

                // A worker that ignores attempt 1 of route-flaky and every route-silent invoke, and answers attempt 2.
                CompletableFuture<Integer> answeredAttempt = CompletableFuture.supplyAsync(() ->
                        answerSecondAttempt(sqs, invokeQueue, resultQueue));

                // 1) The task timeout fires through SQS: the instance fails.
                assertThat(awaitTerminal(engine, state, timesOut)).isEqualTo(InstanceStatus.FAILED);

                // 2) Timeout -> retry delay -> attempt 2, all SQS timers.
                assertThat(answeredAttempt.get(2, TimeUnit.MINUTES)).isEqualTo(2);
                assertThat(awaitTerminal(engine, state, retries)).isEqualTo(InstanceStatus.COMPLETED);

                // 3) The long timer is a one-time schedule, fired into the engine through the timer queue.
                assertThat(scheduler.listSchedules(r -> r.groupName("stint-timers")).schedules()).isNotEmpty();
                assertThat(engine.awaitCompletion(waitsLong, Duration.ofMinutes(4)).toCompletableFuture()
                        .get(5, TimeUnit.MINUTES).get("timedOut").asBoolean()).isTrue();
                assertThat(scheduler.listSchedules(r -> r.groupName("stint-timers")).schedules()).isEmpty();
            }
        }
    }

    private static InstanceStatus awaitTerminal(WorkflowEngine engine, StateStore state, String id) throws Exception {
        engine.awaitCompletion(id, Duration.ofMinutes(2)).toCompletableFuture().handle((r, e) -> null)
                .get(3, TimeUnit.MINUTES);
        InstanceSnapshot snap = state.load(id).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow();
        return snap.status();
    }

    /** Long-polls the invoke queue; answers route-flaky's attempt 2 and returns its attempt number. */
    private static int answerSecondAttempt(SqsClient sqs, String invokeQueue, String resultQueue) {
        DefaultCloudEventCodec codec = new DefaultCloudEventCodec();
        long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
        while (System.nanoTime() < deadline) {
            for (Message m : sqs.receiveMessage(r -> r.queueUrl(invokeQueue).waitTimeSeconds(5)).messages()) {
                sqs.deleteMessage(r -> r.queueUrl(invokeQueue).receiptHandle(m.receiptHandle()));
                TaskInvocation invocation = codec.toInvocation(CeWire.fromJson(m.body().getBytes(StandardCharsets.UTF_8)));
                if (invocation.routingKey().equals("route-flaky") && invocation.attempt() == 2) {
                    sqs.sendMessage(r -> r.queueUrl(resultQueue).messageBody(new String(CeWire.toJson(codec.toEvent(
                            TaskResult.completed(invocation.correlationId(), Json.obj()), invocation.workflowInstanceId())),
                            StandardCharsets.UTF_8)));
                    return invocation.attempt();
                }
            }
        }
        throw new AssertionError("attempt 2 of route-flaky was never dispatched");
    }

    private static String queueArn(SqsClient sqs, String queueUrl) {
        return sqs.getQueueAttributes(r -> r.queueUrl(queueUrl).attributeNames(QueueAttributeName.QUEUE_ARN))
                .attributes().get(QueueAttributeName.QUEUE_ARN);
    }

    /** The SQS timer reporting a lower max delay, so the composite routes a 60 s timer to the Scheduler. */
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
