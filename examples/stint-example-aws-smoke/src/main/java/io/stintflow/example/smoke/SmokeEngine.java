package io.stintflow.example.smoke;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.cloudevents.CloudEvent;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.stintflow.aws.DynamoDbStateStore;
import io.stintflow.aws.EventBridgeEventPublisher;
import io.stintflow.aws.EventBridgeSchedulerService;
import io.stintflow.aws.EventBridgeSchedulerTimerService;
import io.stintflow.aws.S3BlobStore;
import io.stintflow.aws.SqsDelayTimerService;
import io.stintflow.aws.SqsTaskTransport;
import io.stintflow.core.DelayRoutingTimerService;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.core.schedule.ScheduleReconciler;
import io.stintflow.dsl.DslLoader;
import io.stintflow.dsl.LoadOptions;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.CeWire;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerRuntime;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.scheduler.SchedulerClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

/**
 * SDD 2.7: the engine of the smoke app, wired from the clients the Quarkus AWS extensions produce. It uses:
 * <ul>
 *   <li>tasks over SQS, with an in-process worker loop standing in for a deployed worker;</li>
 *   <li>state, waits and outbox in DynamoDB;</li>
 *   <li>blobs in S3;</li>
 *   <li>facts on the EventBridge domain bus;</li>
 *   <li>timers composed from SQS and EventBridge Scheduler;</li>
 *   <li>the definition's cron reconciled into EventBridge Scheduler at startup.</li>
 * </ul>
 */
@ApplicationScoped
public class SmokeEngine {

    public static final WorkflowRef REF = new WorkflowRef("smoke", "aws-smoke", "1.0.0");

    @Inject
    SqsClient sqs;
    @Inject
    DynamoDbClient ddb;
    @Inject
    S3Client s3;
    @Inject
    EventBridgeClient eventBridge;
    @Inject
    SchedulerClient scheduler;

    @ConfigProperty(name = "smoke.sqs.invoke-queue-url")
    String invokeQueue;
    @ConfigProperty(name = "smoke.sqs.result-queue-url")
    String resultQueue;
    @ConfigProperty(name = "smoke.sqs.timer-queue-url")
    String timerQueue;
    @ConfigProperty(name = "smoke.sqs.schedule-queue-url")
    String scheduleQueue;
    @ConfigProperty(name = "smoke.scheduler.role-arn")
    String roleArn;
    @ConfigProperty(name = "smoke.scheduler.timer-target-arn")
    String timerTargetArn;
    @ConfigProperty(name = "smoke.scheduler.schedule-target-arn")
    String scheduleTargetArn;
    @ConfigProperty(name = "smoke.eventbridge.domain-bus")
    String domainBus;
    @ConfigProperty(name = "smoke.s3.bucket")
    String bucket;

    private WorkflowEngine engine;
    private final ExecutorService worker = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "smoke-worker");
        t.setDaemon(true);
        return t;
    });
    private volatile boolean running = true;

    void onStart(@Observes StartupEvent event) throws Exception {
        BlobStore blobs = new S3BlobStore(s3, bucket);
        DelayRoutingTimerService timers = new DelayRoutingTimerService(new SqsDelayTimerService(sqs, timerQueue),
                new EventBridgeSchedulerTimerService(scheduler, "stint-timers", roleArn, timerTargetArn,
                        Duration.ofDays(365)), InstantSource.system());
        WorkflowRegistry registry = new WorkflowRegistry(timers.maxDelay());
        List<WorkflowDefinition> defs = new DslLoader().loadClasspath("stint/workflows/*.yaml", LoadOptions.STRICT);
        defs.forEach(registry::register);
        engine = new WorkflowEngine(registry, new SqsTaskTransport(sqs, invokeQueue, resultQueue),
                new DynamoDbStateStore(ddb, "stint-instances", "stint-waits", "stint-outbox", 4, "stint-inbox"),
                timers, blobs, new TreeInterpreter(new JqExpressionEvaluator()), InstantSource.system(),
                new EventBridgeEventPublisher(eventBridge, domainBus, "default"), null);
        new ScheduleReconciler(registry, new EventBridgeSchedulerService(scheduler, sqs, "stint-schedules", roleArn,
                scheduleTargetArn, scheduleQueue)).reconcile().toCompletableFuture().get(30, TimeUnit.SECONDS);

        WorkerRuntime runtime = new WorkerRuntime(new TaskHandlerRegistry().register("echo", ctx -> {
            ObjectNode out = Json.obj();
            out.put("echo", ctx.input().path("name").asText());
            out.set("nameLength", ctx.input().path("nameLength"));
            out.put("chainId", ctx.chainId());
            return java.util.concurrent.CompletableFuture.completedFuture(out);
        }), blobs);
        worker.submit(() -> workLoop(runtime));
    }

    void onStop(@Observes ShutdownEvent event) {
        running = false;
        worker.shutdownNow();
    }

    public WorkflowEngine engine() {
        return engine;
    }

    /** A prototype worker binding: invoke queue -> worker runtime -> result queue, acked after the result is sent. */
    private void workLoop(WorkerRuntime runtime) {
        while (running) {
            try {
                for (Message m : sqs.receiveMessage(r -> r.queueUrl(invokeQueue).waitTimeSeconds(5)).messages()) {
                    CloudEvent result = runtime.handle(CeWire.fromJson(m.body().getBytes(StandardCharsets.UTF_8)))
                            .toCompletableFuture().get(30, TimeUnit.SECONDS);
                    sqs.sendMessage(r -> r.queueUrl(resultQueue)
                            .messageBody(new String(CeWire.toJson(result), StandardCharsets.UTF_8)));
                    sqs.deleteMessage(r -> r.queueUrl(invokeQueue).receiptHandle(m.receiptHandle()));
                }
            } catch (Exception e) {
                if (running) {
                    org.jboss.logging.Logger.getLogger(SmokeEngine.class).warn("Worker poll failed", e);
                }
            }
        }
    }
}
