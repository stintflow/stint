package io.stintflow.aws;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.spi.ScheduleFire;
import io.stintflow.spi.ScheduleFireHandler;
import io.stintflow.spi.ScheduleService;
import io.stintflow.spi.ScheduleSpec;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import software.amazon.awssdk.services.scheduler.SchedulerClient;
import software.amazon.awssdk.services.scheduler.model.ConflictException;
import software.amazon.awssdk.services.scheduler.model.FlexibleTimeWindowMode;
import software.amazon.awssdk.services.scheduler.model.ResourceNotFoundException;
import software.amazon.awssdk.services.scheduler.model.ScheduleSummary;
import software.amazon.awssdk.services.scheduler.model.Target;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;

/**
 * {@link ScheduleService} on EventBridge Scheduler (SDD 2.4, RF3).
 * <p>
 * Every schedule lives in one group per application, {@code stint.aws.scheduler.group} (sec. 8a). Its
 * expression is the {@link AwsCron} translation, with {@code ScheduleExpressionTimezone} set to the
 * definition's zone (sec. 8c). Its target is the schedule queue, and its {@code Input} is
 * {@code {"schedule", "definition", "scheduledTime": "<aws.scheduler.scheduled-time>"}} — AWS substitutes
 * the placeholder with the occurrence's instant.
 * <p>
 * {@link #onFire} polls that queue and acks a message only after the handler completes. A placeholder left
 * unsubstituted means the scheduler did not fill the occurrence in. SDD 2.7, sec. 8c: by default that is a
 * configuration error — the message is not acked (redelivered, then dead-lettered) and no instance starts. Only
 * with {@code stint.aws.scheduler.sent-timestamp-fallback=true} (for emulators such as floci) is it delivered as
 * an unknown scheduled time, with the message's {@code SentTimestamp} as {@code observedAt}, for the engine to
 * derive the occurrence (SDD 2.4, sec. 8e).
 * {@link #ensure} is create-if-absent ({@code ConflictException} means the schedule is already there) and
 * {@link #remove} ignores a missing schedule.
 */
@ApplicationScoped
public class EventBridgeSchedulerService implements ScheduleService {

    static final String SCHEDULED_TIME_PLACEHOLDER = "<aws.scheduler.scheduled-time>";

    @Inject
    SchedulerClient scheduler;

    @Inject
    SqsClient sqs;

    @ConfigProperty(name = "stint.aws.scheduler.group", defaultValue = "stint-schedules")
    String group;

    @ConfigProperty(name = "stint.aws.scheduler.role-arn", defaultValue = "")
    String roleArn;

    @ConfigProperty(name = "stint.aws.scheduler.schedule-target-arn", defaultValue = "")
    String targetArn;

    @ConfigProperty(name = "stint.aws.sqs.schedule-queue-url", defaultValue = "")
    String queueUrl;

    @ConfigProperty(name = "stint.aws.scheduler.sent-timestamp-fallback", defaultValue = "false")
    boolean sentTimestampFallback;

    private final AtomicBoolean groupEnsured = new AtomicBoolean();
    private SqsPollLoop loop;

    public EventBridgeSchedulerService() {
    }

    /** Test/manual wiring outside CDI, without the {@code SentTimestamp} fallback (the production default). */
    public EventBridgeSchedulerService(SchedulerClient scheduler, SqsClient sqs, String group, String roleArn,
            String targetArn, String queueUrl) {
        this(scheduler, sqs, group, roleArn, targetArn, queueUrl, false);
    }

    /** Test/manual wiring outside CDI; {@code sentTimestampFallback} only for emulators (SDD 2.7, sec. 8c). */
    public EventBridgeSchedulerService(SchedulerClient scheduler, SqsClient sqs, String group, String roleArn,
            String targetArn, String queueUrl, boolean sentTimestampFallback) {
        this.sentTimestampFallback = sentTimestampFallback;
        this.scheduler = scheduler;
        this.sqs = sqs;
        this.group = group;
        this.roleArn = roleArn;
        this.targetArn = targetArn;
        this.queueUrl = queueUrl;
    }

    @Override
    public CompletionStage<Void> ensure(ScheduleSpec spec) {
        return CompletableFuture.runAsync(() -> {
            SchedulerGroups.ensure(scheduler, group, groupEnsured);
            ObjectNode input = Json.obj();
            input.put("schedule", spec.name());
            input.put("definition", spec.definition().canonical());
            input.put("scheduledTime", SCHEDULED_TIME_PLACEHOLDER);
            try {
                scheduler.createSchedule(r -> r.name(spec.name()).groupName(group)
                        .scheduleExpression(AwsCron.of(spec.cron()))
                        .scheduleExpressionTimezone(spec.zone().getId())
                        .flexibleTimeWindow(w -> w.mode(FlexibleTimeWindowMode.OFF))
                        .target(Target.builder().arn(targetArn).roleArn(roleArn).input(input.toString()).build()));
            } catch (ConflictException alreadyThere) {
                // create-if-absent: another replica (or an earlier run) created it
            }
        });
    }

    @Override
    public CompletionStage<List<String>> list() {
        return CompletableFuture.supplyAsync(() -> scheduler.listSchedulesPaginator(r -> r.groupName(group))
                .schedules().stream().map(ScheduleSummary::name).sorted().toList());
    }

    @Override
    public CompletionStage<Void> remove(String name) {
        return CompletableFuture.runAsync(() -> {
            try {
                scheduler.deleteSchedule(r -> r.name(name).groupName(group));
            } catch (ResourceNotFoundException alreadyGone) {
                // idempotent
            }
        });
    }

    @Override
    public synchronized void onFire(ScheduleFireHandler handler) {
        if (loop != null || queueUrl.isBlank()) {
            return;
        }
        loop = new SqsPollLoop(() -> sqs, () -> queueUrl, "stint-aws-schedule-poller", m -> {
            ScheduleFire fire;
            try {
                fire = toFire(m, sentTimestampFallback);
            } catch (RuntimeException e) {
                return CompletableFuture.failedFuture(e); // not acked: redelivered, then dead-lettered
            }
            return handler.handle(fire);
        });
        loop.start();
    }

    static ScheduleFire toFire(Message m, boolean sentTimestampFallback) {
        JsonNode body = Json.read(m.body().getBytes(StandardCharsets.UTF_8));
        String scheduledTime = body.path("scheduledTime").asText("");
        Instant scheduled = scheduledTime.isEmpty() || scheduledTime.startsWith("<") ? null
                : Instant.parse(scheduledTime);
        if (scheduled == null && !sentTimestampFallback) {
            throw new IllegalStateException("Schedule " + body.path("schedule").asText() + " was delivered without "
                    + "its scheduled time (" + SCHEDULED_TIME_PLACEHOLDER + " not substituted): check the schedule's "
                    + "Input, or enable stint.aws.scheduler.sent-timestamp-fallback on an emulator");
        }
        String sent = m.attributes().get(MessageSystemAttributeName.SENT_TIMESTAMP);
        Instant observedAt = sent == null ? Instant.now() : Instant.ofEpochMilli(Long.parseLong(sent));
        return new ScheduleFire(body.get("schedule").asText(), WorkflowRef.parse(body.get("definition").asText()),
                scheduled, observedAt);
    }

    /** For tests/shutdown. */
    synchronized void stopPolling() {
        if (loop != null) {
            loop.stop();
        }
    }
}
