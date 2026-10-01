package io.stintflow.aws;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.spi.TimerFireHandler;
import io.stintflow.spi.TimerRequest;
import io.stintflow.spi.TimerService;
import io.stintflow.wire.Json;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import software.amazon.awssdk.services.scheduler.SchedulerClient;
import software.amazon.awssdk.services.scheduler.model.ActionAfterCompletion;
import software.amazon.awssdk.services.scheduler.model.ConflictException;
import software.amazon.awssdk.services.scheduler.model.FlexibleTimeWindowMode;
import software.amazon.awssdk.services.scheduler.model.ResourceNotFoundException;
import software.amazon.awssdk.services.scheduler.model.Target;

/**
 * Long timers on EventBridge Scheduler one-time schedules (SDD 2.4, RF3, sec. 8d), for delays beyond SQS's
 * 15 minutes, behind {@code io.stintflow.core.DelayRoutingTimerService}. Each timer is one schedule:
 * <ul>
 *   <li>name: the timer id with anything outside {@code [0-9a-zA-Z-_.]} replaced by {@code -};</li>
 *   <li>group: {@code stint.aws.scheduler.timer-group};</li>
 *   <li>expression: {@code at(...)} in UTC, <em>rounded up</em> to the whole minute, so it never fires early;</li>
 *   <li>{@code FlexibleTimeWindow=OFF};</li>
 *   <li>{@code ActionAfterCompletion=DELETE}, so a fired schedule doesn't use up the quota.</li>
 * </ul>
 * Its target is <strong>the timer queue</strong> of {@link SqsDelayTimerService}, and its {@code Input} is
 * <strong>the same JSON</strong>, {@code {timerId, workflowInstanceId}} — not a CloudEvent (SDD 1.3). That
 * queue's poller fires it, so {@link #onFire} has nothing to do here.
 * <p>
 * CDI: injectable by its own type only, so {@code TimerService} injection points keep resolving to
 * {@link SqsDelayTimerService}; wire the composite by hand.
 * <p>
 * Precision: between {@code fireAt} and about a minute later. {@link #cancel} deletes the schedule,
 * best-effort; a late fire finds no wait and is a no-op (SDD 1.3).
 */
@ApplicationScoped
@Typed(EventBridgeSchedulerTimerService.class) // not a second TimerService bean: compose it explicitly
public class EventBridgeSchedulerTimerService implements TimerService {

    private static final DateTimeFormatter AT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(ZoneOffset.UTC);

    @Inject
    SchedulerClient scheduler;

    @ConfigProperty(name = "stint.aws.scheduler.timer-group", defaultValue = "stint-timers")
    String group;

    @ConfigProperty(name = "stint.aws.scheduler.role-arn", defaultValue = "")
    String roleArn;

    @ConfigProperty(name = "stint.aws.scheduler.timer-target-arn", defaultValue = "")
    String targetArn;

    @ConfigProperty(name = "stint.aws.scheduler.timer-max-delay", defaultValue = "P365D")
    Duration maxDelay;

    private final AtomicBoolean groupEnsured = new AtomicBoolean();

    public EventBridgeSchedulerTimerService() {
    }

    /** Test/manual wiring outside CDI. */
    public EventBridgeSchedulerTimerService(SchedulerClient scheduler, String group, String roleArn, String targetArn,
            Duration maxDelay) {
        this.scheduler = scheduler;
        this.group = group;
        this.roleArn = roleArn;
        this.targetArn = targetArn;
        this.maxDelay = maxDelay;
    }

    @Override
    public CompletionStage<String> schedule(TimerRequest req) {
        return CompletableFuture.supplyAsync(() -> {
            SchedulerGroups.ensure(scheduler, group, groupEnsured);
            ObjectNode body = Json.obj();
            body.put("timerId", req.timerId());
            body.put("workflowInstanceId", req.workflowInstanceId());
            try {
                scheduler.createSchedule(r -> r.name(scheduleName(req.timerId())).groupName(group)
                        .scheduleExpression("at(" + AT.format(roundUpToMinute(req.fireAt())) + ")")
                        .scheduleExpressionTimezone("UTC")
                        .flexibleTimeWindow(w -> w.mode(FlexibleTimeWindowMode.OFF))
                        .actionAfterCompletion(ActionAfterCompletion.DELETE)
                        .target(Target.builder().arn(targetArn).roleArn(roleArn).input(body.toString()).build()));
            } catch (ConflictException alreadyArmed) {
                // the same timer armed again (a re-run of the same activation): one schedule is enough
            }
            return req.timerId();
        });
    }

    @Override
    public CompletionStage<Void> cancel(String timerId) {
        return CompletableFuture.runAsync(() -> {
            try {
                scheduler.deleteSchedule(r -> r.name(scheduleName(timerId)).groupName(group));
            } catch (ResourceNotFoundException notThere) {
                // fired already, never armed here (a short timer), or cancelled before
            }
        });
    }

    @Override
    public void onFire(TimerFireHandler handler) {
        // Fires arrive on the timer queue, polled by SqsDelayTimerService.
    }

    @Override
    public Duration maxDelay() {
        return maxDelay;
    }

    static String scheduleName(String timerId) {
        return timerId.replaceAll("[^0-9a-zA-Z\\-_.]", "-");
    }

    static Instant roundUpToMinute(Instant at) {
        Instant truncated = at.truncatedTo(ChronoUnit.MINUTES);
        return truncated.equals(at) ? at : truncated.plus(1, ChronoUnit.MINUTES);
    }
}
