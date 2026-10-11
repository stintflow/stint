package io.stintflow.aws;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.wire.Json;
import io.stintflow.spi.TimerFire;
import io.stintflow.spi.TimerFireHandler;
import io.stintflow.spi.TimerFireTooEarly;
import io.stintflow.spi.TimerRequest;
import io.stintflow.spi.TimerService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * {@link TimerService} using SQS {@code DelaySeconds} (native, ≤ 15 min) for task timeouts and retry
 * delays, with a poller consuming the same queue to actually fire them (SDD 1.3, RF1 — the fire→retry
 * path was the documented MVP gap this SDD closes).
 * <p>
 * Honest limitations: SQS cannot cancel an in-flight delayed message, so {@link #cancel(String)} is a
 * no-op — the orchestrator ignores a fire whose wait was already consumed (SDD 1.3, sec. 8a). Delays
 * beyond {@link #maxDelay()} are rejected by the engine before scheduling (sec. 8d), never truncated;
 * longer ones go to {@link EventBridgeSchedulerTimerService} (SDD 2.4), whose one-time schedules deliver the
 * same JSON to this queue, so this poller fires both.
 * <p>
 * SDD 2.9:
 * <ul>
 *   <li>RF1: the delay is rounded <em>up</em> to whole seconds ({@link #delaySeconds}): a timer never fires before
 *       its {@code fireAt}, and a future {@code fireAt} is never sent with {@code DelaySeconds=0};</li>
 *   <li>sec. 8c: the JSON is {@code {timerId, workflowInstanceId, armedAt}}; {@code armedAt} is optional on read;</li>
 *   <li>sec. 8a: each message is handled on its own. A fire the engine reports as {@link TimerFireTooEarly}
 *       (it arrived before the save of its wait) is not deleted: it is made visible again after
 *       {@link #earlyFireBackoffSeconds} (2, 4, 8, 16 s by receive count), so it is retried within the engine's
 *       early-fire window and well before the queue's redrive count. Any other failure is logged and left for
 *       the queue's visibility timeout, without holding back the rest of the batch.</li>
 * </ul>
 */
@ApplicationScoped
public class SqsDelayTimerService implements TimerService {

    private static final Logger LOG = LoggerFactory.getLogger(SqsDelayTimerService.class);
    private static final int MAX_DELAY_SECONDS = 900;

    @Inject
    SqsClient sqs;

    @ConfigProperty(name = "stint.aws.sqs.timer-queue-url", defaultValue = "")
    String timerQueueUrl;

    private volatile boolean running;
    private ExecutorService poller;

    public SqsDelayTimerService() {
    }

    /** Test/manual wiring outside CDI (SDD 2.7, like the other connectors). */
    public SqsDelayTimerService(SqsClient sqs, String timerQueueUrl) {
        this.sqs = sqs;
        this.timerQueueUrl = timerQueueUrl;
    }

    @Override
    public CompletionStage<String> schedule(TimerRequest req) {
        return CompletableFuture.supplyAsync(() -> {
            if (timerQueueUrl.isBlank()) {
                return req.timerId(); // timer queue not configured: arm is a no-op (MVP)
            }
            Instant now = Instant.now();
            int delay = delaySeconds(now, req.fireAt());
            ObjectNode body = Json.obj();
            body.put("timerId", req.timerId());
            body.put("workflowInstanceId", req.workflowInstanceId());
            body.put("armedAt", now.toString());
            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(timerQueueUrl)
                    .delaySeconds(delay)
                    .messageBody(body.toString())
                    .build());
            return req.timerId();
        });
    }

    @Override
    public CompletionStage<Void> cancel(String timerId) {
        LOG.debug("SQS delay timers cannot be cancelled ({}); relying on wait-key idempotency", timerId);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized void onFire(TimerFireHandler handler) {
        if (running || timerQueueUrl.isBlank()) {
            return;
        }
        running = true;
        poller = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "stint-sqs-timer-poller");
            t.setDaemon(true);
            return t;
        });
        poller.submit(() -> pollLoop(handler));
    }

    @Override
    public Duration maxDelay() {
        return Duration.ofSeconds(MAX_DELAY_SECONDS);
    }

    private void pollLoop(TimerFireHandler handler) {
        while (running) {
            try {
                var resp = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(timerQueueUrl)
                        .maxNumberOfMessages(10)
                        .waitTimeSeconds(5)
                        .messageSystemAttributeNames(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT)
                        .build());
                for (Message m : resp.messages()) {
                    handleOne(handler, m);
                }
            } catch (Exception e) {
                LOG.warn("Timer poll failed: {}", e.getMessage());
            }
        }
    }

    /** One message: fired and deleted, or (too early) made visible again shortly, or (failed) left as is. */
    private void handleOne(TimerFireHandler handler, Message m) {
        TimerFire fire;
        try {
            fire = fireOf(Json.read(m.body().getBytes(StandardCharsets.UTF_8)));
        } catch (RuntimeException unreadable) {
            LOG.warn("Unreadable timer message {} left for the queue's redrive: {}", m.messageId(), unreadable.getMessage());
            return;
        }
        try {
            handler.handle(fire).toCompletableFuture().join();
            sqs.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(timerQueueUrl)
                    .receiptHandle(m.receiptHandle())
                    .build());
        } catch (Exception e) {
            if (TimerFireTooEarly.isCause(e)) {
                int receiveCount = Integer.parseInt(m.attributes()
                        .getOrDefault(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT, "1"));
                int backoff = earlyFireBackoffSeconds(receiveCount);
                sqs.changeMessageVisibility(r -> r.queueUrl(timerQueueUrl).receiptHandle(m.receiptHandle())
                        .visibilityTimeout(backoff));
                LOG.debug("Timer {} fired before its wait was saved (receive {}); delivering it again in {} s",
                        fire.timerId(), receiveCount, backoff);
            } else {
                LOG.warn("Timer {} of instance {} failed; left for redelivery: {}", fire.timerId(),
                        fire.workflowInstanceId(), e.getMessage());
            }
        }
    }

    /** RF1: whole seconds from {@code now} to {@code fireAt}, rounded up; never past the SQS maximum. */
    static int delaySeconds(Instant now, Instant fireAt) {
        Duration delay = Duration.between(now, fireAt);
        if (delay.isNegative() || delay.isZero()) {
            return 0;
        }
        long seconds = delay.getSeconds() + (delay.getNano() > 0 ? 1 : 0);
        if (seconds > MAX_DELAY_SECONDS) {
            if (delay.compareTo(Duration.ofSeconds(MAX_DELAY_SECONDS)) <= 0) {
                return MAX_DELAY_SECONDS;
            }
            throw new IllegalArgumentException("Delay " + delay + " exceeds the SQS native max of "
                    + MAX_DELAY_SECONDS + "s — the caller must validate against maxDelay() before scheduling");
        }
        return (int) seconds;
    }

    /** Sec. 8a: 2, 4, 8, then 16 s for every later receive — 30 s over the first four, within the early-fire window. */
    static int earlyFireBackoffSeconds(int receiveCount) {
        return 1 << Math.min(Math.max(receiveCount, 1), 4);
    }

    /** Sec. 8c: the timer JSON, {@code armedAt} optional (timers armed before SDD 2.9 don't have it). */
    static TimerFire fireOf(JsonNode body) {
        JsonNode armedAt = body.get("armedAt");
        return new TimerFire(body.get("timerId").asText(), body.get("workflowInstanceId").asText(),
                armedAt == null || armedAt.isNull() ? null : Instant.parse(armedAt.asText()));
    }

    /** For tests/shutdown. */
    void stopPolling() {
        running = false;
        if (poller != null) {
            poller.shutdownNow();
        }
    }
}
