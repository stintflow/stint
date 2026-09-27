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

import io.stintflow.core.Json;
import io.stintflow.spi.TimerFire;
import io.stintflow.spi.TimerFireHandler;
import io.stintflow.spi.TimerRequest;
import io.stintflow.spi.TimerService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
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
 * an EventBridge Scheduler connector for those is on the roadmap (SDD 2.4).
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

    @Override
    public CompletionStage<String> schedule(TimerRequest req) {
        return CompletableFuture.supplyAsync(() -> {
            if (timerQueueUrl.isBlank()) {
                return req.timerId(); // timer queue not configured: arm is a no-op (MVP)
            }
            long seconds = Duration.between(Instant.now(), req.fireAt()).getSeconds();
            if (seconds > MAX_DELAY_SECONDS) {
                throw new IllegalArgumentException("Delay " + seconds + "s exceeds the SQS native max of "
                        + MAX_DELAY_SECONDS + "s — the caller must validate against maxDelay() before scheduling");
            }
            int delay = (int) Math.max(0, seconds);
            ObjectNode body = Json.obj();
            body.put("timerId", req.timerId());
            body.put("workflowInstanceId", req.workflowInstanceId());
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
                        .build());
                for (Message m : resp.messages()) {
                    JsonNode body = Json.read(m.body().getBytes(StandardCharsets.UTF_8));
                    TimerFire fire = new TimerFire(body.get("timerId").asText(), body.get("workflowInstanceId").asText());
                    handler.handle(fire).toCompletableFuture().join();
                    sqs.deleteMessage(DeleteMessageRequest.builder()
                            .queueUrl(timerQueueUrl)
                            .receiptHandle(m.receiptHandle())
                            .build());
                }
            } catch (Exception e) {
                LOG.warn("Timer poll failed: {}", e.getMessage());
            }
        }
    }

    /** For tests/shutdown. */
    void stopPolling() {
        running = false;
        if (poller != null) {
            poller.shutdownNow();
        }
    }
}
