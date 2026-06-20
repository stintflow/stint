package io.stintflow.aws;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.core.Json;
import io.stintflow.spi.TimerRequest;
import io.stintflow.spi.TimerService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * {@link TimerService} using SQS {@code DelaySeconds} (native, ≤ 15 min) for task timeouts.
 * <p>
 * Honest limitations: SQS cannot cancel an in-flight delayed message, so {@link #cancel(String)} is a
 * no-op — the orchestrator must ignore a timeout whose correlation already completed (it does). For
 * delays beyond 15 minutes, an EventBridge Scheduler connector is on the roadmap (see connectors.md).
 */
@ApplicationScoped
public class SqsDelayTimerService implements TimerService {

    private static final Logger LOG = LoggerFactory.getLogger(SqsDelayTimerService.class);
    private static final int MAX_DELAY_SECONDS = 900;

    @Inject
    SqsClient sqs;

    @ConfigProperty(name = "stint.aws.sqs.timer-queue-url", defaultValue = "")
    String timerQueueUrl;

    @Override
    public CompletionStage<String> schedule(TimerRequest req) {
        return CompletableFuture.supplyAsync(() -> {
            if (timerQueueUrl.isBlank()) {
                return req.timerId(); // timer queue not configured: arm is a no-op (MVP)
            }
            long seconds = Duration.between(Instant.now(), req.fireAt()).getSeconds();
            int delay = (int) Math.max(0, Math.min(MAX_DELAY_SECONDS, seconds));
            ObjectNode body = Json.obj();
            body.put("timerId", req.timerId());
            body.put("correlationId", req.correlationId());
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
        LOG.debug("SQS delay timers cannot be cancelled ({}); relying on correlation idempotency", timerId);
        return CompletableFuture.completedFuture(null);
    }
}
