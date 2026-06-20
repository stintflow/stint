package io.stintflow.aws;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.stintflow.core.CeWire;
import io.stintflow.spi.AdapterCapabilities;
import io.stintflow.spi.AdapterCapabilities.DeliveryGuarantee;
import io.stintflow.spi.TaskInvocation;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * Primary AWS transport: dispatch over an SQS invoke queue, receive results over an SQS result queue.
 * Fully supported by the Quarkus SQS extension and native-friendly; the default for floci testing.
 */
@ApplicationScoped
public class SqsTaskTransport extends AbstractSqsResultTransport {

    @Inject
    SqsClient sqs;

    @ConfigProperty(name = "stint.aws.sqs.invoke-queue-url")
    String invokeQueueUrl;

    @ConfigProperty(name = "stint.aws.sqs.result-queue-url")
    String resultQueueUrl;

    @Override
    public CompletionStage<Void> dispatch(TaskInvocation invocation) {
        return CompletableFuture.runAsync(() -> {
            String body = new String(CeWire.toJson(codec.toEvent(invocation)), StandardCharsets.UTF_8);
            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(invokeQueueUrl)
                    .messageBody(body)
                    .build());
        });
    }

    @Override
    public AdapterCapabilities capabilities() {
        // SQS standard: at-least-once, unordered, 256 KB, native delay up to 15 min, no dedup.
        return new AdapterCapabilities(DeliveryGuarantee.AT_LEAST_ONCE, false,
                262_144, Duration.ofMinutes(15), true, false);
    }

    @Override
    protected SqsClient sqs() {
        return sqs;
    }

    @Override
    protected String resultQueueUrl() {
        return resultQueueUrl;
    }

    @PreDestroy
    void shutdown() {
        stopPolling();
    }
}
