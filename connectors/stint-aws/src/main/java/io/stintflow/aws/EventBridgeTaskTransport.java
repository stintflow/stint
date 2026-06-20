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
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * Alternative AWS transport: dispatch via EventBridge {@code PutEvents} (the routing key becomes the
 * {@code detail-type}, so EventBridge rules fan tasks out to the right targets), results over SQS.
 * Opt-in; disabled by default to avoid colliding with {@link SqsTaskTransport}.
 * <p>
 * Uses the raw AWS SDK (no Quarkus extension) — see the README native caveat.
 */
@Alternative
@ApplicationScoped
public class EventBridgeTaskTransport extends AbstractSqsResultTransport {

    @Inject
    EventBridgeClient eventBridge;

    @Inject
    SqsClient sqs;

    @ConfigProperty(name = "stint.aws.eventbridge.bus", defaultValue = "default")
    String eventBusName;

    @ConfigProperty(name = "stint.aws.sqs.result-queue-url", defaultValue = "")
    String resultQueueUrl;

    @Override
    public CompletionStage<Void> dispatch(TaskInvocation invocation) {
        return CompletableFuture.runAsync(() -> {
            String detail = new String(CeWire.toJson(codec.toEvent(invocation)), StandardCharsets.UTF_8);
            eventBridge.putEvents(PutEventsRequest.builder()
                    .entries(PutEventsRequestEntry.builder()
                            .eventBusName(eventBusName)
                            .source("io.stintflow")
                            .detailType(invocation.routingKey())
                            .detail(detail)
                            .build())
                    .build());
        });
    }

    @Override
    public AdapterCapabilities capabilities() {
        // EventBridge: at-least-once, unordered, 256 KB, no native delay (use a scheduler timer), no dedup.
        return new AdapterCapabilities(DeliveryGuarantee.AT_LEAST_ONCE, false,
                262_144, Duration.ofMinutes(15), false, false);
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
