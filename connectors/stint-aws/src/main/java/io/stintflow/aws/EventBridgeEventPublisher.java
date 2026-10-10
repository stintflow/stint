package io.stintflow.aws;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.cloudevents.CloudEvent;
import io.stintflow.spi.AdapterCapabilities;
import io.stintflow.spi.AdapterCapabilities.DeliveryGuarantee;
import io.stintflow.spi.EventPublisher;
import io.stintflow.wire.CeWire;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse;

/**
 * Domain channel on a dedicated EventBridge bus (SDD 2.2, RF8): {@code PutEvents} on
 * {@code stint.aws.eventbridge.domain-bus} with {@code Source} = the fact's {@code source},
 * {@code DetailType} = its {@code type} and {@code Detail} = the CloudEvent in structured JSON. A rule
 * targeting an SQS queue with {@code InputPath: "$.detail"} hands the bare CloudEvent to consumers —
 * e.g. another engine's {@link SqsDomainEventSource} (SDD 2.1).
 * <p>
 * The domain bus must differ from the task bus ({@code stint.aws.eventbridge.bus}, used by
 * {@link EventBridgeTaskTransport}): the same bus would mix engine traffic with public facts, so that
 * configuration fails at startup (sec. 8 risks). Accepted = {@code FailedEntryCount == 0}.
 */
@ApplicationScoped
public class EventBridgeEventPublisher implements EventPublisher {

    /** EventBridge's 256 KB entry limit, minus room for Source/DetailType/bus name around the Detail. */
    static final long MAX_PAYLOAD_BYTES = 256_000;

    @Inject
    EventBridgeClient eventBridge;

    @ConfigProperty(name = "stint.aws.eventbridge.domain-bus", defaultValue = "")
    String domainBus;

    @ConfigProperty(name = "stint.aws.eventbridge.bus", defaultValue = "default")
    String taskBus;

    public EventBridgeEventPublisher() {
    }

    /** Test/manual wiring outside CDI. */
    public EventBridgeEventPublisher(EventBridgeClient eventBridge, String domainBus, String taskBus) {
        this.eventBridge = eventBridge;
        this.domainBus = domainBus;
        this.taskBus = taskBus;
        checkChannels();
    }

    @PostConstruct
    void checkChannels() {
        if (domainBus != null && !domainBus.isBlank() && domainBus.equals(taskBus)) {
            throw new IllegalStateException("stint.aws.eventbridge.domain-bus must differ from stint.aws.eventbridge.bus ('"
                    + taskBus + "'): domain facts and engine traffic must not share a bus");
        }
    }

    @Override
    public CompletionStage<Void> publish(CloudEvent event) {
        return CompletableFuture.runAsync(() -> {
            if (domainBus == null || domainBus.isBlank()) {
                throw new IllegalStateException("stint.aws.eventbridge.domain-bus is not configured");
            }
            PutEventsResponse response = eventBridge.putEvents(PutEventsRequest.builder()
                    .entries(PutEventsRequestEntry.builder()
                            .eventBusName(domainBus)
                            .source(event.getSource().toString())
                            .detailType(event.getType())
                            .detail(new String(CeWire.toJson(event), StandardCharsets.UTF_8))
                            .build())
                    .build());
            if (response.failedEntryCount() != null && response.failedEntryCount() > 0) {
                var failure = response.entries().get(0);
                throw new IllegalStateException("EventBridge refused fact " + event.getId() + ": "
                        + failure.errorCode() + " " + failure.errorMessage());
            }
        });
    }

    @Override
    public AdapterCapabilities capabilities() {
        return new AdapterCapabilities(DeliveryGuarantee.AT_LEAST_ONCE, false, MAX_PAYLOAD_BYTES, null, false, false);
    }
}
