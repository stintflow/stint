package io.stintflow.aws;

import java.nio.charset.StandardCharsets;
import java.util.Map;
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
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

/**
 * Domain channel on a dedicated SNS topic (SDD 2.2, RF8), opt-in like {@link SnsTaskTransport}:
 * {@code Publish} on {@code stint.aws.sns.domain-topic-arn} with the CloudEvent (structured JSON) as
 * the message and a {@code type} message attribute for subscription {@code FilterPolicy}s. SQS
 * subscribers should set {@code RawMessageDelivery=true}, so the queue receives the bare CloudEvent
 * (what {@link SqsDomainEventSource} expects).
 * <p>
 * The domain topic must differ from the task topic ({@code stint.aws.sns.topic-arn}). Accepted = SNS
 * returned a {@code MessageId}.
 */
@Alternative
@ApplicationScoped
public class SnsEventPublisher implements EventPublisher {

    /** SNS's 256 KB message limit, minus room for the {@code type} attribute. */
    static final long MAX_PAYLOAD_BYTES = 256_000;

    @Inject
    SnsClient sns;

    @ConfigProperty(name = "stint.aws.sns.domain-topic-arn", defaultValue = "")
    String domainTopicArn;

    @ConfigProperty(name = "stint.aws.sns.topic-arn", defaultValue = "")
    String taskTopicArn;

    public SnsEventPublisher() {
    }

    /** Test/manual wiring outside CDI. */
    public SnsEventPublisher(SnsClient sns, String domainTopicArn, String taskTopicArn) {
        this.sns = sns;
        this.domainTopicArn = domainTopicArn;
        this.taskTopicArn = taskTopicArn;
        checkChannels();
    }

    @PostConstruct
    void checkChannels() {
        if (domainTopicArn != null && !domainTopicArn.isBlank() && domainTopicArn.equals(taskTopicArn)) {
            throw new IllegalStateException("stint.aws.sns.domain-topic-arn must differ from stint.aws.sns.topic-arn: "
                    + "domain facts and engine traffic must not share a topic");
        }
    }

    @Override
    public CompletionStage<Void> publish(CloudEvent event) {
        return CompletableFuture.runAsync(() -> {
            if (domainTopicArn == null || domainTopicArn.isBlank()) {
                throw new IllegalStateException("stint.aws.sns.domain-topic-arn is not configured");
            }
            PublishResponse response = sns.publish(PublishRequest.builder()
                    .topicArn(domainTopicArn)
                    .message(new String(CeWire.toJson(event), StandardCharsets.UTF_8))
                    .messageAttributes(Map.of("type", MessageAttributeValue.builder()
                            .dataType("String").stringValue(event.getType()).build()))
                    .build());
            if (response.messageId() == null) {
                throw new IllegalStateException("SNS returned no MessageId for fact " + event.getId());
            }
        });
    }

    @Override
    public AdapterCapabilities capabilities() {
        return new AdapterCapabilities(DeliveryGuarantee.AT_LEAST_ONCE, false, MAX_PAYLOAD_BYTES, null, false, false);
    }
}
