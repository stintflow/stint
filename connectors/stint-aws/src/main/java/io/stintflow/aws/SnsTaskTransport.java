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
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * Alternative AWS transport: fan-out dispatch via SNS (topic), results still over an SQS result queue
 * (the classic SNS→SQS pattern). Opt-in — enable with a CDI {@code @Priority}/profile. Disabled by
 * default so it does not collide with {@link SqsTaskTransport}.
 */
@Alternative
@ApplicationScoped
public class SnsTaskTransport extends AbstractSqsResultTransport {

    @Inject
    SnsClient sns;

    @Inject
    SqsClient sqs;

    @ConfigProperty(name = "stint.aws.sns.topic-arn", defaultValue = "")
    String topicArn;

    @ConfigProperty(name = "stint.aws.sqs.result-queue-url", defaultValue = "")
    String resultQueueUrl;

    @Override
    public CompletionStage<Void> dispatch(TaskInvocation invocation) {
        return CompletableFuture.runAsync(() -> {
            String body = new String(CeWire.toJson(codec.toEvent(invocation)), StandardCharsets.UTF_8);
            sns.publish(PublishRequest.builder().topicArn(topicArn).message(body).build());
        });
    }

    @Override
    public AdapterCapabilities capabilities() {
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
