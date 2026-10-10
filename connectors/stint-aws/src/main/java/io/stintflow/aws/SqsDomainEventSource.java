package io.stintflow.aws;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.cloudevents.CloudEvent;
import io.stintflow.spi.DomainEventHandler;
import io.stintflow.spi.DomainEventSource;
import io.stintflow.wire.CeWire;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * {@link DomainEventSource} over an SQS inbound queue (SDD 2.1, RF5). Each message body is one
 * CloudEvent in structured JSON mode. EventBridge feeds the same queue through a rule whose SQS
 * target sets {@code InputPath: "$.detail"}, so the queue carries the bare CloudEvent, never the
 * EventBridge envelope (see {@code docs/connectors.md}).
 * <p>
 * Acknowledgement (sec. 8d): a message is deleted only when the handler's stage completes normally. A
 * body that isn't a valid CloudEvent is logged at ERROR and left undeleted, so the queue's redrive
 * policy moves it to the dead-letter queue after {@code maxReceiveCount} receives — the same path as
 * any persistent failure; the code never writes to the DLQ itself.
 */
@ApplicationScoped
public class SqsDomainEventSource implements DomainEventSource {

    private static final Logger LOG = LoggerFactory.getLogger(SqsDomainEventSource.class);

    @Inject
    SqsClient sqs;

    @ConfigProperty(name = "stint.aws.sqs.domain-events-queue-url", defaultValue = "")
    String queueUrl;

    private SqsPollLoop loop;

    public SqsDomainEventSource() {
    }

    /** Test/manual wiring outside CDI. */
    public SqsDomainEventSource(SqsClient sqs, String queueUrl) {
        this.sqs = sqs;
        this.queueUrl = queueUrl;
    }

    @Override
    public synchronized void onEvent(DomainEventHandler handler) {
        if (loop != null) {
            return;
        }
        loop = new SqsPollLoop(() -> sqs, () -> queueUrl, "stint-aws-domain-event-poller", m -> {
            CloudEvent event;
            try {
                event = CeWire.fromJson(m.body().getBytes(StandardCharsets.UTF_8));
            } catch (RuntimeException e) {
                LOG.error("Message {} on {} is not a valid CloudEvent; leaving it for the dead-letter queue: {}",
                        m.messageId(), queueUrl, e.getMessage());
                return CompletableFuture.failedFuture(e);
            }
            return handler.handle(event);
        });
        loop.start();
    }

    @PreDestroy
    synchronized void shutdown() {
        if (loop != null) {
            loop.stop();
        }
    }
}
