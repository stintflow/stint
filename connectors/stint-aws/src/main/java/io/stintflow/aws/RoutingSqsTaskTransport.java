package io.stintflow.aws;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.stintflow.spi.AdapterCapabilities;
import io.stintflow.spi.AdapterCapabilities.DeliveryGuarantee;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.wire.CeWire;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * SDD 3a.9: an SQS task transport that picks the <em>invoke queue</em> by the task's routing key, so one engine can send
 * {@code redact} to one service's queue and {@code decide} to another's. Results come back over one result queue, as in
 * {@link SqsTaskTransport}; the optional {@code replyTo} name is announced on every invoke ({@code stintreplyto}) so a
 * worker shared by several engines answers the right one.
 * <p>
 * Routes match the routing key <em>exactly</em>. A key with no route goes to the default invoke queue, if any;
 * otherwise {@code dispatch} fails with {@link UnroutableTaskException}. Since the engine only sees a failed dispatch
 * as the task's timeout, an application should check its workflows' keys at startup with {@link #unroutable}.
 */
public final class RoutingSqsTaskTransport extends AbstractSqsResultTransport {

    private final SqsClient sqs;
    private final String resultQueueUrl;
    private final String replyTo;
    private final String defaultInvokeQueueUrl;
    private final Map<String, String> routes;

    private RoutingSqsTaskTransport(Builder b) {
        this.sqs = Objects.requireNonNull(b.sqs, "sqs is required");
        this.resultQueueUrl = Objects.requireNonNull(b.resultQueueUrl, "resultQueueUrl is required");
        this.replyTo = b.replyTo;
        this.defaultInvokeQueueUrl = b.defaultInvokeQueueUrl;
        this.routes = Map.copyOf(b.routes);
        if (routes.isEmpty() && defaultInvokeQueueUrl == null) {
            throw new IllegalArgumentException("A routing transport needs at least a route or a default invoke queue");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public CompletionStage<Void> dispatch(TaskInvocation invocation) {
        String queue = routes.getOrDefault(invocation.routingKey(), defaultInvokeQueueUrl);
        if (queue == null) {
            return CompletableFuture.failedFuture(new UnroutableTaskException(invocation.routingKey(), routes.keySet()));
        }
        return CompletableFuture.runAsync(() -> {
            String body = new String(CeWire.toJson(withReplyTo(codec.toEvent(invocation), replyTo)),
                    StandardCharsets.UTF_8);
            sqs.sendMessage(SendMessageRequest.builder().queueUrl(queue).messageBody(body).build());
        });
    }

    /** The keys among {@code routingKeys} that {@link #dispatch} could not route — for a check at startup. */
    public Set<String> unroutable(Collection<String> routingKeys) {
        if (defaultInvokeQueueUrl != null) {
            return Set.of();
        }
        Set<String> missing = new TreeSet<>(routingKeys);
        missing.removeAll(routes.keySet());
        return missing;
    }

    @Override
    public AdapterCapabilities capabilities() {
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

    public void close() {
        stopPolling();
    }

    public static final class Builder {
        private SqsClient sqs;
        private String resultQueueUrl;
        private String replyTo;
        private String defaultInvokeQueueUrl;
        private final Map<String, String> routes = new LinkedHashMap<>();

        private Builder() {
        }

        public Builder sqs(SqsClient sqs) {
            this.sqs = sqs;
            return this;
        }

        /** The queue this engine reads results from. */
        public Builder resultQueueUrl(String url) {
            this.resultQueueUrl = url;
            return this;
        }

        /** The logical name announced as {@code stintreplyto}; the worker maps it to a result queue. Optional. */
        public Builder replyTo(String name) {
            this.replyTo = checkedReplyTo(name);
            return this;
        }

        /** The invoke queue of a routing key with no route of its own. Optional. */
        public Builder defaultInvokeQueueUrl(String url) {
            this.defaultInvokeQueueUrl = url;
            return this;
        }

        /** Sends the tasks whose routing key is exactly {@code routingKey} to {@code invokeQueueUrl}. */
        public Builder route(String routingKey, String invokeQueueUrl) {
            Objects.requireNonNull(routingKey, "routingKey");
            Objects.requireNonNull(invokeQueueUrl, "invokeQueueUrl");
            if (routes.putIfAbsent(routingKey, invokeQueueUrl) != null) {
                throw new IllegalArgumentException("Routing key already routed: " + routingKey);
            }
            return this;
        }

        public RoutingSqsTaskTransport build() {
            return new RoutingSqsTaskTransport(this);
        }
    }
}
