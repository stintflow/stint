package io.stintflow.aws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Test;

import io.cloudevents.CloudEvent;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.spi.wire.StintEvents;
import io.stintflow.wire.CeWire;
import io.stintflow.wire.Json;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

/** SDD 3a.9, CA1 and CA6: the reply name on the invoke, and the invoke queue chosen by the routing key. */
class RoutingSqsTaskTransportTest {

    record Sent(String queue, String body) {
        CloudEvent event() {
            return CeWire.fromJson(body.getBytes(StandardCharsets.UTF_8));
        }
    }

    private final List<Sent> sent = new CopyOnWriteArrayList<>();
    private final SqsClient sqs = (SqsClient) Proxy.newProxyInstance(getClass().getClassLoader(),
            new Class<?>[] {SqsClient.class}, (proxy, method, args) -> {
                if (method.getName().equals("sendMessage") && args[0] instanceof SendMessageRequest r) {
                    sent.add(new Sent(r.queueUrl(), r.messageBody()));
                    return SendMessageResponse.builder().messageId("m").build();
                }
                throw new UnsupportedOperationException(method.getName());
            });

    private static TaskInvocation invocation(String routingKey) {
        return new TaskInvocation("i-1", "/do/0", "c-1", new WorkflowRef("test", "wf", "1.0.0"), routingKey,
                Json.obj().put("x", 1), 1, Lineage.NONE, null);
    }

    private RoutingSqsTaskTransport.Builder builder() {
        return RoutingSqsTaskTransport.builder().sqs(sqs).resultQueueUrl("results");
    }

    @Test
    void a_routing_key_goes_to_its_exact_route_and_any_other_to_the_default() throws Exception {
        RoutingSqsTaskTransport transport = builder().route("redact", "q-privacy").route("register", "q-records")
                .defaultInvokeQueueUrl("q-own").build();
        transport.dispatch(invocation("redact")).toCompletableFuture().get();
        transport.dispatch(invocation("register")).toCompletableFuture().get();
        transport.dispatch(invocation("redact-more")).toCompletableFuture().get();   // not a prefix match

        assertThat(sent).extracting(Sent::queue).containsExactly("q-privacy", "q-records", "q-own");
    }

    @Test
    void an_unknown_key_without_a_default_fails_the_dispatch_with_the_known_keys() {
        RoutingSqsTaskTransport transport = builder().route("redact", "q-privacy").build();

        assertThatThrownBy(() -> transport.dispatch(invocation("nope")).toCompletableFuture().get())
                .isInstanceOf(ExecutionException.class).hasCauseInstanceOf(UnroutableTaskException.class)
                .hasMessageContaining("'nope'").hasMessageContaining("[redact]").hasMessageNotContaining("q-privacy");
        assertThat(sent).isEmpty();
    }

    @Test
    void unroutable_lists_the_keys_no_route_covers() {
        RoutingSqsTaskTransport strict = builder().route("redact", "q1").route("register", "q2").build();
        assertThat(strict.unroutable(List.of("redact", "decide", "audit"))).containsExactlyInAnyOrder("decide", "audit");
        assertThat(strict.unroutable(Set.of("redact"))).isEmpty();

        RoutingSqsTaskTransport withDefault = builder().route("redact", "q1").defaultInvokeQueueUrl("q-own").build();
        assertThat(withDefault.unroutable(List.of("anything"))).isEmpty();
    }

    @Test
    void the_reply_name_is_written_on_the_invoke_when_configured_and_only_then() throws Exception {
        builder().route("redact", "q").replyTo("business-results").build()
                .dispatch(invocation("redact")).toCompletableFuture().get();
        builder().route("redact", "q").build().dispatch(invocation("redact")).toCompletableFuture().get();

        assertThat(sent.get(0).event().getExtension(StintEvents.EXT_REPLY_TO)).isEqualTo("business-results");
        assertThat(sent.get(1).event().getExtension(StintEvents.EXT_REPLY_TO)).isNull();
    }

    @Test
    void an_invoke_without_a_reply_name_is_what_the_plain_transport_sends() throws Exception {
        new SqsTaskTransport(sqs, "q", "results").dispatch(invocation("redact")).toCompletableFuture().get();
        builder().route("redact", "q").build().dispatch(invocation("redact")).toCompletableFuture().get();

        CloudEvent plain = sent.get(0).event();
        CloudEvent routed = sent.get(1).event();
        assertThat(routed.getAttributeNames()).isEqualTo(plain.getAttributeNames());
        assertThat(routed.getExtensionNames()).isEqualTo(plain.getExtensionNames());
        assertThat(routed.getExtensionNames()).doesNotContain(StintEvents.EXT_REPLY_TO);
    }

    @Test
    void the_plain_transport_announces_a_configured_name_and_rejects_a_malformed_one() throws Exception {
        new SqsTaskTransport(sqs, "q", "results", "delivery-results").dispatch(invocation("redact"))
                .toCompletableFuture().get();
        assertThat(sent.get(0).event().getExtension(StintEvents.EXT_REPLY_TO)).isEqualTo("delivery-results");

        assertThatThrownBy(() -> new SqsTaskTransport(sqs, "q", "results", "https://elsewhere/q"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder().replyTo("has space")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void a_transport_needs_a_route_or_a_default_and_a_key_is_routed_once() {
        assertThatThrownBy(() -> builder().build()).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder().route("a", "q1").route("a", "q2"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
