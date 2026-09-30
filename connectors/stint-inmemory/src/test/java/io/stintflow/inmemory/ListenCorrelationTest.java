package io.stintflow.inmemory;

import static io.stintflow.inmemory.ListenFixture.REVIEWED;
import static io.stintflow.inmemory.ListenFixture.byInstance;
import static io.stintflow.inmemory.ListenFixture.event;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.model.ListenNode;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.spi.wire.StintEvents;
import io.stintflow.wire.Json;

/**
 * SDD 2.3: CA1 (a correlated event resumes its instance; another instance's event doesn't affect it), RF6
 * (the resume is caused by the event and stays on the instance's chain), a duplicate never resumes twice
 * and {@code listen.read} (sec. 8e).
 */
class ListenCorrelationTest {

    private static final WorkflowRef REF = new WorkflowRef("review", "await-review", "1.0.0");
    /** export.as "." — the listen's output (the array of consumed events) becomes $context. */
    private static final DataFlow EXPORT_OUTPUT = new DataFlow(null, null, Expr.jq("."));

    private static WorkflowDefinition awaitReview(ListenNode.Read read) {
        return WorkflowBuilder.create()
                .listen("awaitReview", ListenNode.Strategy.ONE, List.of(byInstance(REVIEWED)), read, null, EXPORT_OUTPUT)
                .build(REF);
    }

    @Test
    void ca1_the_correlated_event_resumes_its_instance_and_another_instances_event_does_not() throws Exception {
        ListenFixture f = new ListenFixture(awaitReview(null));
        String a = f.engine.start(REF, Json.obj());
        String b = f.engine.start(REF, Json.obj());
        assertThat(f.load(a).status()).isEqualTo(InstanceStatus.WAITING);

        f.publish(event(REVIEWED, "rev-a", a, "approved"));

        JsonNode output = f.awaitCompleted(a);
        assertThat(output).hasSize(1);
        assertThat(output.get(0).get("note").asText()).isEqualTo("approved");
        assertThat(output.get(0).get("instanceId").asText()).isEqualTo(a);
        // B was never touched: still waiting, same version, its wait intact.
        assertThat(f.load(b).status()).isEqualTo(InstanceStatus.WAITING);
        assertThat(f.load(b).version()).isEqualTo(1);
        assertThat(f.waits(ListenFixture.keyFor(REVIEWED, b))).isTrue();
    }

    @Test
    void ca1_an_event_for_an_unknown_correlation_value_resumes_nothing() throws Exception {
        ListenFixture f = new ListenFixture(awaitReview(null));
        String a = f.engine.start(REF, Json.obj());

        f.publish(event(REVIEWED, "rev-x", "some-other-instance", "approved"));

        assertThat(f.load(a).status()).isEqualTo(InstanceStatus.WAITING);
        assertThat(f.load(a).version()).isEqualTo(1);
    }

    @Test
    void the_resume_is_caused_by_the_event_and_keeps_the_instance_chain() throws Exception {
        ListenFixture f = new ListenFixture(WorkflowBuilder.create()
                .listen("awaitReview", ListenNode.Strategy.ONE, List.of(byInstance(REVIEWED)), null, null, DataFlow.NONE)
                .callRemote("archive", "route-archive", DataFlow.NONE)
                .build(REF));
        String a = f.engine.start(REF, Json.obj());
        String chain = f.load(a).chainId();
        f.publish(CloudEventBuilder.v1(event(REVIEWED, "rev-1", a, "ok"))
                .withExtension(StintEvents.EXT_CHAIN_ID, "chn-of-the-reviewer").build());

        TaskInvocation archive = f.transport.awaitDispatch();
        assertThat(archive.lineage().causationId()).isEqualTo("rev-1");   // the event is the cause...
        assertThat(archive.lineage().chainId()).isEqualTo(chain)          // ...but the chain stays the instance's
                .isNotEqualTo("chn-of-the-reviewer");
    }

    @Test
    void a_duplicate_event_does_not_resume_twice() throws Exception {
        ListenFixture f = new ListenFixture(WorkflowBuilder.create()
                .listen("awaitReview", ListenNode.Strategy.ONE, List.of(byInstance(REVIEWED)), null, null, DataFlow.NONE)
                .callRemote("archive", "route-archive", DataFlow.NONE)
                .build(REF));
        String a = f.engine.start(REF, Json.obj());
        CloudEvent reviewed = event(REVIEWED, "rev-1", a, "ok");

        f.publish(reviewed);
        f.publish(reviewed); // at-least-once redelivery

        assertThat(f.transport.dispatchCount()).isEqualTo(1);
        assertThat(f.load(a).status()).isEqualTo(InstanceStatus.WAITING); // now at 'archive'
        assertThat(f.load(a).version()).isEqualTo(2);
    }

    @Test
    void a_late_duplicate_kept_in_the_inbox_does_not_satisfy_a_new_wait_with_the_same_key() throws Exception {
        // Two listens in a row on the same type and correlation: the second must wait for a new event.
        ListenFixture f = new ListenFixture(WorkflowBuilder.create()
                .listen("first", ListenNode.Strategy.ONE, List.of(byInstance(REVIEWED)), null, null, DataFlow.NONE)
                .callRemote("work", "route-work", DataFlow.NONE)
                .listen("second", ListenNode.Strategy.ONE, List.of(byInstance(REVIEWED)), null, null, EXPORT_OUTPUT)
                .build(REF));
        String a = f.engine.start(REF, Json.obj());
        CloudEvent first = event(REVIEWED, "rev-1", a, "first");
        f.publish(first);
        f.publish(first); // redelivered while the instance is at 'work': no wait, so it is kept in the inbox
        assertThat(f.inbox(ListenFixture.keyFor(REVIEWED, a))).hasSize(1);

        TaskInvocation work = f.transport.awaitDispatch();
        f.transport.deliverResult(TaskResult.completed(work.correlationId(), Json.obj()))
                .toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertThat(f.load(a).status()).isEqualTo(InstanceStatus.WAITING); // 'second' ignored the duplicate
        f.publish(event(REVIEWED, "rev-2", a, "second"));
        assertThat(f.awaitCompleted(a).get(0).get("note").asText()).isEqualTo("second");
    }

    @Test
    void read_envelope_gives_the_whole_event() throws Exception {
        ListenFixture f = new ListenFixture(awaitReview(ListenNode.Read.ENVELOPE));
        String a = f.engine.start(REF, Json.obj());

        f.publish(event(REVIEWED, "rev-e", a, "ok"));

        JsonNode read = f.awaitCompleted(a).get(0);
        assertThat(read.get("id").asText()).isEqualTo("rev-e");
        assertThat(read.get("type").asText()).isEqualTo(REVIEWED);
        assertThat(read.get("data").get("note").asText()).isEqualTo("ok");
    }

    @Test
    void read_raw_keeps_non_json_data_as_base64() throws Exception {
        ListenNode.Filter bySubject = new ListenNode.Filter(REVIEWED, java.util.Map.of(), new java.util.TreeMap<>(
                java.util.Map.of("instance", new ListenNode.Correlation(Expr.jq(".subject"), Expr.jq("$workflow.id")))));
        ListenFixture f = new ListenFixture(WorkflowBuilder.create()
                .listen("awaitReview", ListenNode.Strategy.ONE, List.of(bySubject), ListenNode.Read.RAW, null,
                        EXPORT_OUTPUT)
                .build(REF));
        String a = f.engine.start(REF, Json.obj());

        f.publish(CloudEventBuilder.v1().withId("rev-bin").withSource(URI.create("https://acme.example/r"))
                .withType(REVIEWED).withSubject(a).withDataContentType("application/octet-stream")
                .withData(new byte[] {1, 2, 3}).build());

        assertThat(f.awaitCompleted(a).get(0).asText()).isEqualTo("AQID");
    }

    @Test
    void a_second_instance_waiting_on_the_same_key_fails_with_correlation_conflict() throws Exception {
        ListenNode.Filter byOrder = new ListenNode.Filter(REVIEWED, java.util.Map.of(), new java.util.TreeMap<>(
                java.util.Map.of("order", new ListenNode.Correlation(Expr.jq(".data.orderId"), Expr.jq("\"A-1\"")))));
        ListenFixture f = new ListenFixture(WorkflowBuilder.create()
                .listen("awaitOrder", ListenNode.Strategy.ONE, List.of(byOrder), null, null, DataFlow.NONE)
                .build(REF));

        String first = f.engine.start(REF, Json.obj());
        String second = f.engine.start(REF, Json.obj());

        assertThat(f.load(first).status()).isEqualTo(InstanceStatus.WAITING);
        assertThat(f.load(second).status()).isEqualTo(InstanceStatus.FAILED); // not retried forever
    }

    @Test
    void an_event_whose_other_with_attributes_differ_does_not_resume() throws Exception {
        ListenNode.Filter fromBilling = new ListenNode.Filter(REVIEWED,
                java.util.Map.of("source", "https://acme.example/billing"), byInstance(REVIEWED).correlate());
        ListenFixture f = new ListenFixture(WorkflowBuilder.create()
                .listen("awaitReview", ListenNode.Strategy.ONE, List.of(fromBilling), null, null, EXPORT_OUTPUT)
                .build(REF));
        String a = f.engine.start(REF, Json.obj());

        f.publish(event(REVIEWED, "rev-wrong-source", a, "ignored")); // source is .../review

        assertThat(f.load(a).status()).isEqualTo(InstanceStatus.WAITING);
        f.publish(CloudEventBuilder.v1(event(REVIEWED, "rev-billing", a, "taken"))
                .withSource(URI.create("https://acme.example/billing")).build());
        assertThat(f.awaitCompleted(a).get(0).get("note").asText()).isEqualTo("taken");
    }
}
