package io.stintflow.inmemory;

import static io.stintflow.inmemory.ListenFixture.APPROVED;
import static io.stintflow.inmemory.ListenFixture.REVIEWED;
import static io.stintflow.inmemory.ListenFixture.byInstance;
import static io.stintflow.inmemory.ListenFixture.event;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.model.ListenNode;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;

/**
 * SDD 2.3: CA2 ({@code all} resumes only once every event arrived — also when two arrive at the same time,
 * neither partial is lost) and {@code any} ending the other waits (sec. 8b). The output is the consumed
 * events in consumption order.
 */
class ListenAllTest {

    private static final WorkflowRef REF = new WorkflowRef("billing", "await-both", "1.0.0");
    private static final DataFlow EXPORT_OUTPUT = new DataFlow(null, null, Expr.jq("."));

    private static WorkflowDefinition awaitBoth(ListenNode.Strategy strategy) {
        return WorkflowBuilder.create()
                .listen("awaitBoth", strategy, List.of(byInstance(REVIEWED), byInstance(APPROVED)), null, null,
                        EXPORT_OUTPUT)
                .build(REF);
    }

    @Test
    void ca2_all_resumes_only_when_every_event_arrived() throws Exception {
        ListenFixture f = new ListenFixture(awaitBoth(ListenNode.Strategy.ALL));
        String a = f.engine.start(REF, Json.obj());

        f.publish(event(APPROVED, "apr-1", a, "approved"));

        assertThat(f.load(a).status()).isEqualTo(InstanceStatus.WAITING);
        assertThat(f.waits(ListenFixture.keyFor(APPROVED, a))).isFalse(); // its own key is consumed...
        assertThat(f.waits(ListenFixture.keyFor(REVIEWED, a))).isTrue();  // ...the other one still waits

        f.publish(event(REVIEWED, "rev-1", a, "reviewed"));

        JsonNode output = f.awaitCompleted(a);
        // Consumption order (DSL 1.0: "a sequentially ordered array of all the events it has consumed"),
        // not declaration order.
        assertThat(output).extracting(e -> e.get("note").asText()).containsExactly("approved", "reviewed");
    }

    @Test
    void ca2_two_simultaneous_arrivals_both_count() throws Exception {
        BarrierStateStore state = new BarrierStateStore();
        ListenFixture f = new ListenFixture(state, awaitBoth(ListenNode.Strategy.ALL));
        String a = f.engine.start(REF, Json.obj());

        // Both arrivals load the same snapshot version before either saves: one save wins, the other gets a
        // CONFLICT, reloads, sees the winner's partial and completes the listen.
        state.holdLoads();
        CompletableFuture<Void> reviewed = CompletableFuture.runAsync(() -> publish(f, event(REVIEWED, "rev-1", a, "r")));
        CompletableFuture<Void> approved = CompletableFuture.runAsync(() -> publish(f, event(APPROVED, "apr-1", a, "p")));
        CompletableFuture.allOf(reviewed, approved).get(15, TimeUnit.SECONDS);

        JsonNode output = f.awaitCompleted(a);
        assertThat(output).extracting(e -> e.get("note").asText()).containsExactlyInAnyOrder("r", "p");
        assertThat(f.waits(ListenFixture.keyFor(REVIEWED, a))).isFalse();
        assertThat(f.waits(ListenFixture.keyFor(APPROVED, a))).isFalse();
    }

    @Test
    void any_resumes_on_the_first_event_and_ends_the_other_waits() throws Exception {
        ListenFixture f = new ListenFixture(awaitBoth(ListenNode.Strategy.ANY));
        String a = f.engine.start(REF, Json.obj());

        f.publish(event(APPROVED, "apr-1", a, "approved"));

        JsonNode output = f.awaitCompleted(a);
        assertThat(output).extracting(e -> e.get("note").asText()).containsExactly("approved");
        assertThat(f.waits(ListenFixture.keyFor(REVIEWED, a))).isFalse(); // consumed with the first event
        long version = f.load(a).version();

        f.publish(event(REVIEWED, "rev-1", a, "late"));

        assertThat(f.load(a).version()).isEqualTo(version); // the late event changes nothing
    }

    private static void publish(ListenFixture f, io.cloudevents.CloudEvent event) {
        try {
            f.publish(event);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
