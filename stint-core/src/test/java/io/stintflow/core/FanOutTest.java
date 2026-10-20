package io.stintflow.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.core.FanOut.Action;
import io.stintflow.core.FanOut.Step;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.model.FlowDirective;
import io.stintflow.core.model.ForRunNode;
import io.stintflow.wire.Json;

/** SDD 3a.8, 5.1: the join, over the JSON a parent keeps — pure, no engine. */
class FanOutTest {

    private static ForRunNode node(int concurrency, ForRunNode.Join join) {
        return new ForRunNode("fan", "/do/0/fan", DataFlow.NONE, FlowDirective.CONTINUE, Expr.jq(".items"), "item", "i",
                new ForRunNode.Run("ns", "child", "1.0.0", null), concurrency, join, 50);
    }

    private static ObjectNode open(int total, int concurrency, ForRunNode.Join join) {
        List<JsonNode> inputs = new java.util.ArrayList<>();
        for (int i = 0; i < total; i++) {
            inputs.add(Json.obj().put("n", i));
        }
        return FanOut.initial(node(concurrency, join), "p-1", inputs);
    }

    private static JsonNode out(int n) {
        return Json.obj().put("n", n * 10);
    }

    @Test
    void the_first_window_is_the_concurrency_and_a_child_ending_opens_the_next_slot() {
        ObjectNode state = open(8, 3, ForRunNode.Join.ALL);
        Step first = FanOut.open(state);

        assertThat(first.action()).isEqualTo(Action.WAIT);
        assertThat(first.start()).extracting(FanOut.Start::index).containsExactly(0, 1, 2);
        assertThat(first.start().get(1).id()).isEqualTo("p-1:/do/0/fan:1");
        assertThat(first.start().get(1).input().path("n").asInt()).isEqualTo(1);
        assertThat(FanOut.runningWaits(FanOut.withFor(null, state))).containsExactly("child:p-1:/do/0/fan:0",
                "child:p-1:/do/0/fan:1", "child:p-1:/do/0/fan:2");

        Step next = FanOut.settle(state, 1, FanOut.COMPLETED, out(1), null);
        assertThat(next.action()).isEqualTo(Action.WAIT);
        assertThat(next.start()).extracting(FanOut.Start::index).containsExactly(3);   // never more than 3 at once
    }

    @Test
    void all_puts_the_outputs_in_the_order_of_in_whatever_the_order_of_arrival() {
        ObjectNode state = open(3, 3, ForRunNode.Join.ALL);
        FanOut.open(state);

        assertThat(FanOut.settle(state, 2, FanOut.COMPLETED, out(2), null).action()).isEqualTo(Action.WAIT);
        assertThat(FanOut.settle(state, 0, FanOut.COMPLETED, out(0), null).action()).isEqualTo(Action.WAIT);
        Step done = FanOut.settle(state, 1, FanOut.COMPLETED, out(1), null);

        assertThat(done.action()).isEqualTo(Action.COMPLETE);
        assertThat(done.output()).extracting(n -> n.path("n").asInt()).containsExactly(0, 10, 20);
    }

    @Test
    void all_fails_on_the_first_failure_and_cancels_the_children_still_running() {
        ObjectNode state = open(5, 3, ForRunNode.Join.ALL);
        FanOut.open(state);

        Step failed = FanOut.settle(state, 1, FanOut.FAILED, null, "failed at /do/0/work");

        assertThat(failed.action()).isEqualTo(Action.FAIL);
        assertThat(failed.message()).contains("child 1 failed");
        assertThat(failed.cancel()).containsExactly("p-1:/do/0/fan:0", "p-1:/do/0/fan:2");
        assertThat(failed.start()).isEmpty();
    }

    @Test
    void a_cancelled_child_counts_as_a_failure_for_all() {
        ObjectNode state = open(2, 2, ForRunNode.Join.ALL);
        FanOut.open(state);

        assertThat(FanOut.settle(state, 0, FanOut.CANCELLED, null, "cancelled").action()).isEqualTo(Action.FAIL);
    }

    @Test
    void any_ends_on_the_first_success_and_cancels_the_others() {
        ObjectNode state = open(4, 2, ForRunNode.Join.ANY);
        FanOut.open(state);

        Step won = FanOut.settle(state, 1, FanOut.COMPLETED, out(1), null);

        assertThat(won.action()).isEqualTo(Action.COMPLETE);
        assertThat(won.output().path("index").asInt()).isEqualTo(1);
        assertThat(won.output().path("output").path("n").asInt()).isEqualTo(10);
        assertThat(won.cancel()).containsExactly("p-1:/do/0/fan:0");
    }

    @Test
    void any_keeps_going_after_a_failure_and_fails_only_when_every_child_failed() {
        ObjectNode state = open(3, 1, ForRunNode.Join.ANY);
        FanOut.open(state);

        Step one = FanOut.settle(state, 0, FanOut.FAILED, null, "x");
        assertThat(one.action()).isEqualTo(Action.WAIT);
        assertThat(one.start()).extracting(FanOut.Start::index).containsExactly(1);
        Step two = FanOut.settle(state, 1, FanOut.FAILED, null, "x");
        assertThat(two.start()).extracting(FanOut.Start::index).containsExactly(2);
        Step three = FanOut.settle(state, 2, FanOut.FAILED, null, "last");

        assertThat(three.action()).isEqualTo(Action.FAIL);
        assertThat(three.message()).contains("all 3 children failed");
    }

    @Test
    void collect_waits_for_every_child_and_never_fails_the_parent() {
        ObjectNode state = open(3, 3, ForRunNode.Join.COLLECT);
        FanOut.open(state);

        FanOut.settle(state, 2, FanOut.FAILED, null, "boom");
        FanOut.settle(state, 0, FanOut.COMPLETED, out(0), null);
        Step done = FanOut.settle(state, 1, FanOut.CANCELLED, null, "cancelled");

        assertThat(done.action()).isEqualTo(Action.COMPLETE);
        assertThat(done.output()).extracting(e -> e.path("status").asText())
                .containsExactly("COMPLETED", "CANCELLED", "FAILED");
        assertThat(done.output().get(0).path("output").path("n").asInt()).isEqualTo(0);
        assertThat(done.output().get(2).path("error").asText()).isEqualTo("boom");
    }

    @Test
    void a_redelivered_end_counts_once() {
        ObjectNode state = open(2, 2, ForRunNode.Join.ALL);
        FanOut.open(state);
        FanOut.settle(state, 0, FanOut.COMPLETED, out(0), null);

        Step again = FanOut.settle(state, 0, FanOut.COMPLETED, out(0), null);

        assertThat(again.action()).isEqualTo(Action.WAIT);
        assertThat(again.start()).isEmpty();
        assertThat(state.path("settled").asInt()).isEqualTo(1);
    }

    @Test
    void a_for_over_no_items_completes_with_an_empty_array_and_any_over_none_fails() {
        assertThat(FanOut.open(open(0, 1, ForRunNode.Join.ALL)).output().isArray()).isTrue();
        assertThat(FanOut.open(open(0, 1, ForRunNode.Join.ALL)).action()).isEqualTo(Action.COMPLETE);
        assertThat(FanOut.open(open(0, 1, ForRunNode.Join.ANY)).action()).isEqualTo(Action.FAIL);
    }

    @Test
    void the_composition_keeps_the_parent_link_when_the_for_is_over() {
        JsonNode link = FanOut.withParent("grand", "/do/0/fan", 4);
        JsonNode both = FanOut.withFor(link, open(1, 1, ForRunNode.Join.ALL));

        assertThat(FanOut.parentOf(both).path("id").asText()).isEqualTo("grand");
        assertThat(FanOut.forOf(both)).isNotNull();
        assertThat(FanOut.forOf(FanOut.withoutFor(both))).isNull();
        assertThat(FanOut.parentOf(FanOut.withoutFor(both)).path("index").asInt()).isEqualTo(4);
        assertThat(FanOut.withoutFor(FanOut.withFor(null, open(1, 1, ForRunNode.Join.ALL)))).isNull();
    }
}
