package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.core.CancelOutcome;
import io.stintflow.dsl.DslValidationException;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.OutboxEntry;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;

/**
 * SDD 3a.8: a {@code for} over {@code run: workflow} starts a child instance per item, at most N at once; the join
 * ({@code all}, {@code any}, {@code collect}) decides the output, in the order of {@code in}; what stops waiting for a child
 * cancels it, down to its own children. Barriers and the controllable clock — no sleep.
 */
class ForRunTest {

    static final WorkflowRef FAN = new WorkflowRef("fan", "fan", "1.0.0");

    private static FanOutFixture fixture(int concurrency, String join, int maxIterations) throws Exception {
        return new FanOutFixture(FanOutFixture.leaf("leaf"), FanOutFixture.parent("fan", "leaf", concurrency, join,
                maxIterations));
    }

    @Test
    void ca1_runs_at_most_n_children_at_once_and_the_output_follows_the_order_of_in() throws Exception {
        FanOutFixture f = fixture(3, "all", 50);
        String id = f.engine.start(FAN, FanOutFixture.items(8));

        f.awaitInvocation(f.childId(id, 2));
        assertThat(f.dispatched).hasSize(3);   // the first window, no more
        int maxInFlight = 0;
        List<Integer> order = List.of(2, 0, 1, 5, 4, 3, 7, 6);   // not the order of in
        for (int index : order) {
            f.awaitInvocation(f.childId(id, index));
            maxInFlight = Math.max(maxInFlight, f.dispatched.size() - f.answered.size());
            f.complete(f.childId(id, index));
        }

        assertThat(maxInFlight).isLessThanOrEqualTo(3);
        JsonNode done = f.awaitCompleted(id);
        assertThat(done.path("results")).extracting(r -> r.path("n").asInt()).containsExactly(0, 10, 20, 30, 40, 50, 60, 70);
        for (int i = 0; i < 8; i++) {
            assertThat(f.snapshot(f.childId(id, i)).status()).isEqualTo(InstanceStatus.COMPLETED);
        }
    }

    @Test
    void ca1_in_series_by_default_a_child_at_a_time() throws Exception {
        FanOutFixture f = new FanOutFixture(FanOutFixture.leaf("leaf"), """
                document: { dsl: '1.0.0', namespace: fan, name: fan, version: '1.0.0' }
                do:
                  - fan:
                      for: { each: n, in: '${ .items }' }
                      do:
                        - run: { run: { workflow: { namespace: fan, name: leaf, version: '1.0.0', input: '${ {n: $n} }' } } }
                      export: { as: '${ {results: .} }' }
                """);
        String id = f.engine.start(FAN, FanOutFixture.items(3));

        for (int i = 0; i < 3; i++) {
            f.awaitInvocation(f.childId(id, i));
            assertThat(f.dispatched).hasSize(i + 1);
            f.complete(f.childId(id, i));
        }
        assertThat(f.awaitCompleted(id).path("results")).hasSize(3);
    }

    @Test
    void ca2_all_fails_on_the_first_failed_child_and_cancels_the_ones_still_running() throws Exception {
        FanOutFixture f = fixture(4, "all", 50);
        String id = f.engine.start(FAN, FanOutFixture.items(6));
        f.awaitInvocation(f.childId(id, 3));

        f.complete(f.childId(id, 0));
        f.fail(f.childId(id, 1));

        assertThat(f.awaitTerminal(id)).isEqualTo(InstanceStatus.FAILED);
        for (int i : new int[] {2, 3, 4}) {   // 4 had been started in the slot child 0 left
            assertThat(f.awaitTerminal(f.childId(id, i))).isEqualTo(InstanceStatus.ABORTED);
        }
        assertThat(f.find(f.childId(id, 5))).isEmpty();   // never started
        assertThat(f.snapshot(f.childId(id, 1)).status()).isEqualTo(InstanceStatus.FAILED);
    }

    @Test
    void ca2_any_ends_on_the_first_success_cancels_the_others_and_the_parent_goes_on() throws Exception {
        FanOutFixture f = fixture(3, "any", 50);
        String id = f.engine.start(FAN, FanOutFixture.items(3));
        f.awaitInvocation(f.childId(id, 2));

        f.fail(f.childId(id, 0));
        f.complete(f.childId(id, 2));

        JsonNode done = f.awaitCompleted(id);
        assertThat(done.path("results").path("index").asInt()).isEqualTo(2);
        assertThat(done.path("results").path("output").path("n").asInt()).isEqualTo(20);
        assertThat(f.awaitTerminal(f.childId(id, 1))).isEqualTo(InstanceStatus.ABORTED);
    }

    @Test
    void ca2_any_fails_only_when_every_child_failed() throws Exception {
        FanOutFixture f = fixture(2, "any", 50);
        String id = f.engine.start(FAN, FanOutFixture.items(2));
        f.awaitInvocation(f.childId(id, 1));

        f.fail(f.childId(id, 0));
        assertThat(f.snapshot(id).status()).isEqualTo(InstanceStatus.WAITING);
        f.fail(f.childId(id, 1));

        assertThat(f.awaitTerminal(id)).isEqualTo(InstanceStatus.FAILED);
    }

    @Test
    void ca2_collect_waits_for_every_child_whatever_happens_to_it() throws Exception {
        FanOutFixture f = fixture(3, "collect", 50);
        String id = f.engine.start(FAN, FanOutFixture.items(3));
        f.awaitInvocation(f.childId(id, 2));

        f.fail(f.childId(id, 1));
        assertThat(f.engine.cancel(f.childId(id, 2), "an operator").toCompletableFuture().get(10, TimeUnit.SECONDS))
                .isEqualTo(CancelOutcome.CANCELLED);
        f.complete(f.childId(id, 0));

        JsonNode results = f.awaitCompleted(id).path("results");
        assertThat(results).extracting(r -> r.path("status").asText()).containsExactly("COMPLETED", "FAILED", "CANCELLED");
        assertThat(results.get(0).path("output").path("n").asInt()).isEqualTo(0);
        assertThat(results.get(1).path("error").asText()).startsWith("failed at");
    }

    @Test
    void ca3_more_items_than_the_limit_fail_the_for_before_any_child_starts() throws Exception {
        FanOutFixture f = fixture(2, "all", 3);
        String id = f.engine.start(FAN, FanOutFixture.items(5));

        assertThat(f.awaitTerminal(id)).isEqualTo(InstanceStatus.FAILED);
        assertThat(f.dispatched).isEmpty();
        assertThat(f.find(f.childId(id, 0))).isEmpty();
        assertThat(f.store.outbox).isEmpty();

        FanOutFixture capped = new FanOutFixture(FanOutFixture.leaf("leaf"),
                FanOutFixture.parent("fan", "leaf", 1, "all", 50));
        capped.engine.forMaxIterations(2);
        String other = capped.engine.start(FAN, FanOutFixture.items(3));
        assertThat(capped.awaitTerminal(other)).isEqualTo(InstanceStatus.FAILED);   // the engine's ceiling wins
        assertThat(capped.dispatched).isEmpty();
    }

    @Test
    void a_for_over_an_empty_array_completes_with_an_empty_output() throws Exception {
        FanOutFixture f = fixture(2, "all", 50);
        String id = f.engine.start(FAN, FanOutFixture.items(0));

        assertThat(f.awaitCompleted(id).path("results")).isEmpty();
        assertThat(f.dispatched).isEmpty();
    }

    @Test
    void a_restricted_instance_is_refused_a_for_nothing_of_its_children_rests_in_its_item() throws Exception {
        FanOutFixture f = fixture(2, "all", 50);
        String id = f.engine.start(FAN, FanOutFixture.items(2), Sensitivity.RESTRICTED);

        assertThat(f.awaitTerminal(id)).isEqualTo(InstanceStatus.FAILED);
        assertThat(f.dispatched).isEmpty();
    }

    @Test
    void ca4_twenty_children_ending_at_the_same_instant_do_not_fail_the_parent_for_a_conflict() throws Exception {
        for (int round = 0; round < 3; round++) {
            FanOutFixture f = fixture(20, "all", 50);
            String id = f.engine.start(FAN, FanOutFixture.items(20));
            for (int i = 0; i < 20; i++) {
                f.awaitInvocation(f.childId(id, i));
            }
            CyclicBarrier go = new CyclicBarrier(20);
            List<CompletableFuture<Void>> ends = new ArrayList<>();
            java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(20);
            for (int i = 0; i < 20; i++) {
                String child = f.childId(id, i);
                ends.add(CompletableFuture.runAsync(() -> {
                    try {
                        go.await(10, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                    f.complete(child);
                }, pool));
            }
            for (CompletableFuture<Void> end : ends) {
                end.get(30, TimeUnit.SECONDS);
            }
            pool.shutdown();

            JsonNode done = f.awaitCompleted(id);
            assertThat(done.path("results")).hasSize(20);
            assertThat(done.path("results").get(19).path("n").asInt()).isEqualTo(190);
        }
    }

    @Test
    void ca5_a_redelivered_start_or_end_neither_duplicates_a_child_nor_counts_twice() throws Exception {
        FanOutFixture f = fixture(2, "all", 50);
        String id = f.engine.start(FAN, FanOutFixture.items(2));
        f.awaitInvocation(f.childId(id, 1));
        List<OutboxEntry> starts = f.store.outbox.stream().filter(e -> e.eventId().startsWith("start:")).toList();
        assertThat(starts).hasSize(2);

        // the starts are delivered again: nothing more is dispatched, the children stay as they are
        f.engine.outboxRelay().publishNow(starts).toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertThat(f.dispatched).hasSize(2);

        f.complete(f.childId(id, 0));
        f.complete(f.childId(id, 1));
        JsonNode done = f.awaitCompleted(id);
        long version = f.snapshot(id).version();
        List<OutboxEntry> finished = f.store.outbox.stream().filter(e -> e.eventId().startsWith("finished:")).toList();
        assertThat(finished).hasSize(2);

        // the ends are delivered again: the parent is not touched
        f.engine.outboxRelay().publishNow(finished).toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertThat(f.snapshot(id).version()).isEqualTo(version);
        assertThat(f.snapshot(id).context()).isEqualTo(done);
    }

    @Test
    void ca6_cancelling_the_parent_reaches_every_level() throws Exception {
        String mid = """
                document: { dsl: '1.0.0', namespace: fan, name: mid, version: '1.0.0' }
                do:
                  - fan:
                      for: { each: n, in: '${ [1, 2] }', at: i }
                      do:
                        - run: { run: { workflow: { namespace: fan, name: leaf, version: '1.0.0', input: '${ {n: $n, i: $i} }' } } }
                      metadata: { x-stint-concurrency: 2 }
                      export: { as: '${ {results: .} }' }
                """;
        FanOutFixture f = new FanOutFixture(FanOutFixture.leaf("leaf"), mid,
                FanOutFixture.parent("fan", "mid", 5, "all", 50));
        String top = f.engine.start(FAN, FanOutFixture.items(3));
        String[] mids = {f.childId(top, 0), f.childId(top, 1), f.childId(top, 2)};
        List<String> leaves = new ArrayList<>();
        for (String m : mids) {
            for (int i = 0; i < 2; i++) {
                leaves.add(m + ":/do/0/fan:" + i);
            }
        }
        for (String leaf : leaves) {
            f.awaitInvocation(leaf);
        }
        // mid 0 has ended before the cancel: both its leaves completed
        f.complete(leaves.get(0));
        f.complete(leaves.get(1));
        assertThat(f.awaitTerminal(mids[0])).isEqualTo(InstanceStatus.COMPLETED);

        assertThat(f.engine.cancel(top, "the batch was withdrawn").toCompletableFuture().get(10, TimeUnit.SECONDS))
                .isEqualTo(CancelOutcome.CANCELLED);

        assertThat(f.awaitTerminal(top)).isEqualTo(InstanceStatus.ABORTED);
        assertThat(f.snapshot(mids[0]).status()).isEqualTo(InstanceStatus.COMPLETED);   // already ended: left alone
        assertThat(f.awaitTerminal(mids[1])).isEqualTo(InstanceStatus.ABORTED);
        assertThat(f.awaitTerminal(mids[2])).isEqualTo(InstanceStatus.ABORTED);
        for (String leaf : leaves.subList(2, 6)) {
            assertThat(f.awaitTerminal(leaf)).isEqualTo(InstanceStatus.ABORTED);
        }
    }

    @Test
    void ca8_the_cascade_survives_a_failure_between_the_parents_save_and_the_cancel_of_its_children() throws Exception {
        FanOutFixture f = fixture(3, "all", 50);
        String id = f.engine.start(FAN, FanOutFixture.items(3));
        f.awaitInvocation(f.childId(id, 2));

        // the store is down for the children while the parent is cancelled: the parent's save (with the cancel
        // entries in it) lands, the cancelling of the children does not
        f.store.failLoadsOf = instance -> instance.startsWith(id + ":");
        assertThat(f.engine.cancel(id, "stop").toCompletableFuture().get(10, TimeUnit.SECONDS))
                .isEqualTo(CancelOutcome.CANCELLED);
        assertThat(f.snapshot(id).status()).isEqualTo(InstanceStatus.ABORTED);
        for (int i = 0; i < 3; i++) {
            assertThat(f.store.delegate.load(f.childId(id, i)).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow()
                    .status()).isEqualTo(InstanceStatus.WAITING);   // not cancelled yet
        }

        f.store.failLoadsOf = instance -> false;   // back up; the sweep finds the entries still in the outbox
        f.now.set(f.now.get().plus(Duration.ofMinutes(1)));
        assertThat(f.engine.outboxRelay().sweep().toCompletableFuture().get(10, TimeUnit.SECONDS))
                .isGreaterThanOrEqualTo(3);
        for (int i = 0; i < 3; i++) {
            assertThat(f.snapshot(f.childId(id, i)).status()).isEqualTo(InstanceStatus.ABORTED);
        }
    }

    @Test
    void the_definition_is_checked_when_it_is_loaded() {
        String head = "document: { dsl: '1.0.0', namespace: fan, name: bad, version: '1.0.0' }\ndo:\n";
        String body = """
                  - fan:
                      for: { each: n, in: '${ .items }' }
                      do:
                        - run: { run: { workflow: { namespace: fan, name: leaf, version: '1.0.0' } } }
                """;
        FanOutFixture.load(head + body);   // valid

        assertThatThrownBy(() -> FanOutFixture.load(head + body + "      metadata: { x-stint-on-failure: collect }\n"))
                .isInstanceOf(DslValidationException.class).hasMessageContaining("failFast");
        assertThatThrownBy(() -> FanOutFixture.load(head + body + "      metadata: { x-stint-parallel: 3 }\n"))
                .isInstanceOf(DslValidationException.class).hasMessageContaining("x-stint-parallel");
        assertThatThrownBy(() -> FanOutFixture.load(head + body + "      metadata: { x-stint-concurrency: 0 }\n"))
                .isInstanceOf(DslValidationException.class).hasMessageContaining("from 1 to");
        assertThatThrownBy(() -> FanOutFixture.load(head + body + "      metadata: { x-stint-join: some }\n"))
                .isInstanceOf(DslValidationException.class).hasMessageContaining("not a join");
        assertThatThrownBy(() -> FanOutFixture.load(head + """
                  - fan:
                      for: { each: n, in: '${ .items }' }
                      do:
                        - a: { set: '${ . }' }
                """)).isInstanceOf(DslValidationException.class).hasMessageContaining("run: workflow");
        assertThatThrownBy(() -> FanOutFixture.load(head + """
                  - fan:
                      for: { each: n, in: '${ .items }' }
                      do:
                        - a: { run: { workflow: { namespace: fan, name: leaf, version: '1.0.0' } } }
                        - b: { run: { workflow: { namespace: fan, name: leaf, version: '1.0.0' } } }
                """)).isInstanceOf(DslValidationException.class).hasMessageContaining("exactly one task");
    }
}
