package io.stintflow.inmemory;

import static io.stintflow.inmemory.ListenFixture.REVIEWED;
import static io.stintflow.inmemory.ListenFixture.byInstance;
import static io.stintflow.inmemory.ListenFixture.event;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.model.FlowDirective;
import io.stintflow.core.model.ListenNode;
import io.stintflow.core.model.RetryPolicy;
import io.stintflow.core.model.TryNode;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;

/**
 * SDD 2.3: CA3 (a {@code listen} timeout fires on the controllable clock and is handled by {@code catch}),
 * event × timeout with exactly one winner (barrier), and the registration rules of sec. 8d/8f.
 */
class ListenTimeoutTest {

    private static final WorkflowRef REF = new WorkflowRef("review", "await-or-escalate", "1.0.0");
    private static final DataFlow EXPORT_OUTPUT = new DataFlow(null, null, Expr.jq("."));
    /** Catches only timeouts; the compensation's result flows on as the data. */
    private static final TryNode.Catch ON_TIMEOUT = new TryNode.Catch(
            Expr.jq(".type == \"stint://errors/timeout\""), null, null, null,
            Expr.jq("{timedOut: true}"), FlowDirective.CONTINUE);

    private static WorkflowDefinition awaitOrEscalate() {
        return WorkflowBuilder.create()
                .tryListen("guarded", DataFlow.NONE, "awaitReview", ListenNode.Strategy.ONE,
                        List.of(byInstance(REVIEWED)), null, Duration.ofMinutes(10), DataFlow.NONE, ON_TIMEOUT,
                        FlowDirective.CONTINUE)
                .set("outcome", Expr.jq("."), EXPORT_OUTPUT)
                .build(REF);
    }

    @Test
    void ca3_the_listen_timeout_fires_and_is_handled_by_catch() throws Exception {
        ListenFixture f = new ListenFixture(awaitOrEscalate());
        String a = f.engine.start(REF, Json.obj());

        f.clock.advance(Duration.ofMinutes(9));
        f.timer.tick();
        assertThat(f.load(a).status()).isEqualTo(InstanceStatus.WAITING); // not yet

        f.clock.advance(Duration.ofMinutes(2));
        f.timer.tick();

        assertThat(f.awaitCompleted(a).get("timedOut").asBoolean()).isTrue();
        assertThat(f.waits(ListenFixture.keyFor(REVIEWED, a))).isFalse(); // the timeout consumed the event wait
    }

    @Test
    void an_event_before_the_timeout_completes_the_try_normally() throws Exception {
        ListenFixture f = new ListenFixture(awaitOrEscalate());
        String a = f.engine.start(REF, Json.obj());

        f.publish(event(REVIEWED, "rev-1", a, "on time"));

        JsonNode outcome = f.awaitCompleted(a);
        assertThat(outcome.get(0).get("note").asText()).isEqualTo("on time");
        f.clock.advance(Duration.ofMinutes(11));
        f.timer.tick(); // cancelled — and even if it fired, it would find no wait
        assertThat(f.load(a).status()).isEqualTo(InstanceStatus.COMPLETED);
    }

    @Test
    void an_uncaught_listen_timeout_fails_the_instance() throws Exception {
        ListenFixture f = new ListenFixture(WorkflowBuilder.create()
                .listen("awaitReview", ListenNode.Strategy.ONE, List.of(byInstance(REVIEWED)), null,
                        Duration.ofMinutes(1), DataFlow.NONE)
                .build(REF));
        String a = f.engine.start(REF, Json.obj());

        f.clock.advance(Duration.ofMinutes(2));
        f.timer.fireAndWait(f.timerKeyOf(a), a);

        assertThat(f.load(a).status()).isEqualTo(InstanceStatus.FAILED);
    }

    @Test
    void event_versus_timeout_exactly_one_wins() throws Exception {
        BarrierStateStore state = new BarrierStateStore();
        ListenFixture f = new ListenFixture(state, WorkflowBuilder.create()
                .tryListen("guarded", DataFlow.NONE, "awaitReview", ListenNode.Strategy.ONE,
                        List.of(byInstance(REVIEWED)), null, Duration.ofMinutes(10), DataFlow.NONE, ON_TIMEOUT,
                        FlowDirective.CONTINUE)
                .callRemote("next", "route-next", DataFlow.NONE)
                .build(REF));
        String a = f.engine.start(REF, Json.obj());
        String timerKey = f.timerKeyOf(a);
        f.clock.advance(Duration.ofMinutes(11));

        // The event and the timeout both load the same waiting snapshot before either saves.
        state.holdLoads();
        CompletableFuture<Void> byEvent = CompletableFuture.runAsync(() -> run(() ->
                f.publish(event(REVIEWED, "rev-1", a, "just in time"))));
        CompletableFuture<Void> byTimer = CompletableFuture.runAsync(() -> run(() -> f.timer.fireAndWait(timerKey, a)));
        CompletableFuture.allOf(byEvent, byTimer).get(15, TimeUnit.SECONDS);

        assertThat(f.transport.dispatchCount()).isEqualTo(1); // 'next' dispatched once: one winner, one no-op
        assertThat(f.load(a).version()).isEqualTo(2);
        assertThat(f.load(a).status()).isEqualTo(InstanceStatus.WAITING);
    }

    @Test
    void registration_rejects_a_listen_timeout_above_the_timer_max_delay() {
        WorkflowRegistry registry = new WorkflowRegistry(Duration.ofMinutes(15));
        WorkflowDefinition fortyEightHours = WorkflowBuilder.create()
                .listen("awaitReview", ListenNode.Strategy.ONE, List.of(byInstance(REVIEWED)), null,
                        Duration.ofHours(48), DataFlow.NONE)
                .build(REF);

        assertThatThrownBy(() -> registry.register(fortyEightHours))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("timeout.after=PT48H")
                .hasMessageContaining("max delay");
    }

    @Test
    void registration_rejects_what_this_phase_does_not_support() {
        WorkflowRegistry registry = new WorkflowRegistry();
        ListenNode.Filter uncorrelated = new ListenNode.Filter(REVIEWED, Map.of(), new TreeMap<>());
        ListenNode.Filter noExpect = new ListenNode.Filter(REVIEWED, Map.of(), new TreeMap<>(Map.of("instance",
                new ListenNode.Correlation(Expr.jq(".data.instanceId"), null))));
        ListenNode.Filter lambdaFrom = new ListenNode.Filter(REVIEWED, Map.of(), new TreeMap<>(Map.of("instance",
                new ListenNode.Correlation(Expr.of((ctx, data) -> data), Expr.jq("$workflow.id")))));

        assertThatThrownBy(() -> registry.register(listenOn(List.of(uncorrelated))))
                .hasMessageContaining("needs 'correlate'");
        assertThatThrownBy(() -> registry.register(listenOn(List.of(noExpect))))
                .hasMessageContaining("needs both 'from' and 'expect'");
        assertThatThrownBy(() -> registry.register(listenOn(List.of(lambdaFrom))))
                .hasMessageContaining("must be a runtime expression");
        assertThatThrownBy(() -> registry.register(listenOn(List.of())))
                .hasMessageContaining("at least one event filter");
        assertThatThrownBy(() -> registry.register(WorkflowBuilder.create()
                .tryListen("guarded", DataFlow.NONE, "awaitReview", ListenNode.Strategy.ONE,
                        List.of(byInstance(REVIEWED)), null, Duration.ofMinutes(1), DataFlow.NONE,
                        WorkflowBuilder.catchAnyWithRetry(new RetryPolicy(Duration.ofSeconds(1), RetryPolicy.Backoff.CONSTANT, 3, null, 0.0), null,
                                FlowDirective.CONTINUE),
                        FlowDirective.CONTINUE)
                .build(REF)))
                .hasMessageContaining("retry around a listen is not supported");
    }

    private static WorkflowDefinition listenOn(List<ListenNode.Filter> filters) {
        return WorkflowBuilder.create()
                .listen("awaitReview", ListenNode.Strategy.ANY, filters, null, null, DataFlow.NONE)
                .build(REF);
    }

    private interface Step {
        void run() throws Exception;
    }

    private static void run(Step step) {
        try {
            step.run();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
