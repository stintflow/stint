package io.stintflow.inmemory;

import static io.stintflow.inmemory.ListenFixture.REVIEWED;
import static io.stintflow.inmemory.ListenFixture.byInstance;
import static io.stintflow.inmemory.ListenFixture.event;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.cloudevents.CloudEvent;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.model.FlowDirective;
import io.stintflow.core.model.ListenNode;
import io.stintflow.core.model.TryNode;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;

/**
 * SDD 2.3, RF3/sec. 8c — an event that arrives before its wait:
 * <ol>
 *   <li>emit → listen: the wait and the request fact are saved together and the fact is published only
 *       after, so even a synchronous answer finds the wait (no inbox involved);</li>
 *   <li>otherwise the event is kept in the inbox for the window and consumed when the listen suspends —
 *       but not past the window;</li>
 *   <li>arrival and suspension racing ("write, then check" on both sides) resume exactly once.</li>
 * </ol>
 */
class ListenEarlyEventTest {

    private static final WorkflowRef REF = new WorkflowRef("review", "request-review", "1.0.0");
    private static final String REQUESTED = "io.acme.review.requested.v1";
    private static final DataFlow EXPORT_OUTPUT = new DataFlow(null, null, Expr.jq("."));

    /** work → listen (with a caught 1-minute timeout) → next. */
    private static WorkflowDefinition workThenListen() {
        return WorkflowBuilder.create()
                .callRemote("work", "route-work", DataFlow.NONE)
                .tryListen("guarded", DataFlow.NONE, "awaitReview", ListenNode.Strategy.ONE,
                        List.of(byInstance(REVIEWED)), null, Duration.ofMinutes(1), DataFlow.NONE,
                        new TryNode.Catch(null, null, null, null, Expr.jq("{timedOut: true}"), FlowDirective.CONTINUE),
                        FlowDirective.CONTINUE)
                .set("outcome", Expr.jq("."), EXPORT_OUTPUT)
                .build(REF);
    }

    @Test
    void emit_then_listen_an_immediate_answer_finds_the_wait() throws Exception {
        ListenFixture f = new ListenFixture(WorkflowBuilder.create()
                .emit("ask", REQUESTED, Expr.jq("{source: \"https://acme.example/wf\", type: \"" + REQUESTED
                        + "\", data: {instanceId: $workflow.id}}"), DataFlow.NONE)
                .listen("awaitReview", ListenNode.Strategy.ONE, List.of(byInstance(REVIEWED)), null, null,
                        EXPORT_OUTPUT)
                .build(REF));
        // The reviewer answers synchronously, inside the publish of the request.
        f.publisher.subscribe(request -> {
            String instanceId = Json.read(request.getData().toBytes()).get("instanceId").asText();
            return f.bus.publish(event(REVIEWED, "rev-1", instanceId, "instant"));
        });

        String a = f.engine.start(REF, Json.obj());

        assertThat(f.awaitCompleted(a).get(0).get("note").asText()).isEqualTo("instant");
        assertThat(f.inbox(ListenFixture.keyFor(REVIEWED, a))).isEmpty(); // it found the wait, not the inbox
    }

    @Test
    void an_early_event_within_the_window_is_consumed_when_the_listen_suspends() throws Exception {
        ListenFixture f = new ListenFixture(workThenListen());
        String a = f.engine.start(REF, Json.obj());
        TaskInvocation work = f.transport.awaitDispatch();

        f.publish(event(REVIEWED, "rev-early", a, "early")); // the instance is still at 'work'
        assertThat(f.inbox(ListenFixture.keyFor(REVIEWED, a))).hasSize(1);

        f.clock.advance(Duration.ofMinutes(4)); // within the 5-minute window
        f.transport.deliverResult(TaskResult.completed(work.correlationId(), Json.obj()))
                .toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertThat(f.awaitCompleted(a).get(0).get("note").asText()).isEqualTo("early");
        assertThat(f.inbox(ListenFixture.keyFor(REVIEWED, a))).isEmpty(); // removed by the save that consumed it
    }

    @Test
    void an_early_event_past_the_window_is_not_consumed() throws Exception {
        ListenFixture f = new ListenFixture(workThenListen());
        String a = f.engine.start(REF, Json.obj());
        TaskInvocation work = f.transport.awaitDispatch();
        f.publish(event(REVIEWED, "rev-early", a, "too early"));

        f.clock.advance(Duration.ofMinutes(6)); // past the 5-minute window
        f.transport.deliverResult(TaskResult.completed(work.correlationId(), Json.obj()))
                .toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertThat(f.load(a).status()).isEqualTo(InstanceStatus.WAITING);

        f.clock.advance(Duration.ofMinutes(2));
        f.timer.tick();
        assertThat(f.awaitCompleted(a).get("timedOut").asBoolean()).isTrue();
    }

    @Test
    void arrival_and_suspension_racing_resume_exactly_once() throws Exception {
        BarrierStateStore state = new BarrierStateStore();
        ListenFixture f = new ListenFixture(state, WorkflowBuilder.create()
                .callRemote("work", "route-work", DataFlow.NONE)
                .listen("awaitReview", ListenNode.Strategy.ONE, List.of(byInstance(REVIEWED)), null, null,
                        DataFlow.NONE)
                .callRemote("next", "route-next", DataFlow.NONE)
                .build(REF));
        String a = f.engine.start(REF, Json.obj());
        TaskInvocation work = f.transport.awaitDispatch();
        CloudEvent early = event(REVIEWED, "rev-1", a, "raced");

        // The arrival puts the event in the inbox and the listen saves its wait; neither checks until both
        // have written — so each sees the other and both try to resume.
        state.holdAfter(waits -> waits.stream().anyMatch(w -> w.waitKey().startsWith("event:")));
        CompletableFuture<Void> arrival = CompletableFuture.runAsync(() -> {
            try {
                f.publish(early);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
        state.inboxWritten().get(10, TimeUnit.SECONDS); // the arrival found no wait and wrote first
        CompletableFuture<Void> suspension = f.transport.deliverResult(
                TaskResult.completed(work.correlationId(), Json.obj())).toCompletableFuture();
        CompletableFuture.allOf(arrival, suspension).get(15, TimeUnit.SECONDS);

        TaskInvocation next = f.transport.awaitDispatch();
        assertThat(next.taskId()).isEqualTo("/do/2/next");
        assertThat(f.transport.dispatchCount()).isEqualTo(2); // 'work' and 'next', once each
        assertThat(f.load(a).status()).isEqualTo(InstanceStatus.WAITING);
    }
}
