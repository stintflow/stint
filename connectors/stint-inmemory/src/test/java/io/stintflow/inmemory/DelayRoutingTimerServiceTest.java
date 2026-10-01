package io.stintflow.inmemory;

import static io.stintflow.inmemory.ListenFixture.REVIEWED;
import static io.stintflow.inmemory.ListenFixture.byInstance;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.stintflow.core.DelayRoutingTimerService;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.model.FlowDirective;
import io.stintflow.core.model.ListenNode;
import io.stintflow.core.model.TryNode;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.TimerFireHandler;
import io.stintflow.spi.TimerRequest;
import io.stintflow.spi.TimerService;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;

/**
 * SDD 2.4, CA2 (sec. 8d): the composite timer sends a delay up to the short connector's max (SQS: 15 min) to
 * it and anything longer to the long one (EventBridge Scheduler), and its max is the long one's — so the
 * SDD 2.3 {@code listen} of 48 h registers and times out through the long path.
 */
class DelayRoutingTimerServiceTest {

    private static final WorkflowRef REF = new WorkflowRef("review", "await-48h", "1.0.0");

    @Test
    void ca2_a_two_hour_timer_goes_to_the_long_connector_and_a_one_minute_one_to_the_short() throws Exception {
        EmitFixture.MutableClock clock = new EmitFixture.MutableClock();
        Recording sqsLike = new Recording(new InMemoryTimerService(clock), Duration.ofMinutes(15));
        Recording schedulerLike = new Recording(new InMemoryTimerService(clock), Duration.ofDays(365));
        DelayRoutingTimerService timers = new DelayRoutingTimerService(sqsLike, schedulerLike, clock);
        Instant now = clock.instant();

        timers.schedule(new TimerRequest("timer:short", "i-1", "c-1", now.plus(Duration.ofMinutes(1))))
                .toCompletableFuture().get();
        timers.schedule(new TimerRequest("timer:edge", "i-1", "c-2", now.plus(Duration.ofMinutes(15))))
                .toCompletableFuture().get();
        timers.schedule(new TimerRequest("timer:long", "i-1", "c-3", now.plus(Duration.ofHours(2))))
                .toCompletableFuture().get();

        assertThat(sqsLike.scheduled).containsExactly("timer:short", "timer:edge");
        assertThat(schedulerLike.scheduled).containsExactly("timer:long");
        assertThat(timers.maxDelay()).isEqualTo(Duration.ofDays(365));
    }

    @Test
    void ca2_a_48_hour_listen_registers_and_times_out_through_the_long_path() throws Exception {
        EmitFixture.MutableClock clock = new EmitFixture.MutableClock();
        InMemoryTimerService sqsTimers = new InMemoryTimerService(clock);
        InMemoryTimerService schedulerTimers = new InMemoryTimerService(clock);
        Recording sqsLike = new Recording(sqsTimers, Duration.ofMinutes(15));
        Recording schedulerLike = new Recording(schedulerTimers, Duration.ofDays(365));
        DelayRoutingTimerService timers = new DelayRoutingTimerService(sqsLike, schedulerLike, clock);
        // The registry validates against the composite's max delay: 48 h is accepted (SDD 2.3 left it to 2.4).
        WorkflowRegistry registry = new WorkflowRegistry(timers.maxDelay());
        registry.register(WorkflowBuilder.create()
                .tryListen("guarded", DataFlow.NONE, "awaitReview", ListenNode.Strategy.ONE,
                        List.of(byInstance(REVIEWED)), null, Duration.ofHours(48), DataFlow.NONE,
                        new TryNode.Catch(null, null, null, null, Expr.jq("{timedOut: true}"), FlowDirective.CONTINUE),
                        FlowDirective.CONTINUE)
                .set("outcome", Expr.jq("."), new DataFlow(null, null, Expr.jq(".")))
                .build(REF));
        InMemoryStateStore state = new InMemoryStateStore();
        WorkflowEngine engine = new WorkflowEngine(registry, new DomainEventFixture.CapturingTransport(),
                state, timers, new FilesystemBlobStore(Files.createTempDirectory("stint-sdd24")),
                new TreeInterpreter(new JqExpressionEvaluator()), clock, null, null);

        String id = engine.start(REF, Json.obj());
        assertThat(schedulerLike.scheduled).hasSize(1);
        assertThat(sqsLike.scheduled).isEmpty();

        clock.advance(Duration.ofHours(47));
        schedulerTimers.tick();
        assertThat(state.load(id).toCompletableFuture().get().orElseThrow().status())
                .isEqualTo(InstanceStatus.WAITING); // 47 h: still waiting
        clock.advance(Duration.ofHours(2));
        schedulerTimers.tick();

        assertThat(engine.awaitCompletion(id, Duration.ofSeconds(10)).toCompletableFuture().get(15, TimeUnit.SECONDS)
                .get("timedOut").asBoolean()).isTrue();
    }

    @Test
    void cancel_reaches_both_connectors() throws Exception {
        EmitFixture.MutableClock clock = new EmitFixture.MutableClock();
        Recording sqsLike = new Recording(new InMemoryTimerService(clock), Duration.ofMinutes(15));
        Recording schedulerLike = new Recording(new InMemoryTimerService(clock), Duration.ofDays(365));

        new DelayRoutingTimerService(sqsLike, schedulerLike, clock).cancel("timer:x").toCompletableFuture().get();

        assertThat(sqsLike.cancelled).containsExactly("timer:x");
        assertThat(schedulerLike.cancelled).containsExactly("timer:x");
    }

    /** A timer service with a given max delay, recording what was scheduled and cancelled. */
    private static final class Recording implements TimerService {
        private final InMemoryTimerService delegate;
        private final Duration maxDelay;
        final List<String> scheduled = new CopyOnWriteArrayList<>();
        final List<String> cancelled = new CopyOnWriteArrayList<>();

        Recording(InMemoryTimerService delegate, Duration maxDelay) {
            this.delegate = delegate;
            this.maxDelay = maxDelay;
        }

        @Override
        public CompletionStage<String> schedule(TimerRequest req) {
            scheduled.add(req.timerId());
            return delegate.schedule(req);
        }

        @Override
        public CompletionStage<Void> cancel(String timerId) {
            cancelled.add(timerId);
            return delegate.cancel(timerId);
        }

        @Override
        public void onFire(TimerFireHandler handler) {
            delegate.onFire(handler);
        }

        @Override
        public Duration maxDelay() {
            return maxDelay;
        }
    }
}
