package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.stintflow.spi.Lineage;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TimerRequest;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.DefaultCloudEventCodec;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerRuntime;

/**
 * SDD 3a.3c, CA2: every dispatch carries its attempt's deadline — the instant its timeout timer fires — on the first
 * attempt and on a retry, and the worker runtime hands it to the handler.
 */
class DeadlineDispatchTest {

    private static Instant fireAtOf(DecisionFixture f, TaskInvocation inv) {
        return f.armed.stream().filter(t -> t.timerId().equals("timer:" + inv.correlationId()))
                .map(TimerRequest::fireAt).findFirst().orElseThrow();
    }

    @Test
    void the_deadline_is_the_attempt_s_timeout_timer_on_every_attempt() throws Exception {
        DecisionFixture f = new DecisionFixture(DecisionReexecutionTest.GUARDED).manual();
        String id = f.start("item: aprovado");
        TaskInvocation attempt1 = f.awaitDispatch();
        f.awaitSaved(s -> s.instanceId().equals(id) && ("task:" + attempt1.correlationId()).equals(s.waitingKey()));

        f.fireTimer("timer:" + attempt1.correlationId());           // attempt 1 times out, at T0 + 30 s
        var retrying = f.awaitSaved(s -> s.instanceId().equals(id) && s.waitingKey() != null
                && s.waitingKey().startsWith("retry:"));
        f.fireTimer(retrying.waitingKey());                         // the retry delay, 1 s later
        TaskInvocation attempt2 = f.awaitDispatch();

        assertThat(attempt1.deadline()).isEqualTo(DecisionFixture.T0.plusSeconds(30)).isEqualTo(fireAtOf(f, attempt1));
        assertThat(attempt2.attempt()).isEqualTo(2);
        assertThat(attempt2.deadline()).isEqualTo(DecisionFixture.T0.plusSeconds(61)).isEqualTo(fireAtOf(f, attempt2));
    }

    @Test
    void the_worker_runtime_hands_the_deadline_to_the_handler() throws Exception {
        AtomicReference<Instant> seen = new AtomicReference<>();
        WorkerRuntime worker = new WorkerRuntime(new TaskHandlerRegistry().register("ask", ctx -> {
            seen.set(ctx.deadline());
            return CompletableFuture.completedFuture(Json.obj());
        }), null);
        DefaultCloudEventCodec codec = new DefaultCloudEventCodec();
        Instant deadline = Instant.parse("2026-10-12T12:05:00Z");
        TaskInvocation inv = new TaskInvocation("i-1", "/do/0/ask", "c-1",
                new WorkflowRef("ai", "notes", "1.0.0"), "ask", Json.obj(), 1,
                Lineage.NONE, deadline);

        worker.handle(codec.toEvent(inv)).toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertThat(seen.get()).isEqualTo(deadline);
    }
}
