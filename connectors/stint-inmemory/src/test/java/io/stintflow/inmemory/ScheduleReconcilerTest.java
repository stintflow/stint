package io.stintflow.inmemory;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.model.Schedule;
import io.stintflow.core.schedule.ScheduleReconciler;
import io.stintflow.spi.CronExpression;
import io.stintflow.spi.ScheduleFireHandler;
import io.stintflow.spi.ScheduleService;
import io.stintflow.spi.ScheduleSpec;
import io.stintflow.spi.WorkflowRef;

/**
 * SDD 2.4, CA1 (sec. 8a): reconciliation is create-only and idempotent — two replicas reconciling at the same
 * time create each schedule once; a replica stopping, or starting with an older set of definitions, removes
 * nothing; only an explicit {@code prune()} removes, and only what no definition wants.
 */
class ScheduleReconcilerTest {

    static final WorkflowRef DAILY = new WorkflowRef("reports", "daily", "1.0.0");
    static final WorkflowRef WEEKLY = new WorkflowRef("reports", "weekly", "1.0.0");

    static WorkflowDefinition scheduled(WorkflowRef ref, String cron) {
        return WorkflowBuilder.create()
                .set("noop", Expr.jq("."), DataFlow.NONE)
                .schedule(Schedule.cron(CronExpression.parse(cron), ZoneId.of("UTC")))
                .build(ref);
    }

    private static WorkflowRegistry registryWith(WorkflowDefinition... defs) {
        WorkflowRegistry registry = new WorkflowRegistry();
        for (WorkflowDefinition def : defs) {
            registry.register(def);
        }
        return registry;
    }

    @Test
    void ca1_two_replicas_reconciling_at_once_create_each_schedule_once() throws Exception {
        InMemoryScheduleService shared = new InMemoryScheduleService(new EmitFixture.MutableClock());
        CyclicBarrier bothInside = new CyclicBarrier(2);
        // Both replicas reach the first ensure before either has created anything.
        ScheduleService replicaA = new MeetingFirst(shared, bothInside);
        ScheduleService replicaB = new MeetingFirst(shared, bothInside);
        WorkflowDefinition daily = scheduled(DAILY, "0 7 * * *");
        WorkflowDefinition weekly = scheduled(WEEKLY, "0 9 * * MON");

        CompletableFuture<List<String>> a = CompletableFuture.supplyAsync(() ->
                new ScheduleReconciler(registryWith(daily, weekly), replicaA).reconcile().toCompletableFuture().join());
        CompletableFuture<List<String>> b = CompletableFuture.supplyAsync(() ->
                new ScheduleReconciler(registryWith(daily, weekly), replicaB).reconcile().toCompletableFuture().join());

        assertThat(a.get(10, TimeUnit.SECONDS)).hasSize(2).isEqualTo(b.get(10, TimeUnit.SECONDS));
        assertThat(shared.creations()).isEqualTo(2);
        assertThat(shared.list().toCompletableFuture().get()).hasSize(2);
    }

    @Test
    void ca1_a_replica_stopping_or_running_an_older_version_removes_nothing() throws Exception {
        InMemoryScheduleService schedules = new InMemoryScheduleService(new EmitFixture.MutableClock());
        new ScheduleReconciler(registryWith(scheduled(DAILY, "0 7 * * *"), scheduled(WEEKLY, "0 9 * * MON")),
                schedules).reconcile().toCompletableFuture().get();

        // A replica of an older version only knows 'daily': reconciling never removes 'weekly'.
        new ScheduleReconciler(registryWith(scheduled(DAILY, "0 7 * * *")), schedules).reconcile()
                .toCompletableFuture().get();

        assertThat(schedules.list().toCompletableFuture().get()).hasSize(2);
    }

    @Test
    void ca1_only_an_explicit_prune_removes_and_only_what_no_definition_wants() throws Exception {
        InMemoryScheduleService schedules = new InMemoryScheduleService(new EmitFixture.MutableClock());
        WorkflowRegistry before = registryWith(scheduled(DAILY, "0 7 * * *"), scheduled(WEEKLY, "0 9 * * MON"));
        new ScheduleReconciler(before, schedules).reconcile().toCompletableFuture().get();
        String weeklyName = before.scheduleSpecs().stream().filter(s -> s.definition().equals(WEEKLY))
                .findFirst().orElseThrow().name();
        // The deployed version: 'weekly' is gone and 'daily' moved to 08:00 (a new, content-addressed name).
        WorkflowRegistry deployed = registryWith(scheduled(DAILY, "0 8 * * *"));
        ScheduleReconciler reconciler = new ScheduleReconciler(deployed, schedules);
        reconciler.reconcile().toCompletableFuture().get();
        assertThat(schedules.list().toCompletableFuture().get()).hasSize(3); // nothing removed yet

        List<String> pruned = reconciler.prune().toCompletableFuture().get();

        assertThat(pruned).hasSize(2).contains(weeklyName);
        assertThat(schedules.list().toCompletableFuture().get())
                .containsExactly(deployed.scheduleSpecs().get(0).name());
    }

    @Test
    void schedule_names_are_content_addressed_and_valid_aws_names() {
        WorkflowRegistry registry = registryWith(scheduled(DAILY, "0 7 * * *"));
        String name = registry.scheduleSpecs().get(0).name();
        assertThat(name).matches("stint-[0-9a-f]{40}").hasSizeLessThanOrEqualTo(64);
        assertThat(registryWith(scheduled(DAILY, "0 7 * * *")).scheduleSpecs().get(0).name()).isEqualTo(name);
        assertThat(registryWith(scheduled(DAILY, "0 8 * * *")).scheduleSpecs().get(0).name()).isNotEqualTo(name);
    }

    /** Delegates; the first {@code ensure} of each replica waits for the other replica's first one. */
    private static final class MeetingFirst implements ScheduleService {
        private final ScheduleService delegate;
        private final CyclicBarrier barrier;
        private volatile boolean met;

        MeetingFirst(ScheduleService delegate, CyclicBarrier barrier) {
            this.delegate = delegate;
            this.barrier = barrier;
        }

        @Override
        public CompletionStage<Void> ensure(ScheduleSpec spec) {
            if (!met) {
                met = true;
                try {
                    barrier.await(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
            return delegate.ensure(spec);
        }

        @Override
        public CompletionStage<List<String>> list() {
            return delegate.list();
        }

        @Override
        public CompletionStage<Void> remove(String name) {
            return delegate.remove(name);
        }

        @Override
        public void onFire(ScheduleFireHandler handler) {
            delegate.onFire(handler);
        }
    }
}
