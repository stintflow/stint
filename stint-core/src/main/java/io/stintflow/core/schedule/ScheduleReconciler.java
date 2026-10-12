package io.stintflow.core.schedule;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import io.stintflow.core.WorkflowRegistry;
import io.stintflow.spi.ScheduleService;
import io.stintflow.spi.ScheduleSpec;

/**
 * Keeps a {@link ScheduleService} in line with the registered definitions (SDD 2.4, sec. 8a).
 * <ul>
 *   <li>{@link #reconcile()}: create-only and idempotent. Every schedule the definitions want is ensured and
 *       nothing is removed. Every replica runs it at startup; concurrent runs converge (the service creates if
 *       absent) without a leader.</li>
 *   <li>{@link #prune()}: removes the schedules no registered definition wants. It never runs by itself — a
 *       replica stopping, crashing or starting with an older version must not delete anything — so it is an
 *       explicit deploy/CLI step, run with the definitions of the version being deployed.</li>
 * </ul>
 */
public final class ScheduleReconciler {

    private static final Logger LOG = System.getLogger(ScheduleReconciler.class.getName());

    private final WorkflowRegistry registry;
    private final ScheduleService schedules;

    public ScheduleReconciler(WorkflowRegistry registry, ScheduleService schedules) {
        this.registry = registry;
        this.schedules = schedules;
    }

    /** @return the names ensured */
    public CompletionStage<List<String>> reconcile() {
        List<ScheduleSpec> desired = registry.scheduleSpecs();
        CompletionStage<Void> chain = CompletableFuture.completedFuture(null);
        for (ScheduleSpec spec : desired) {
            chain = chain.thenCompose(v -> schedules.ensure(spec));
        }
        return chain.thenApply(v -> desired.stream().map(ScheduleSpec::name).toList());
    }

    /** @return the names removed */
    public CompletionStage<List<String>> prune() {
        Set<String> desired = registry.scheduleSpecs().stream().map(ScheduleSpec::name).collect(Collectors.toSet());
        return schedules.list().thenCompose(existing -> {
            List<String> orphans = existing.stream().filter(name -> !desired.contains(name)).toList();
            CompletionStage<Void> chain = CompletableFuture.completedFuture(null);
            for (String orphan : orphans) {
                chain = chain.thenCompose(v -> schedules.remove(orphan)).thenRun(() ->
                        LOG.log(Level.INFO, "Pruned schedule {0}: no registered definition wants it", orphan));
            }
            return chain.thenApply(v -> orphans);
        });
    }
}
