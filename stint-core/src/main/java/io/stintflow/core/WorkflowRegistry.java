package io.stintflow.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import io.stintflow.core.model.CallRemoteNode;
import io.stintflow.core.model.DoNode;
import io.stintflow.core.model.EmitNode;
import io.stintflow.core.model.ListenNode;
import io.stintflow.core.model.TaskNode;
import io.stintflow.core.model.TryNode;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.spi.wire.StintEvents;

/**
 * In-process registry of known workflow definitions, keyed by {@code namespace:name:version}.
 * <p>
 * SDD 1.3, RF3/sec. 8d: validates every {@code timeout.after} against the timer connector's
 * {@link io.stintflow.spi.TimerService#maxDelay()} at registration time — a static property of the
 * definition, so it's checked once here rather than on every dispatch.
 * <p>
 * SDD 2.2, sec. 8e: an {@code emit} whose literal {@code type} belongs to the engine's internal protocol
 * ({@link StintEvents#RESERVED_TYPE_PREFIXES}) is rejected here too — covering definitions built in
 * Java, not only the ones loaded from YAML.
 * <p>
 * SDD 2.3, sec. 8a: registering a definition also indexes the correlation shape of every {@code listen}
 * filter by event type ({@link #correlationShapes}) — static, derived from the definitions alone, so every
 * engine that registers the same definitions has the same index. The subset of {@code listen} this phase
 * supports is enforced here as well (sec. 8f).
 */
public final class WorkflowRegistry {

    private final Map<String, WorkflowDefinition> byRef = new ConcurrentHashMap<>();
    private final Duration maxTimerDelay;
    /** SDD 2.3: event type → shape id → shape. */
    private final Map<String, Map<String, EventCorrelation.Shape>> shapesByType = new ConcurrentHashMap<>();

    /** No delay limit enforced — for tests/examples that don't care (matches pre-SDD-1.3 behaviour). */
    public WorkflowRegistry() {
        this(Duration.ofDays(365));
    }

    public WorkflowRegistry(Duration maxTimerDelay) {
        this.maxTimerDelay = maxTimerDelay;
    }

    /**
     * SDD 1.4, sec. 8f: registering the same {@link WorkflowRef} twice with structurally equal
     * content (e.g. a classpath reload) is an idempotent no-op — in-flight instances never see a
     * version's definition change. Registering it again with different content is rejected: bump
     * {@code document.version} instead of silently replacing a version instances may be running.
     */
    public void register(WorkflowDefinition def) {
        validateTimeouts(def.root());
        validateEmitTypes(def.root());
        List<ListenNode> listens = new ArrayList<>();
        collectListens(def.root(), listens);
        for (ListenNode listen : listens) {
            validateListen(listen);
        }
        WorkflowDefinition existing = byRef.putIfAbsent(def.ref().canonical(), def);
        if (existing != null && !existing.equals(def)) {
            throw new IllegalStateException("Workflow " + def.ref().canonical()
                    + " is already registered with different content; bump document.version instead of "
                    + "re-registering the same version with a changed definition.");
        }
        for (ListenNode listen : listens) {
            for (ListenNode.Filter filter : listen.filters()) {
                EventCorrelation.Shape shape = EventCorrelation.shapeOf(filter);
                shapesByType.computeIfAbsent(filter.type(), t -> new ConcurrentHashMap<>()).putIfAbsent(shape.id(), shape);
            }
        }
    }

    /** SDD 2.3, sec. 8a: the correlation shapes some registered {@code listen} waits on for {@code eventType}. */
    public List<EventCorrelation.Shape> correlationShapes(String eventType) {
        Map<String, EventCorrelation.Shape> shapes = shapesByType.get(eventType);
        return shapes == null ? List.of() : List.copyOf(shapes.values());
    }

    public Optional<WorkflowDefinition> find(WorkflowRef ref) {
        return Optional.ofNullable(byRef.get(ref.canonical()));
    }

    private void validateTimeouts(TaskNode node) {
        if (node instanceof CallRemoteNode remote && remote.timeout() != null
                && remote.timeout().compareTo(maxTimerDelay) > 0) {
            throw new IllegalArgumentException("Task '" + remote.name() + "' at " + remote.pointer()
                    + " has timeout.after=" + remote.timeout() + " exceeding the timer's max delay " + maxTimerDelay);
        }
        if (node instanceof ListenNode listen && listen.timeout() != null
                && listen.timeout().compareTo(maxTimerDelay) > 0) {
            throw new IllegalArgumentException("Listen '" + listen.name() + "' at " + listen.pointer()
                    + " has timeout.after=" + listen.timeout() + " exceeding the timer's max delay " + maxTimerDelay);
        }
        if (node instanceof TryNode tryNode) {
            validateTimeouts(tryNode.task());
            if (tryNode.listen() != null && tryNode.catchClause().retry() != null) {
                throw new IllegalArgumentException("Try '" + tryNode.name() + "' at " + tryNode.pointer()
                        + ": retry around a listen is not supported (SDD 2.3, sec. 5)");
            }
        }
        if (node instanceof DoNode doNode) {
            for (TaskNode child : doNode.tasks()) {
                validateTimeouts(child);
            }
        }
    }

    private static void collectListens(TaskNode node, List<ListenNode> into) {
        if (node instanceof ListenNode listen) {
            into.add(listen);
        } else if (node instanceof TryNode tryNode) {
            collectListens(tryNode.task(), into);
        } else if (node instanceof DoNode doNode) {
            for (TaskNode child : doNode.tasks()) {
                collectListens(child, into);
            }
        }
    }

    /** SDD 2.3, sec. 8a/8f: the {@code listen} subset this phase supports. */
    private static void validateListen(ListenNode listen) {
        String where = "Listen '" + listen.name() + "' at " + listen.pointer() + ": ";
        if (listen.filters().isEmpty()) {
            throw new IllegalArgumentException(where + "needs at least one event filter "
                    + "(listening to every event, 'any: []', is not supported)");
        }
        if (listen.strategy() == ListenNode.Strategy.ONE && listen.filters().size() != 1) {
            throw new IllegalArgumentException(where + "'one' takes exactly one event filter");
        }
        for (ListenNode.Filter filter : listen.filters()) {
            if (filter.type() == null || filter.type().isBlank()) {
                throw new IllegalArgumentException(where + "every event filter needs a 'type'");
            }
            if (filter.correlate().isEmpty()) {
                throw new IllegalArgumentException(where + "the filter on '" + filter.type()
                        + "' needs 'correlate' — an uncorrelated listen is not supported (SDD 2.3, sec. 8a)");
            }
            for (Map.Entry<String, ListenNode.Correlation> c : filter.correlate().entrySet()) {
                if (c.getValue().from() == null || c.getValue().expect() == null) {
                    throw new IllegalArgumentException(where + "correlate." + c.getKey()
                            + " needs both 'from' and 'expect' (SDD 2.3, sec. 8a)");
                }
            }
            try {
                EventCorrelation.shapeOf(filter);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(where + e.getMessage(), e);
            }
        }
    }

    private void validateEmitTypes(TaskNode node) {
        if (node instanceof EmitNode emit && StintEvents.isReservedType(emit.declaredType())) {
            throw new IllegalArgumentException("Emit '" + emit.name() + "' at " + emit.pointer() + " uses type '"
                    + emit.declaredType() + "', reserved for the engine's internal protocol "
                    + StintEvents.RESERVED_TYPE_PREFIXES);
        }
        if (node instanceof DoNode doNode) {
            for (TaskNode child : doNode.tasks()) {
                validateEmitTypes(child);
            }
        }
    }
}
