package io.stintflow.core;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import io.stintflow.core.model.CallRemoteNode;
import io.stintflow.core.model.DoNode;
import io.stintflow.core.model.EmitNode;
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
 */
public final class WorkflowRegistry {

    private final Map<String, WorkflowDefinition> byRef = new ConcurrentHashMap<>();
    private final Duration maxTimerDelay;

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
        WorkflowDefinition existing = byRef.putIfAbsent(def.ref().canonical(), def);
        if (existing != null && !existing.equals(def)) {
            throw new IllegalStateException("Workflow " + def.ref().canonical()
                    + " is already registered with different content; bump document.version instead of "
                    + "re-registering the same version with a changed definition.");
        }
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
        if (node instanceof TryNode tryNode) {
            validateTimeouts(tryNode.body());
        }
        if (node instanceof DoNode doNode) {
            for (TaskNode child : doNode.tasks()) {
                validateTimeouts(child);
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
