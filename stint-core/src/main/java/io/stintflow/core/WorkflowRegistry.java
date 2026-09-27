package io.stintflow.core;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import io.stintflow.core.model.CallRemoteNode;
import io.stintflow.core.model.DoNode;
import io.stintflow.core.model.TaskNode;
import io.stintflow.core.model.TryNode;
import io.stintflow.spi.WorkflowRef;

/**
 * In-process registry of known workflow definitions, keyed by {@code namespace:name:version}.
 * <p>
 * SDD 1.3, RF3/sec. 8d: validates every {@code timeout.after} against the timer connector's
 * {@link io.stintflow.spi.TimerService#maxDelay()} at registration time — a static property of the
 * definition, so it's checked once here rather than on every dispatch.
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

    public void register(WorkflowDefinition def) {
        validateTimeouts(def.root());
        byRef.put(def.ref().canonical(), def);
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
}
