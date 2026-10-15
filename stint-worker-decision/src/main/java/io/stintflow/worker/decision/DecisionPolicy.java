package io.stintflow.worker.decision;

import java.util.Optional;

import io.stintflow.worker.TaskContext;

/**
 * The extension point of the sensitivity policy of SDD 3a.5 (SDD 3a.2, 4.7): consulted before every call to an
 * adapter, with the adapter's provider, model and whether the input leaves the process. A refusal fails the task with
 * an {@code authorization} error (403), without calling the adapter. The default allows everything.
 */
@FunctionalInterface
public interface DecisionPolicy {

    /** Why {@code adapter} may not decide this task, or empty. The reason goes into the error: no content in it. */
    Optional<String> refusal(TaskContext task, AdapterDescriptor adapter);

    DecisionPolicy ALLOW = (task, adapter) -> Optional.empty();
}
