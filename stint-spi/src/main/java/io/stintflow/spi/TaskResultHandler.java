package io.stintflow.spi;

import java.util.concurrent.CompletionStage;

/**
 * Callback the orchestrator registers with a {@link TaskTransport} to be notified when results arrive.
 * The implementation looks up the suspended instance by correlation id and resumes it.
 */
@FunctionalInterface
public interface TaskResultHandler {
    CompletionStage<Void> handle(TaskResult result);
}
