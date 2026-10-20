package io.stintflow.worker.langchain4j;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.stintflow.worker.TaskContext;

/**
 * Where an execution's {@link LlmUsage} goes (SDD 3a.3, 5.6) — called once per execution, failed ones included. A
 * metrics registry is one more implementation; the default is a log line.
 */
@FunctionalInterface
public interface UsageSink {

    void record(TaskContext task, LlmUsage usage);

    /** An INFO line with the task's identifiers and the numbers; the identifiers are in the MDC too. */
    UsageSink LOG = new UsageSink() {
        private final Logger log = LoggerFactory.getLogger(UsageSink.class);

        @Override
        public void record(TaskContext task, LlmUsage usage) {
            log.info("LLM usage of task {} in instance {} (chain {}, attempt {}): model={}, calls={}, inputTokens={}, "
                            + "outputTokens={}, toolCalls={}, latencyMs={}, finishReason={}",
                    task.taskId(), task.instanceId(), task.chainId(), task.attempt(), usage.model(), usage.calls(),
                    usage.inputTokens(), usage.outputTokens(), usage.toolCalls(), usage.latency().toMillis(),
                    usage.finishReason());
        }
    };
}
