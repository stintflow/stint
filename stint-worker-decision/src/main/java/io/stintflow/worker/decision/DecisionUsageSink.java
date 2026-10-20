package io.stintflow.worker.decision;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.stintflow.worker.TaskContext;

/** Where each decision's usage goes (SDD 3a.2, 4.6). The default is a log line with the task's identifiers. */
@FunctionalInterface
public interface DecisionUsageSink {

    void record(TaskContext task, AdapterDescriptor adapter, String model, DecisionUsage usage);

    DecisionUsageSink LOG = new DecisionUsageSink() {
        private final Logger log = LoggerFactory.getLogger(DecisionUsageSink.class);

        @Override
        public void record(TaskContext task, AdapterDescriptor adapter, String model, DecisionUsage usage) {
            log.info("Decision usage of task {} in instance {} (chain {}, attempt {}): adapter={}, provider={}, "
                            + "model={}, calls={}, inputTokens={}, outputTokens={}, latencyMs={}, estimatedCost={}",
                    task.taskId(), task.instanceId(), task.chainId(), task.attempt(), adapter.id(), adapter.provider(),
                    model, usage.calls(), usage.inputTokens(), usage.outputTokens(),
                    usage.latency() == null ? null : usage.latency().toMillis(), usage.estimatedCost());
        }
    };
}
