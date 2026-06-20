package io.stintflow.core;

import java.util.List;

import io.stintflow.spi.WorkflowRef;

/**
 * Minimal internal workflow model: an ordered list of remote steps.
 * <p>
 * NOTE: this is the MVP shape. In production this model is produced by parsing a CNCF Serverless
 * Workflow definition (YAML/JSON) — the parser is a swappable front-end and is deliberately out of
 * scope for the skeleton. What matters here is the distributed execution machinery underneath it.
 */
public record WorkflowDefinition(WorkflowRef ref, List<RemoteStep> steps) {

    public RemoteStep step(int index) {
        return steps.get(index);
    }

    public boolean isLast(int index) {
        return index >= steps.size() - 1;
    }
}
