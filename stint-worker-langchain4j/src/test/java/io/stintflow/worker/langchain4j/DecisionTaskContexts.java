package io.stintflow.worker.langchain4j;

import io.stintflow.spi.Lineage;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.decision.DecisionRequest;
import io.stintflow.worker.TaskContext;

/** The task context of a decision, with synthetic identifiers. */
final class DecisionTaskContexts {

    private DecisionTaskContexts() {
    }

    static TaskContext of(DecisionRequest request) {
        return new TaskContext("/do/0/classify", "i-1", "c-1", 1, request.toJson(),
                new WorkflowRef("test", "decide", "1.0.0"), "chn-1", Lineage.NONE);
    }
}
