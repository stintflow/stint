package io.stintflow.spi;

import java.util.Map;
import java.util.Set;

/**
 * The only attributes a span — and the log MDC — may carry (SDD 2.5, decision 7): identifiers. Never a
 * task's input or output, a payload, or an event's content. The same names are used as MDC keys, so a log
 * line and a span describe an execution with one vocabulary.
 */
public final class TraceAttributes {

    public static final String INSTANCE_ID = "stint.instanceId";
    public static final String CHAIN_ID = "stint.chainId";
    public static final String CAUSATION_ID = "stint.causationId";
    public static final String WORKFLOW = "stint.workflow";
    public static final String TASK_ID = "stint.taskId";
    public static final String ATTEMPT = "stint.attempt";
    public static final String CORRELATION_ID = "stint.correlationId";

    public static final Set<String> ALLOWED =
            Set.of(INSTANCE_ID, CHAIN_ID, CAUSATION_ID, WORKFLOW, TASK_ID, ATTEMPT, CORRELATION_ID);

    private TraceAttributes() {
    }

    /** @throws IllegalArgumentException if any key is not an identifier from {@link #ALLOWED} */
    public static Map<String, String> requireIdentifiersOnly(Map<String, String> attributes) {
        for (String key : attributes.keySet()) {
            if (!ALLOWED.contains(key)) {
                throw new IllegalArgumentException("Span/log attribute '" + key + "' is not allowed: only identifiers "
                        + ALLOWED + " may be recorded, never inputs, outputs or payloads");
            }
        }
        return attributes;
    }
}
