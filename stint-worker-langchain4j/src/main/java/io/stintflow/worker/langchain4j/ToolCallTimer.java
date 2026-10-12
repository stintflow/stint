package io.stintflow.worker.langchain4j;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.service.tool.BeforeToolExecution;
import dev.langchain4j.service.tool.ToolExecution;

/**
 * Counts and times one execution's tool calls (SDD 3a.3, 5.8). A tool call that takes longer than the limit is a
 * sign the tool should be a task of the workflow — with its own retry, timeout and checkpoint — so it is reported as a
 * WARN with the tool's name and duration; never its arguments or result. It is a signal for the author, not a rule.
 */
final class ToolCallTimer {

    private static final Logger LOG = LoggerFactory.getLogger(ToolCallTimer.class);

    private final InstantSource clock;
    private final Duration limit;
    private final Map<String, Instant> started = new HashMap<>();
    private int calls;

    ToolCallTimer(InstantSource clock, Duration limit) {
        this.clock = clock;
        this.limit = limit;
    }

    synchronized void before(BeforeToolExecution before) {
        started.put(key(before.request()), clock.instant());
    }

    synchronized void after(ToolExecution execution) {
        calls++;
        Instant start = started.remove(key(execution.request()));
        if (start == null) {
            return;
        }
        Duration took = Duration.between(start, clock.instant());
        if (took.compareTo(limit) > 0) {
            LOG.warn("Tool call '{}' took {} ms, over the limit of {} ms: a tool this slow, or one that needs approval, "
                    + "should be a task of the workflow (SDD 3a.3)", execution.request().name(), took.toMillis(),
                    limit.toMillis());
        }
    }

    synchronized int calls() {
        return calls;
    }

    private static String key(ToolExecutionRequest request) {
        return request.id() != null ? request.id() : request.name();
    }
}
