package io.stintflow.worker.decision;

import io.stintflow.worker.TaskContext;

/**
 * What an adapter gets besides the request (SDD 3a.2, 4.2): the task — for its identifiers in usage records and spans,
 * never for content — and the question of the route, from the worker's configuration (8h).
 */
public record DecisionTask(TaskContext task, String instructions) {
}
