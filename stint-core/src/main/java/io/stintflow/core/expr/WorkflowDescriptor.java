package io.stintflow.core.expr;

import java.time.Instant;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.spi.WorkflowRef;

/**
 * What {@code $workflow} describes (SDD 2.1, RF6). DSL 1.0 {@code dsl.md}, Workflow Descriptor:
 * {@code id} ("A unique id of the workflow execution"), {@code input} ("The workflow's raw input
 * (i.e BEFORE the input.from expression)") and {@code startedAt} ("The start time of the
 * execution"). {@code ref} (namespace/name/version) is a Stint extension kept for compatibility;
 * the spec's {@code definition} (the parsed document) is not exposed.
 * <p>
 * {@code id}, {@code input} and {@code startedAt} are {@code null} where no instance exists (e.g.
 * evaluating a bare definition in a test).
 */
public record WorkflowDescriptor(String id, JsonNode input, Instant startedAt, WorkflowRef ref) {

    public static WorkflowDescriptor of(WorkflowRef ref) {
        return new WorkflowDescriptor(null, null, null, ref);
    }
}
