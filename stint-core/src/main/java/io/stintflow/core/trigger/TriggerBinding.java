package io.stintflow.core.trigger;

import io.stintflow.spi.WorkflowRef;

/**
 * "When an event matching {@code filter} arrives, start {@code target}" (SDD 2.1, RF3). SDD 2.4 builds
 * these from a definition's {@code schedule.on}; until then they are registered programmatically.
 *
 * @param id     unique binding id, used to {@link TriggerBindings#unbind} it
 * @param filter which events it reacts to
 * @param target the definition an event starts
 */
public record TriggerBinding(String id, EventFilter filter, WorkflowRef target) {

    public TriggerBinding {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("TriggerBinding.id is required");
        }
        if (filter == null || target == null) {
            throw new IllegalArgumentException("TriggerBinding requires a filter and a target");
        }
    }
}
