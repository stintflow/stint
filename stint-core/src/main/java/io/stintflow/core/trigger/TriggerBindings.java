package io.stintflow.core.trigger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.cloudevents.CloudEvent;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.spi.WorkflowRef;

/**
 * Event → definition mapping (SDD 2.1, RF3, sec. 8b). Programmatic API that SDD 2.4 later feeds from
 * each definition's {@code schedule.on}.
 * <p>
 * One event may start <strong>several</strong> definitions — each one declares its own subscription,
 * as in pub/sub; restricting it to one would reintroduce a central router. Targets are deduplicated
 * per definition: two bindings of the same definition matching one event start it once (the
 * deterministic instance id of sec. 8a guarantees that too).
 */
public final class TriggerBindings {

    private final WorkflowRegistry registry;
    private final Map<String, TriggerBinding> byId = new LinkedHashMap<>();

    public TriggerBindings(WorkflowRegistry registry) {
        this.registry = registry;
    }

    /** @throws IllegalArgumentException if the target isn't registered or the id is already bound */
    public synchronized void bind(TriggerBinding binding) {
        if (registry.find(binding.target()).isEmpty()) {
            throw new IllegalArgumentException("Cannot bind '" + binding.id() + "': workflow "
                    + binding.target().canonical() + " is not registered");
        }
        if (byId.putIfAbsent(binding.id(), binding) != null) {
            throw new IllegalArgumentException("Trigger binding '" + binding.id() + "' is already bound");
        }
    }

    public synchronized void unbind(String bindingId) {
        byId.remove(bindingId);
    }

    /** Distinct target definitions whose bindings match {@code event}, in binding order. */
    public synchronized List<WorkflowRef> targetsFor(CloudEvent event) {
        Set<WorkflowRef> targets = new LinkedHashSet<>();
        for (TriggerBinding binding : byId.values()) {
            if (binding.filter().matches(event)) {
                targets.add(binding.target());
            }
        }
        return new ArrayList<>(targets);
    }
}
