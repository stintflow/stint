package io.stintflow.core.trigger;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.cloudevents.CloudEvent;

/**
 * Which inbound CloudEvents a {@link TriggerBinding} reacts to (SDD 2.1, RF3).
 * <p>
 * DSL 1.0 {@code dsl-reference.md}, Event Filter, {@code with}: "A name/value mapping of the
 * attributes filtered events must define. Supports both regular expressions and runtime
 * expressions." This SDD implements the <strong>exact-match subset</strong> only: every entry of
 * {@code with} must equal the event's context attribute ({@code source}, {@code subject},
 * {@code datacontenttype}, {@code dataschema}, ...) or extension of the same name, compared as
 * strings. Regex/runtime-expression matching and filtering on {@code data} are left to SDD 2.4.
 *
 * @param type required — the event {@code type} (keeps a binding from accidentally matching everything)
 * @param with additional attributes the event must carry with exactly these values; may be empty
 */
public record EventFilter(String type, Map<String, String> with) {

    private static final Set<String> CONTEXT_ATTRIBUTES = Set.of(
            "specversion", "id", "source", "type", "datacontenttype", "dataschema", "subject", "time");

    public EventFilter {
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("EventFilter.type is required");
        }
        with = with == null ? Map.of() : Map.copyOf(with);
    }

    public static EventFilter ofType(String type) {
        return new EventFilter(type, Map.of());
    }

    public boolean matches(CloudEvent event) {
        if (!type.equals(event.getType())) {
            return false;
        }
        for (Map.Entry<String, String> required : with.entrySet()) {
            if (!required.getValue().equals(attributeAsString(event, required.getKey()))) {
                return false;
            }
        }
        return true;
    }

    private static String attributeAsString(CloudEvent event, String name) {
        Object value = CONTEXT_ATTRIBUTES.contains(name) ? event.getAttribute(name) : event.getExtension(name);
        return Objects.toString(value, null);
    }
}
