package io.stintflow.core;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.cloudevents.CloudEvent;
import io.stintflow.wire.Json;

/**
 * The {@code listen} bookkeeping kept in {@link io.stintflow.spi.InstanceSnapshot#listenState()} (SDD 2.3,
 * sec. 8b/8c) — opaque JSON to the stores:
 * <pre>
 * { "active":   { "listenId", "strategy", "keys": [per filter], "pending": [not yet consumed],
 *                 "timerKey" (optional), "events": [{"filter": i, "event": envelope}, ...] (consumption order) },
 *   "consumed": [ "&lt;source&gt;|&lt;id&gt;", ... the last {@value #CONSUMED_KEPT} events consumed ] }
 * </pre>
 * {@code active} exists only while the instance waits in a {@code listen}. {@code consumed} outlives it: an
 * event this instance already consumed, redelivered later and kept in the inbox, must not satisfy a new wait
 * with the same key (sec. 8c).
 */
final class ListenState {

    static final int CONSUMED_KEPT = 25;

    private ListenState() {
    }

    /** The active listen, or {@code null}. */
    static ObjectNode active(JsonNode state) {
        return state != null && state.get("active") instanceof ObjectNode active ? active : null;
    }

    /** What an activation carries forward: {@code consumed} only, or {@code null} if there is nothing. */
    static JsonNode carried(JsonNode state) {
        if (state == null || !state.has("consumed")) {
            return null;
        }
        ObjectNode out = Json.obj();
        out.set("consumed", state.get("consumed").deepCopy());
        return out;
    }

    static ObjectNode withActive(JsonNode carried, String listenId, String strategy, List<String> keys,
            String timerKey) {
        ObjectNode out = carried == null ? Json.obj() : (ObjectNode) carried.deepCopy();
        ObjectNode active = out.putObject("active");
        active.put("listenId", listenId);
        active.put("strategy", strategy);
        ArrayNode keyArray = active.putArray("keys");
        keys.forEach(keyArray::add);
        ArrayNode pending = active.putArray("pending");
        keys.forEach(pending::add);
        if (timerKey != null) {
            active.put("timerKey", timerKey);
        }
        active.putArray("events");
        return out;
    }

    /**
     * A copy of {@code state} after {@code event} (as {@code envelope}) satisfied filter {@code filterIndex}:
     * its key is no longer pending, the event is appended in consumption order and remembered as consumed.
     */
    static ObjectNode withPartial(JsonNode state, String consumedKey, int filterIndex, JsonNode envelope,
            CloudEvent event) {
        ObjectNode out = (ObjectNode) withConsumed(state, event);
        ObjectNode active = (ObjectNode) out.get("active");
        ArrayNode pending = active.putArray("pending");
        strings(state.get("active").get("pending")).stream().filter(k -> !k.equals(consumedKey)).forEach(pending::add);
        ObjectNode entry = ((ArrayNode) active.get("events")).addObject();
        entry.put("filter", filterIndex);
        entry.set("event", envelope);
        return out;
    }

    /** A copy of {@code state} (which may be {@code null}) remembering {@code event} as consumed. */
    static JsonNode withConsumed(JsonNode state, CloudEvent event) {
        ObjectNode out = state == null ? Json.obj() : (ObjectNode) state.deepCopy();
        List<String> consumed = strings(out.get("consumed"));
        consumed.add(identity(event));
        ArrayNode array = out.putArray("consumed");
        consumed.subList(Math.max(0, consumed.size() - CONSUMED_KEPT), consumed.size()).forEach(array::add);
        return out;
    }

    static boolean alreadyConsumed(JsonNode state, CloudEvent event) {
        return state != null && strings(state.get("consumed")).contains(identity(event));
    }

    static List<String> strings(JsonNode array) {
        List<String> out = new ArrayList<>();
        if (array != null) {
            array.forEach(item -> out.add(item.asText()));
        }
        return out;
    }

    /** CloudEvents: {@code source} + {@code id} identify an event. */
    private static String identity(CloudEvent event) {
        return event.getSource() + "|" + event.getId();
    }
}
