package io.stintflow.core;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.ListenNode;
import io.stintflow.wire.Json;

/**
 * The {@code event:} wait keys of SDD 2.3 (sec. 4.1): {@code event:<type>:<shapeId>:<valueHash>}.
 * <p>
 * A <em>shape</em> is what a filter's {@code correlate} looks like — its names and {@code from}
 * expressions — and is known from the definition alone, so the registry can index shapes by event type
 * (sec. 8a). The value hash covers the correlation values: on the waiting side each {@code expect}, on the
 * arriving side each {@code from} applied to the event. Both sides therefore compute the same key exactly
 * when every correlation matches — several {@code correlate} entries are an AND.
 */
public final class EventCorrelation {

    private static final String PREFIX = "event:";

    private EventCorrelation() {
    }

    /**
     * One correlation shape: {@code from} per correlation name, in name order.
     *
     * @param id short, stable hash of the names and {@code from} sources
     */
    public record Shape(String id, SortedMap<String, Expr> from) {
    }

    /** @throws IllegalArgumentException if a {@code from} is not a jq expression (a lambda has no stable source) */
    public static Shape shapeOf(ListenNode.Filter filter) {
        SortedMap<String, Expr> from = new TreeMap<>();
        StringBuilder canonical = new StringBuilder();
        for (Map.Entry<String, ListenNode.Correlation> entry : filter.correlate().entrySet()) {
            if (!(entry.getValue().from() instanceof Expr.Jq jq)) {
                throw new IllegalArgumentException("correlate." + entry.getKey()
                        + ".from must be a runtime expression (jq), not a lambda");
            }
            from.put(entry.getKey(), jq);
            canonical.append(entry.getKey()).append('=').append(jq.source()).append('\n');
        }
        return new Shape(sha256(canonical.toString()).substring(0, 16), from);
    }

    /** The key for {@code values} (correlation name → value) of {@code type} in {@code shape}. */
    public static String waitKey(String type, Shape shape, SortedMap<String, JsonNode> values) {
        StringBuilder canonical = new StringBuilder();
        for (Map.Entry<String, JsonNode> entry : values.entrySet()) {
            canonical.append(entry.getKey()).append('=').append(canonicalJson(entry.getValue())).append('\n');
        }
        return PREFIX + type + ':' + shape.id() + ':' + sha256(canonical.toString()).substring(0, 32);
    }

    public static boolean isEventKey(String waitKey) {
        return waitKey.startsWith(PREFIX);
    }

    /** JSON with object keys sorted at every level, so equal values always hash alike. */
    static String canonicalJson(JsonNode node) {
        return sorted(node).toString();
    }

    private static JsonNode sorted(JsonNode node) {
        if (node.isObject()) {
            List<String> names = new ArrayList<>();
            node.fieldNames().forEachRemaining(names::add);
            names.sort(null);
            ObjectNode out = Json.obj();
            for (String name : names) {
                out.set(name, sorted(node.get(name)));
            }
            return out;
        }
        if (node.isArray()) {
            var out = Json.MAPPER.createArrayNode();
            node.forEach(child -> out.add(sorted(child)));
            return out;
        }
        return node;
    }

    private static String sha256(String text) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(text.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is always available", e);
        }
    }
}
