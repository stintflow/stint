package io.stintflow.core;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.spi.wire.StintEvents;
import io.stintflow.wire.Json;

/**
 * The single place a domain fact's CloudEvent is built (SDD 2.2, RF9, sec. 8f) — SDD 2.5 adds its
 * chain/trace extensions here, additively.
 * <p>
 * Maps the evaluated DSL 1.0 {@code emit.event.with} object onto CloudEvents attributes; any key that
 * isn't a standard attribute becomes an extension (the interpreter already validated names/values).
 */
final class FactFactory {

    /** CloudEvents extension for the claim-check pattern (CloudEvents {@code extensions/dataref.md}). */
    static final String DATAREF = "dataref";

    private static final Set<String> STANDARD = Set.of(
            "id", "source", "type", "time", "subject", "datacontenttype", "dataschema", "data", "specversion");

    private FactFactory() {
    }

    /**
     * SDD 2.2, sec. 8b: the author's {@code id} if declared (DSL 1.0 asks for one); otherwise a
     * name-based UUID of (instance, the snapshot version the activation started from, the emitting
     * node, its occurrence in the activation) — a re-run after a crash replays the same walk from the
     * same version, so it produces the same ids.
     */
    static String factId(JsonNode with, String instanceId, long baseVersion, String pointer, int occurrence) {
        JsonNode declared = with.get("id");
        if (declared != null && declared.isTextual() && !declared.asText().isBlank()) {
            return declared.asText();
        }
        String name = instanceId + '|' + baseVersion + '|' + pointer + '|' + occurrence;
        return UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8)).toString();
    }

    /** The fact with its {@code data} inline, carrying the emitting activation's {@code lineage} (SDD 2.5). */
    static CloudEvent build(String id, JsonNode with, Instant now, Lineage lineage) {
        CloudEventBuilder builder = attributes(id, with, now, lineage);
        JsonNode data = with.get("data");
        if (data != null && !data.isNull()) {
            builder.withData(dataBytes(data, contentType(with)));
        }
        return builder.build();
    }

    /** The same fact with its {@code data} replaced by a {@code dataref} pointer (sec. 8d). */
    static CloudEvent buildWithDataref(String id, JsonNode with, Instant now, URI dataref, Lineage lineage) {
        return attributes(id, with, now, lineage).withExtension(DATAREF, dataref.toString()).build();
    }

    /** The {@code data} bytes as they'd travel inline — what goes to the facts store when too large. */
    static byte[] dataBytes(JsonNode with) {
        JsonNode data = with.get("data");
        return data == null || data.isNull() ? new byte[0] : dataBytes(data, contentType(with));
    }

    static boolean hasData(JsonNode with) {
        JsonNode data = with.get("data");
        return data != null && !data.isNull();
    }

    /** {@code facts/<namespace>/<name>/<id>}: deterministic, so a re-run overwrites the same object. */
    static String factKey(WorkflowRef ref, String id) {
        return "facts/" + ref.namespace() + "/" + ref.name() + "/" + id;
    }

    private static CloudEventBuilder attributes(String id, JsonNode with, Instant now, Lineage lineage) {
        String type = with.get("type").asText();
        if (StintEvents.isReservedType(type)) {
            // Unreachable by construction (the interpreter fails such an emit); kept as a last guard.
            throw new IllegalStateException("Refusing to build a fact with reserved type " + type);
        }
        CloudEventBuilder builder = CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create(with.get("source").asText()))
                .withType(type)
                .withTime(with.hasNonNull("time") ? OffsetDateTime.parse(with.get("time").asText())
                        : now.atOffset(ZoneOffset.UTC))
                .withDataContentType(contentType(with));
        if (with.hasNonNull("subject")) {
            builder.withSubject(with.get("subject").asText());
        }
        if (with.hasNonNull("dataschema")) {
            builder.withDataSchema(URI.create(with.get("dataschema").asText()));
        }
        var fields = with.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            if (STANDARD.contains(field.getKey())) {
                continue;
            }
            JsonNode value = field.getValue();
            if (value.isBoolean()) {
                builder.withExtension(field.getKey(), value.booleanValue());
            } else if (value.isTextual()) {
                builder.withExtension(field.getKey(), value.asText());
            } else {
                builder.withExtension(field.getKey(), value.intValue());
            }
        }
        // SDD 2.5 (sec. 8a/8d): chain, cause and the trace context of the emitting activation — set by the
        // engine only (the interpreter rejects an emit that tries to set them).
        putIfSet(builder, StintEvents.EXT_CHAIN_ID, lineage.chainId());
        putIfSet(builder, StintEvents.EXT_CAUSATION_ID, lineage.causationId());
        putIfSet(builder, StintEvents.EXT_TRACEPARENT, lineage.traceparent());
        putIfSet(builder, StintEvents.EXT_TRACESTATE, lineage.tracestate());
        return builder;
    }

    private static void putIfSet(CloudEventBuilder builder, String name, String value) {
        if (value != null && !value.isBlank()) {
            builder.withExtension(name, value);
        }
    }

    /** DSL 1.0: "If omitted, it implies the data is a JSON value conforming to the application/json media type." */
    private static String contentType(JsonNode with) {
        return with.hasNonNull("datacontenttype") ? with.get("datacontenttype").asText() : StintEvents.CONTENT_TYPE_JSON;
    }

    private static byte[] dataBytes(JsonNode data, String contentType) {
        if (!contentType.contains("json") && data.isTextual()) {
            return data.asText().getBytes(StandardCharsets.UTF_8);
        }
        return Json.bytes(data);
    }
}
