package io.stintflow.wire;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.spi.wire.StintEvents;
import io.stintflow.spi.wire.CloudEventCodec;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;

/**
 * Jackson-backed {@link CloudEventCodec}. Implemented once here and reused by every transport
 * connector, so the orchestrator and all workers agree on the exact envelope on the wire.
 * <p>
 * SDD 1.3, sec. 8e: writes the new {@link ErrorInfo} shape only, but reads both the new shape and
 * the pre-SDD-1.3 {@code {type, message}} shape (accepted 0.x wire evolution).
 */
public final class DefaultCloudEventCodec implements CloudEventCodec {

    private static final URI SOURCE_ENGINE = URI.create("stint://engine");
    private static final URI SOURCE_WORKER = URI.create("stint://worker");
    private static final String LEGACY_TYPE_PREFIX = "urn:legacy:";

    @Override
    public CloudEvent toEvent(TaskInvocation inv) {
        CloudEventBuilder builder = CloudEventBuilder.v1()
                .withId(inv.correlationId())
                .withSource(SOURCE_ENGINE)
                .withType(StintEvents.TYPE_TASK_INVOKE)
                .withSubject(inv.routingKey())
                .withDataContentType(StintEvents.CONTENT_TYPE_JSON)
                .withData(Json.bytes(inv.input()))
                .withExtension(StintEvents.EXT_CORRELATION_ID, inv.correlationId())
                .withExtension(StintEvents.EXT_WORKFLOW_INSTANCE_ID, inv.workflowInstanceId())
                .withExtension(StintEvents.EXT_TASK_ID, inv.taskId())
                .withExtension(StintEvents.EXT_ATTEMPT, String.valueOf(inv.attempt()))
                .withExtension(StintEvents.EXT_DEFINITION, inv.definition().canonical());
        return withLineage(builder, inv.lineage()).build();
    }

    @Override
    public CloudEvent toEvent(TaskResult result, String workflowInstanceId) {
        ObjectNode data = Json.obj();
        data.put("status", result.status().name());
        if (result.output() != null) {
            data.set("output", result.output());
        }
        if (result.error() != null) {
            data.set("error", errorToJson(result.error()));
        }
        CloudEventBuilder builder = CloudEventBuilder.v1()
                .withId(result.correlationId())
                .withSource(SOURCE_WORKER)
                .withType(StintEvents.TYPE_TASK_RESULT)
                .withDataContentType(StintEvents.CONTENT_TYPE_JSON)
                .withData(Json.bytes(data))
                .withExtension(StintEvents.EXT_CORRELATION_ID, result.correlationId())
                .withExtension(StintEvents.EXT_WORKFLOW_INSTANCE_ID, workflowInstanceId);
        return withLineage(builder, result.lineage()).build();
    }

    @Override
    public TaskInvocation toInvocation(CloudEvent event) {
        JsonNode input = Json.read(event.getData().toBytes());
        return new TaskInvocation(
                ext(event, StintEvents.EXT_WORKFLOW_INSTANCE_ID),
                ext(event, StintEvents.EXT_TASK_ID),
                ext(event, StintEvents.EXT_CORRELATION_ID),
                WorkflowRef.parse(ext(event, StintEvents.EXT_DEFINITION)),
                event.getSubject(),
                input,
                Integer.parseInt(ext(event, StintEvents.EXT_ATTEMPT)),
                lineage(event));
    }

    @Override
    public TaskResult toResult(CloudEvent event) {
        JsonNode data = Json.read(event.getData().toBytes());
        String correlationId = ext(event, StintEvents.EXT_CORRELATION_ID);
        TaskResult.Status status = TaskResult.Status.valueOf(data.get("status").asText());
        if (status == TaskResult.Status.FAILED) {
            return TaskResult.failed(correlationId, errorFromJson(data.get("error"))).withLineage(lineage(event));
        }
        return TaskResult.completed(correlationId, data.get("output")).withLineage(lineage(event));
    }

    /** SDD 2.5: writes only the lineage extensions that are set. */
    private static CloudEventBuilder withLineage(CloudEventBuilder builder, Lineage lineage) {
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

    /**
     * SDD 2.5, tolerant read: any of the four extensions may be missing (an event in the pre-2.5 format
     * has none of them) — missing reads as {@code null}, never as an error.
     */
    public static Lineage lineage(CloudEvent event) {
        return new Lineage(optionalExt(event, StintEvents.EXT_CHAIN_ID), optionalExt(event, StintEvents.EXT_CAUSATION_ID),
                optionalExt(event, StintEvents.EXT_TRACEPARENT), optionalExt(event, StintEvents.EXT_TRACESTATE));
    }

    private static String optionalExt(CloudEvent event, String name) {
        Object value = event.getExtension(name);
        return value == null ? null : value.toString();
    }

    private static ObjectNode errorToJson(ErrorInfo error) {
        ObjectNode err = Json.obj();
        err.put("type", error.type().toString());
        if (error.status() != null) {
            err.put("status", error.status());
        }
        if (error.title() != null) {
            err.put("title", error.title());
        }
        if (error.detail() != null) {
            err.put("detail", error.detail());
        }
        if (error.instance() != null) {
            err.put("instance", error.instance());
        }
        if (error.retryAfter() != null) {
            err.put("retryAfter", error.retryAfter().toString());
        }
        return err;
    }

    /** Accepts the new shape and the pre-SDD-1.3 {@code {type, message}} shape (SDD 1.3, sec. 8e). */
    private static ErrorInfo errorFromJson(JsonNode err) {
        String rawType = err.path("type").asText(null);
        URI type = parseTypeUri(rawType);
        if (err.hasNonNull("message") && !err.hasNonNull("detail")) {
            // legacy shape: {type, message}
            return new ErrorInfo(type, null, null, err.path("message").asText(), null, null);
        }
        Integer status = err.hasNonNull("status") ? err.get("status").asInt() : null;
        String title = err.path("title").asText(null);
        String detail = err.path("detail").asText(null);
        String instance = err.path("instance").asText(null);
        Duration retryAfter = err.hasNonNull("retryAfter") ? Duration.parse(err.get("retryAfter").asText()) : null;
        return new ErrorInfo(type, status, title, detail, instance, retryAfter);
    }

    private static URI parseTypeUri(String rawType) {
        if (rawType == null) {
            return ErrorInfo.TYPE_RUNTIME;
        }
        return tryParseUri(rawType).orElseGet(() -> URI.create(LEGACY_TYPE_PREFIX + rawType));
    }

    private static Optional<URI> tryParseUri(String rawType) {
        try {
            URI uri = new URI(rawType);
            return uri.isAbsolute() ? Optional.of(uri) : Optional.empty();
        } catch (URISyntaxException e) {
            return Optional.empty();
        }
    }

    private static String ext(CloudEvent event, String name) {
        Object v = event.getExtension(name);
        return v == null ? null : v.toString();
    }
}
