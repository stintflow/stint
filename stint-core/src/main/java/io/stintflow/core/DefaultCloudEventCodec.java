package io.stintflow.core;

import java.net.URI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.spi.ErrorInfo;
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
 */
public final class DefaultCloudEventCodec implements CloudEventCodec {

    private static final URI SOURCE_ENGINE = URI.create("stint://engine");
    private static final URI SOURCE_WORKER = URI.create("stint://worker");

    @Override
    public CloudEvent toEvent(TaskInvocation inv) {
        return CloudEventBuilder.v1()
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
                .withExtension(StintEvents.EXT_DEFINITION, inv.definition().canonical())
                .build();
    }

    @Override
    public CloudEvent toEvent(TaskResult result, String workflowInstanceId) {
        ObjectNode data = Json.obj();
        data.put("status", result.status().name());
        if (result.output() != null) {
            data.set("output", result.output());
        }
        if (result.error() != null) {
            ObjectNode err = data.putObject("error");
            err.put("type", result.error().type());
            err.put("message", result.error().message());
        }
        return CloudEventBuilder.v1()
                .withId(result.correlationId())
                .withSource(SOURCE_WORKER)
                .withType(StintEvents.TYPE_TASK_RESULT)
                .withDataContentType(StintEvents.CONTENT_TYPE_JSON)
                .withData(Json.bytes(data))
                .withExtension(StintEvents.EXT_CORRELATION_ID, result.correlationId())
                .withExtension(StintEvents.EXT_WORKFLOW_INSTANCE_ID, workflowInstanceId)
                .build();
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
                Integer.parseInt(ext(event, StintEvents.EXT_ATTEMPT)));
    }

    @Override
    public TaskResult toResult(CloudEvent event) {
        JsonNode data = Json.read(event.getData().toBytes());
        String correlationId = ext(event, StintEvents.EXT_CORRELATION_ID);
        TaskResult.Status status = TaskResult.Status.valueOf(data.get("status").asText());
        if (status == TaskResult.Status.FAILED) {
            JsonNode err = data.get("error");
            return TaskResult.failed(correlationId,
                    new ErrorInfo(err.path("type").asText(), err.path("message").asText()));
        }
        return TaskResult.completed(correlationId, data.get("output"));
    }

    private static String ext(CloudEvent event, String name) {
        Object v = event.getExtension(name);
        return v == null ? null : v.toString();
    }
}
