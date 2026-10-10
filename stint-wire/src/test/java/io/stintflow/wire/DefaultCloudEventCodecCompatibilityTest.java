package io.stintflow.wire;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.spi.wire.StintEvents;
import io.cloudevents.core.builder.CloudEventBuilder;

/** SDD 1.3, sec. 8e: the codec still reads the pre-SDD-1.3 {@code {type, message}} error shape. */
class DefaultCloudEventCodecCompatibilityTest {

    @Test
    void reads_the_legacy_type_and_message_error_shape() {
        ObjectNode data = Json.obj();
        data.put("status", "FAILED");
        ObjectNode error = data.putObject("error");
        error.put("type", "java.lang.IllegalStateException");
        error.put("message", "boom from before SDD 1.3");

        var event = CloudEventBuilder.v1()
                .withId("corr-legacy")
                .withSource(URI.create("stint://worker"))
                .withType(StintEvents.TYPE_TASK_RESULT)
                .withDataContentType(StintEvents.CONTENT_TYPE_JSON)
                .withData(Json.bytes(data))
                .withExtension(StintEvents.EXT_CORRELATION_ID, "corr-legacy")
                .withExtension(StintEvents.EXT_WORKFLOW_INSTANCE_ID, "inst-legacy")
                .build();

        TaskResult result = new DefaultCloudEventCodec().toResult(event);

        assertThat(result.status()).isEqualTo(TaskResult.Status.FAILED);
        assertThat(result.error().detail()).isEqualTo("boom from before SDD 1.3");
        assertThat(result.error().type().toString()).contains("java.lang.IllegalStateException");
    }

    @Test
    void writes_and_reads_the_new_shape_round_trip() {
        var codec = new DefaultCloudEventCodec();
        var error = new io.stintflow.spi.ErrorInfo(io.stintflow.spi.ErrorInfo.TYPE_COMMUNICATION, 429,
                "Too Many Requests", "rate limited", "task-1", java.time.Duration.ofSeconds(30));
        TaskResult original = TaskResult.failed("corr-new", error);

        var event = codec.toEvent(original, "inst-new");
        TaskResult roundTripped = codec.toResult(event);

        assertThat(roundTripped.error().type()).isEqualTo(io.stintflow.spi.ErrorInfo.TYPE_COMMUNICATION);
        assertThat(roundTripped.error().status()).isEqualTo(429);
        assertThat(roundTripped.error().detail()).isEqualTo("rate limited");
        assertThat(roundTripped.error().retryAfter()).isEqualTo(java.time.Duration.ofSeconds(30));
    }

    /**
     * SDD 1.5, CA7: {@code definition} already carries the full {@code namespace:name:version} on
     * the wire (sec. 8c) — this proves the current-format event is read back correctly end to end,
     * the same shape {@link io.stintflow.worker.WorkerRuntime} decodes on the worker side.
     */
    @Test
    void reads_the_current_definition_extension_into_a_complete_workflow_ref() {
        var codec = new DefaultCloudEventCodec();
        WorkflowRef ref = new WorkflowRef("reports", "build-report", "1.0.0");
        TaskInvocation original = new TaskInvocation("inst-1", "/do/0/step", "corr-1", ref, "route-a", Json.obj(), 1);

        var event = codec.toEvent(original);
        TaskInvocation decoded = codec.toInvocation(event);

        assertThat(decoded.definition()).isEqualTo(ref);
        assertThat(decoded.definition().namespace()).isEqualTo("reports");
        assertThat(decoded.definition().name()).isEqualTo("build-report");
        assertThat(decoded.definition().version()).isEqualTo("1.0.0");
    }
}
