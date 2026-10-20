package io.stintflow.wire;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.ModelProvider;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.spi.wire.StintEvents;

/**
 * SDD 3a.5, additive wire: an invoke carries the level as {@code stintsensitivity}, a result the provider as
 * {@code stintprovider}. Absent, both read as before 3a.5; an unreadable level is restricted, an unreadable provider is
 * dropped (it only feeds the audit), and a provider that doesn't say {@code remote=false} is remote.
 */
class ComplianceCodecTest {

    private final DefaultCloudEventCodec codec = new DefaultCloudEventCodec();

    @Test
    void the_level_round_trips_on_an_invoke_and_absent_reads_as_none() {
        TaskInvocation restricted = new TaskInvocation("i-1", "/do/0/ask", "c-1", new WorkflowRef("ai", "n", "1.0.0"),
                "ask", Json.obj(), 1, Lineage.NONE, null, Sensitivity.RESTRICTED);
        CloudEvent event = codec.toEvent(restricted);
        assertThat(event.getExtension(StintEvents.EXT_SENSITIVITY)).hasToString("restricted");
        assertThat(codec.toInvocation(event)).isEqualTo(restricted);

        CloudEvent legacy = codec.toEvent(new TaskInvocation("i-1", "/do/0/ask", "c-1",
                new WorkflowRef("ai", "n", "1.0.0"), "ask", Json.obj(), 1, Lineage.NONE, null));
        assertThat(legacy.getExtension(StintEvents.EXT_SENSITIVITY)).isNull();
        assertThat(codec.toInvocation(legacy).sensitivity()).isNull();

        CloudEvent unreadable = CloudEventBuilder.v1(event).withExtension(StintEvents.EXT_SENSITIVITY, "public").build();
        assertThat(codec.toInvocation(unreadable).sensitivity()).isEqualTo(Sensitivity.RESTRICTED);
    }

    @Test
    void the_provider_round_trips_on_a_result_success_or_failure() {
        ModelProvider provider = new ModelProvider("approved-cloud", "m-1", true);
        TaskResult ok = TaskResult.completed("c-1", Json.obj().put("summary", "synthetic")).withProvider(provider);
        CloudEvent event = codec.toEvent(ok, "i-1");

        assertThat(event.getExtension(StintEvents.EXT_PROVIDER)).hasToString("approved-cloud;model=m-1;remote=true");
        assertThat(codec.toResult(event).provider()).isEqualTo(provider);
        assertThat(codec.toResult(codec.toEvent(TaskResult.failed("c-1", ErrorInfo.of(new IllegalStateException()))
                .withProvider(new ModelProvider("litjev", null, false)), "i-1")).provider())
                .isEqualTo(new ModelProvider("litjev", null, false));
        assertThat(codec.toResult(codec.toEvent(TaskResult.completed("c-1", Json.obj()), "i-1")).provider()).isNull();
    }

    @Test
    void a_provider_value_is_read_tolerantly_and_remote_unless_it_says_otherwise() {
        assertThat(ModelProvider.parse("litjev;remote=false")).hasValue(new ModelProvider("litjev", null, false));
        assertThat(ModelProvider.parse("some-cloud")).hasValue(new ModelProvider("some-cloud", null, true));
        assertThat(ModelProvider.parse("some-cloud;remote=maybe")).hasValue(new ModelProvider("some-cloud", null, true));
        assertThat(ModelProvider.parse(";model=x")).isEmpty();

        CloudEvent garbled = CloudEventBuilder.v1(codec.toEvent(TaskResult.completed("c-1", Json.obj()), "i-1"))
                .withExtension(StintEvents.EXT_PROVIDER, " ").build();
        assertThat(codec.toResult(garbled).provider()).isNull();
    }
}
