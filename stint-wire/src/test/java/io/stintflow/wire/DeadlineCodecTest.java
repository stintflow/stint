package io.stintflow.wire;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.spi.wire.StintEvents;

/**
 * SDD 3a.3c, CA1: an invoke's deadline travels as the {@code stintdeadline} extension; an invoke without it — an engine
 * before 3a.3c — or with an unreadable one has no known deadline, never an error.
 */
class DeadlineCodecTest {

    static final Instant DEADLINE = Instant.parse("2026-10-12T12:05:00Z");
    private final DefaultCloudEventCodec codec = new DefaultCloudEventCodec();

    private static TaskInvocation invocation(Instant deadline) {
        return new TaskInvocation("i-1", "/do/0/ask", "c-1", new WorkflowRef("ai", "notes", "1.0.0"), "ask",
                Json.obj().put("text", "synthetic"), 2, Lineage.NONE, deadline);
    }

    @Test
    void the_deadline_round_trips() {
        CloudEvent event = codec.toEvent(invocation(DEADLINE));

        assertThat(event.getExtension(StintEvents.EXT_DEADLINE)).hasToString("2026-10-12T12:05:00Z");
        assertThat(codec.toInvocation(event).deadline()).isEqualTo(DEADLINE);
        assertThat(codec.toInvocation(event)).isEqualTo(invocation(DEADLINE));
    }

    @Test
    void no_deadline_writes_no_extension_and_reads_as_none() {
        CloudEvent event = codec.toEvent(invocation(null));

        assertThat(event.getExtensionNames()).doesNotContain(StintEvents.EXT_DEADLINE);
        assertThat(codec.toInvocation(event).deadline()).isNull();
        // the pre-3a.3c constructors still build the same invocation
        assertThat(new TaskInvocation("i-1", "/do/0/ask", "c-1", new WorkflowRef("ai", "notes", "1.0.0"), "ask",
                Json.obj().put("text", "synthetic"), 2, Lineage.NONE)).isEqualTo(invocation(null));
    }

    @Test
    void an_unreadable_deadline_is_no_deadline_and_is_logged_without_its_value() {
        CloudEvent event = CloudEventBuilder.v1(codec.toEvent(invocation(null)))
                .withExtension(StintEvents.EXT_DEADLINE, "UNREADABLE-DEADLINE-MARKER").build();
        List<String> logged = new CopyOnWriteArrayList<>();
        java.util.logging.Logger jul = java.util.logging.Logger.getLogger(DefaultCloudEventCodec.class.getName());
        java.util.logging.Handler handler = new java.util.logging.Handler() {
            @Override
            public void publish(java.util.logging.LogRecord record) {
                logged.add(record.getLevel() + " " + java.text.MessageFormat.format(record.getMessage(),
                        record.getParameters()));
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        jul.addHandler(handler);
        try {
            assertThat(codec.toInvocation(event).deadline()).isNull();
        } finally {
            jul.removeHandler(handler);
        }
        assertThat(logged).singleElement().satisfies(line -> assertThat(line).startsWith("WARNING")
                .contains("Task invoke c-1 has an unreadable stintdeadline extension")
                .doesNotContain("UNREADABLE-DEADLINE-MARKER"));
    }
}
