package io.stintflow.otel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.stintflow.spi.ExecutionTracer;
import io.stintflow.spi.TraceAttributes;

/**
 * SDD 2.5, CA8 (decision 7): a span may only carry identifiers. Recording a task's input, output or any
 * payload is refused — by the OpenTelemetry tracer and by the no-op one alike — so the rule can't be
 * bypassed by swapping implementations. (The MDC goes through the same allow-list: {@code LogContext}.)
 */
class ExecutionAttributesTest {

    @Test
    void an_attribute_that_is_not_an_identifier_is_rejected_by_every_tracer() throws Exception {
        List<ExecutionTracer> tracers = List.of(new OpenTelemetryTraceTest.Spans().tracer(), ExecutionTracer.NOOP);
        for (ExecutionTracer tracer : tracers) {
            assertThatThrownBy(() -> tracer.start("stint.task",
                    Map.of(TraceAttributes.INSTANCE_ID, "inst-1", "stint.input", "{\"cpf\":\"123\"}"), null, List.of()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("stint.input");
        }
    }

    @Test
    void identifiers_are_accepted() throws Exception {
        var span = new OpenTelemetryTraceTest.Spans().tracer().start("stint.task",
                Map.of(TraceAttributes.INSTANCE_ID, "inst-1", TraceAttributes.CHAIN_ID, "chn-1"), null, List.of());
        assertThat(span.context().traceparent()).startsWith("00-");
        span.close();
    }
}
