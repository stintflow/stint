package io.stintflow.core;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.model.DataFlow;
import io.stintflow.spi.WorkflowRef;

/** SDD 1.3, sec. 8d: {@code timeout.after} is validated against the timer's max delay at registration, not execution. */
class WorkflowRegistryTimeoutValidationTest {

    @Test
    void registering_a_task_with_a_timeout_beyond_the_timers_max_delay_is_rejected() {
        WorkflowRef ref = new WorkflowRef("test", "too-long-timeout", "1.0.0");
        WorkflowDefinition def = WorkflowBuilder.create()
                .callRemote("slow", "route-slow", DataFlow.NONE, io.stintflow.core.model.FlowDirective.END,
                        Duration.ofMinutes(20))
                .build(ref);

        WorkflowRegistry registry = new WorkflowRegistry(Duration.ofMinutes(15));

        assertThatThrownBy(() -> registry.register(def))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("timeout.after")
                .hasMessageContaining("PT20M");
    }

    @Test
    void a_timeout_within_the_limit_registers_fine() {
        WorkflowRef ref = new WorkflowRef("test", "ok-timeout", "1.0.0");
        WorkflowDefinition def = WorkflowBuilder.create()
                .callRemote("fast", "route-fast", DataFlow.NONE, io.stintflow.core.model.FlowDirective.END,
                        Duration.ofMinutes(10))
                .build(ref);

        WorkflowRegistry registry = new WorkflowRegistry(Duration.ofMinutes(15));

        assertThatCode(() -> registry.register(def)).doesNotThrowAnyException();
    }
}
