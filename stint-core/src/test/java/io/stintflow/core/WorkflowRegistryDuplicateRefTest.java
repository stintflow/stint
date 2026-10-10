package io.stintflow.core;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.model.FlowDirective;
import io.stintflow.spi.WorkflowRef;

/** SDD 1.4, sec. 8f: re-registering the same {@link WorkflowRef} is idempotent if the content is
 *  equal, and rejected if it differs — a registered version's definition never silently changes. */
class WorkflowRegistryDuplicateRefTest {

    private static final WorkflowRef REF = new WorkflowRef("test", "dup", "1.0.0");

    @Test
    void registering_the_same_ref_with_equal_content_is_idempotent() {
        WorkflowRegistry registry = new WorkflowRegistry();
        registry.register(definition("route-a"));

        assertThatCode(() -> registry.register(definition("route-a"))).doesNotThrowAnyException();
        assertThatCode(() -> registry.register(definition("route-a"))).doesNotThrowAnyException();
    }

    @Test
    void registering_the_same_ref_with_different_content_is_rejected() {
        WorkflowRegistry registry = new WorkflowRegistry();
        registry.register(definition("route-a"));

        assertThatThrownBy(() -> registry.register(definition("route-b")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(REF.canonical())
                .hasMessageContaining("document.version");
    }

    private static WorkflowDefinition definition(String routingKey) {
        return WorkflowBuilder.create()
                .callRemote("step", routingKey, DataFlow.NONE, FlowDirective.END)
                .build(REF);
    }
}
