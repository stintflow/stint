package io.stintflow.inmemory;

import static io.stintflow.inmemory.EmitPublishTest.order;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.DataFlow;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.WorkflowRef;

/**
 * SDD 2.2, CA5 (sec. 8e): an {@code emit} can never use the engine's internal types. The load-time
 * check lives in {@code DslLoaderTest}; here: registration (a Java-built definition) and runtime (a
 * computed type, only known when the emit runs).
 */
class EmitReservedTypeTest {

    private static final WorkflowRef REF = new WorkflowRef("billing", "spoof", "1.0.0");

    @Test
    void a_literal_reserved_type_is_rejected_at_registration() {
        var def = WorkflowBuilder.create()
                .emit("spoof", "io.stintflow.task.invoke.v1",
                        Expr.jq("{source: \"https://acme.example\", type: \"io.stintflow.task.invoke.v1\"}"), DataFlow.NONE)
                .build(REF);

        assertThatThrownBy(() -> new WorkflowRegistry().register(def))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("io.stintflow.task.invoke.v1")
                .hasMessageContaining("reserved");
    }

    @Test
    void a_computed_reserved_type_fails_the_instance_and_nothing_is_recorded_or_published() throws Exception {
        // declaredType null: the registry can't know the type — the interpreter catches it at runtime.
        var def = WorkflowBuilder.create()
                .emit("spoof", null,
                        Expr.jq("{source: \"https://acme.example\", type: (\"io.stintflow.\" + .kind)}"), DataFlow.NONE)
                .build(REF);
        EmitFixture f = new EmitFixture(def);
        ObjectNode input = order("A-1", 1);
        input.put("kind", "timer.fire.v1");

        String instanceId = f.engine.start(REF, input);

        assertThat(f.load(instanceId).status()).isEqualTo(InstanceStatus.FAILED);
        assertThat(f.outbox()).isEmpty();
        assertThat(f.publisher.attempts()).isZero();
    }
}
