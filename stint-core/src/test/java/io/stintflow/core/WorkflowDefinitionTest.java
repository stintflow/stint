package io.stintflow.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.model.CallRemoteNode;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.model.DoNode;
import io.stintflow.core.model.FlowDirective;
import io.stintflow.spi.WorkflowRef;

/** Pointer resolution and {@code then} semantics (SDD 1.1, RF2/RF3). */
class WorkflowDefinitionTest {

    private static final WorkflowRef REF = new WorkflowRef("test", "pointers", "1.0.0");

    @Test
    void pointer_format_matches_the_do_index_name_convention() {
        WorkflowDefinition def = WorkflowBuilder.create()
                .callRemote("extractData", "route-a", DataFlow.NONE)
                .callRemote("formatFile", "route-b", DataFlow.NONE)
                .build(REF);

        assertThat(def.at("/do/0/extractData").name()).isEqualTo("extractData");
        assertThat(def.at("/do/1/formatFile").name()).isEqualTo("formatFile");
        assertThatThrownBy(() -> def.at("/do/2/nope")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void continue_moves_to_the_next_sibling_and_ends_the_workflow_after_the_last() {
        WorkflowDefinition def = WorkflowBuilder.create()
                .callRemote("a", "route-a", DataFlow.NONE)
                .callRemote("b", "route-b", DataFlow.NONE)
                .build(REF);

        assertThat(def.next("/do/0/a", FlowDirective.CONTINUE)).contains("/do/1/b");
        assertThat(def.next("/do/1/b", FlowDirective.CONTINUE)).isEmpty();
    }

    @Test
    void end_terminates_the_workflow_regardless_of_position() {
        WorkflowDefinition def = WorkflowBuilder.create()
                .callRemote("a", "route-a", DataFlow.NONE, FlowDirective.END)
                .callRemote("b", "route-b", DataFlow.NONE)
                .build(REF);

        assertThat(def.next("/do/0/a", FlowDirective.END)).isEmpty();
    }

    @Test
    void goto_jumps_to_a_named_sibling_and_rejects_an_unknown_name() {
        WorkflowDefinition def = WorkflowBuilder.create()
                .callRemote("a", "route-a", DataFlow.NONE, FlowDirective.goTo("c"))
                .callRemote("b", "route-b", DataFlow.NONE)
                .callRemote("c", "route-c", DataFlow.NONE)
                .build(REF);

        assertThat(def.next("/do/0/a", FlowDirective.goTo("c"))).contains("/do/2/c");
        assertThatThrownBy(() -> def.next("/do/0/a", FlowDirective.goTo("nope")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void exit_leaves_a_nested_do_and_continues_the_enclosing_flow() {
        DoNode nested = new DoNode("group", "/do/0/group", DataFlow.NONE, FlowDirective.CONTINUE, List.of(
                task("inner", "/do/0/group/do/0/inner", FlowDirective.EXIT)));
        DoNode root = new DoNode("root", "", DataFlow.NONE, FlowDirective.END, List.of(
                nested,
                task("after", "/do/1/after", FlowDirective.CONTINUE)));
        WorkflowDefinition def = new WorkflowDefinition(REF, root);

        assertThat(def.next("/do/0/group/do/0/inner", FlowDirective.EXIT)).contains("/do/1/after");
    }

    @Test
    void root_do_node_must_have_the_empty_pointer() {
        DoNode badRoot = new DoNode("root", "/not-root", DataFlow.NONE, FlowDirective.END, List.of());
        assertThatThrownBy(() -> new WorkflowDefinition(REF, badRoot))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static CallRemoteNode task(String name, String pointer, FlowDirective then) {
        return new CallRemoteNode(name, pointer, DataFlow.NONE, then, "route-" + name, null);
    }
}
