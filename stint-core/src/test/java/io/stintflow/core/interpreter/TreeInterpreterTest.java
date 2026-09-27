package io.stintflow.core.interpreter;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.core.Json;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.model.DoNode;
import io.stintflow.core.model.FlowDirective;
import io.stintflow.core.model.SetNode;
import io.stintflow.core.model.SwitchNode;
import io.stintflow.spi.WorkflowRef;

/** CA2, CA3, CA5, CA6 — the local interpreter, in isolation from the engine's I/O. */
class TreeInterpreterTest {

    private static final WorkflowRef REF = new WorkflowRef("test", "interpreter", "1.0.0");

    private final TreeInterpreter interpreter = new TreeInterpreter(new JqExpressionEvaluator());

    // --- CA2: switch follows the first matching case, including the default ---------------------

    private WorkflowDefinition switchWorkflow() {
        return WorkflowBuilder.create()
                .switchOn("route", DataFlow.NONE, List.of(
                        new SwitchNode.Case("high", Expr.jq(".score > 80"), FlowDirective.goTo("setHigh")),
                        new SwitchNode.Case("low", Expr.jq(".score < 20"), FlowDirective.goTo("setLow")),
                        new SwitchNode.Case("default", null, FlowDirective.goTo("setDefault"))))
                .set("setHigh", null, new DataFlow(null, null, Expr.jq("$context + {branch: \"high\"}")), FlowDirective.END)
                .set("setLow", null, new DataFlow(null, null, Expr.jq("$context + {branch: \"low\"}")), FlowDirective.END)
                .set("setDefault", null, new DataFlow(null, null, Expr.jq("$context + {branch: \"default\"}")), FlowDirective.END)
                .build(REF);
    }

    @Test
    void switch_follows_the_high_branch_when_its_condition_matches() {
        assertThat(branchTakenFor(95)).isEqualTo("high");
    }

    @Test
    void switch_follows_the_low_branch_when_its_condition_matches() {
        assertThat(branchTakenFor(5)).isEqualTo("low");
    }

    @Test
    void switch_follows_the_default_branch_when_no_condition_matches() {
        assertThat(branchTakenFor(50)).isEqualTo("default");
    }

    private String branchTakenFor(int score) {
        ObjectNode input = Json.obj();
        input.put("score", score);
        InterpretResult.Complete result = (InterpretResult.Complete) interpreter.run(switchWorkflow(), "", input, input);
        return result.context().get("branch").asText();
    }

    // --- CA3: then: <name>, then: end, then: exit -------------------------------------------------

    @Test
    void then_name_jumps_to_the_named_task_skipping_the_ones_in_between() {
        WorkflowDefinition def = WorkflowBuilder.create()
                .set("setA", null, new DataFlow(null, null, null), FlowDirective.goTo("setC"))
                .set("setB", null, new DataFlow(null, null, Expr.jq("$context + {visitedB: true}")), FlowDirective.CONTINUE)
                .set("setC", null, new DataFlow(null, null, Expr.jq("$context + {visitedC: true}")), FlowDirective.END)
                .build(REF);

        ObjectNode input = Json.obj();
        InterpretResult.Complete result = (InterpretResult.Complete) interpreter.run(def, "", input, input);

        assertThat(result.context().has("visitedB")).isFalse();
        assertThat(result.context().get("visitedC").asBoolean()).isTrue();
    }

    @Test
    void then_end_terminates_the_workflow_immediately() {
        WorkflowDefinition def = WorkflowBuilder.create()
                .set("setA", null, new DataFlow(null, null, Expr.jq("$context + {done: true}")), FlowDirective.END)
                .set("setB", null, new DataFlow(null, null, Expr.jq("$context + {visitedB: true}")), FlowDirective.CONTINUE)
                .build(REF);

        ObjectNode input = Json.obj();
        InterpretResult.Complete result = (InterpretResult.Complete) interpreter.run(def, "", input, input);

        assertThat(result.context().get("done").asBoolean()).isTrue();
        assertThat(result.context().has("visitedB")).isFalse();
    }

    @Test
    void then_exit_leaves_the_current_do_and_continues_the_enclosing_flow() {
        DoNode nested = new DoNode("group", "/do/0/group", DataFlow.NONE, FlowDirective.CONTINUE, List.of(
                new SetNode("inner", "/do/0/group/do/0/inner", DataFlow.NONE, FlowDirective.EXIT, null)));
        SetNode after = new SetNode("after", "/do/1/after", new DataFlow(null, null, Expr.jq("$context + {afterRan: true}")),
                FlowDirective.END, null);
        DoNode root = new DoNode("root", "", DataFlow.NONE, FlowDirective.END, List.of(nested, after));
        WorkflowDefinition def = new WorkflowDefinition(REF, root);

        ObjectNode input = Json.obj();
        InterpretResult.Complete result = (InterpretResult.Complete) interpreter.run(def, "", input, input);

        assertThat(result.context().get("afterRan").asBoolean()).isTrue();
    }

    // --- CA5: an infinite local loop fails, citing the limit ---------------------------------------

    @Test
    void an_infinite_local_loop_fails_citing_the_node_limit() {
        WorkflowDefinition def = WorkflowBuilder.create()
                .set("pingA", null, new DataFlow(null, null, null), FlowDirective.goTo("pingB"))
                .set("pingB", null, new DataFlow(null, null, null), FlowDirective.goTo("pingA"))
                .build(REF);

        TreeInterpreter smallLimitInterpreter = new TreeInterpreter(new JqExpressionEvaluator(), 10);
        ObjectNode input = Json.obj();
        InterpretResult.Failed result = (InterpretResult.Failed) smallLimitInterpreter.run(def, "", input, input);

        assertThat(result.message()).contains("10").containsIgnoringCase("limit");
    }

    // --- CA6: input.from / output.as / export.as, isolated and combined ---------------------------

    @Test
    void input_from_transforms_the_data_seen_by_the_task() {
        WorkflowDefinition def = WorkflowBuilder.create()
                .set("selectScore", Expr.jq("."), new DataFlow(Expr.jq(".score"), null, null), FlowDirective.CONTINUE)
                .set("double", Expr.jq(". * 2"), new DataFlow(null, null, Expr.jq("$context + {doubled: .}")), FlowDirective.END)
                .build(REF);

        ObjectNode input = Json.obj();
        input.put("score", 42);
        input.put("noise", "ignored");
        InterpretResult.Complete result = (InterpretResult.Complete) interpreter.run(def, "", input, input);

        assertThat(result.context().get("doubled").asInt()).isEqualTo(84);
    }

    @Test
    void output_as_transforms_the_raw_output_before_it_flows_onward() {
        WorkflowDefinition def = WorkflowBuilder.create()
                .set("compute", Expr.jq("{a: 1, b: 2}"),
                        new DataFlow(null, Expr.jq(".a"), Expr.jq("$context + {out: .}")), FlowDirective.END)
                .build(REF);

        ObjectNode input = Json.obj();
        InterpretResult.Complete result = (InterpretResult.Complete) interpreter.run(def, "", input, input);

        assertThat(result.context().get("out").asInt()).isEqualTo(1);
        assertThat(result.context().has("b")).isFalse();
    }

    @Test
    void export_as_defaults_to_leaving_context_unchanged_not_to_the_output() {
        WorkflowDefinition def = WorkflowBuilder.create()
                .set("compute", Expr.jq("42"), new DataFlow(null, null, null), FlowDirective.END)
                .build(REF);

        ObjectNode seedContext = Json.obj();
        seedContext.put("seed", 1);
        ObjectNode input = Json.obj();
        InterpretResult.Complete result = (InterpretResult.Complete) interpreter.run(def, "", input, seedContext);

        assertThat(result.context()).isEqualTo(seedContext);
    }

    @Test
    void input_from_output_as_and_export_as_compose_in_one_node() {
        WorkflowDefinition def = WorkflowBuilder.create()
                .set("compute", Expr.jq(".n * 3"),
                        new DataFlow(Expr.jq("{n: $context.seed}"), Expr.jq(". + 1"), Expr.jq("$context + {result: .}")),
                        FlowDirective.END)
                .build(REF);

        ObjectNode seedContext = Json.obj();
        seedContext.put("seed", 10);
        JsonNode anyInput = Json.obj();
        InterpretResult.Complete result = (InterpretResult.Complete) interpreter.run(def, "", anyInput, seedContext);

        assertThat(result.context().get("result").asInt()).isEqualTo(31); // (10 * 3) + 1
    }
}
