package io.stintflow.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import org.junit.jupiter.api.Test;

import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.CallRemoteNode;
import io.stintflow.core.model.FlowDirective;
import io.stintflow.core.model.SwitchNode;
import io.stintflow.core.model.TaskNode;
import io.stintflow.core.model.TryNode;
import io.stintflow.wire.Json;

/**
 * SDD 3a.1, CA5 (RF2/RF3): {@code call: decision} compiles to a {@code call: remote} with the decision contract
 * followed by a {@code switch}, and STRICT validates every key of it.
 */
class DslLoaderDecisionTest {

    private static final String HEADER = """
            document:
              dsl: '1.0.0'
              namespace: test
              name: decision
              version: '1.0.0'
            """;

    private static final String CLASSIFY = """
            do:
              - classify:
                  input: { from: '${ .item }' }
                  call: decision
                  with:
                    task: decide-item
                    kind: choice
                    input: '${ {text: .text} }'
                    options:
                      - approve: { description: "Approved", then: recordApproval }
                      - reject: { description: "Rejected" }
                  onUncertain: humanReview
                  timeout: { after: { seconds: 60 } }
                  output: { as: '${ {item: .value} }' }
                  export: { as: '${ $context + {decision: .value} }' }
                  then: archive
            """;

    private static WorkflowDefinition load(String tasks) {
        return new DslLoader().load(new ByteArrayInputStream((HEADER + tasks).getBytes(StandardCharsets.UTF_8)),
                "decision.yaml", LoadOptions.STRICT);
    }

    @Test
    void a_decision_compiles_to_a_remote_call_and_a_route_switch() {
        WorkflowDefinition def = load(CLASSIFY);

        assertThat(def.root().tasks()).extracting(TaskNode::name).containsExactly("classify", "classify/route");
        CallRemoteNode call = (CallRemoteNode) def.at("/do/0/classify");
        assertThat(call.routingKey()).isEqualTo("decide-item");
        assertThat(call.timeout()).isEqualTo(Duration.ofSeconds(60));
        assertThat(call.then()).isEqualTo(FlowDirective.CONTINUE);
        assertThat(call.dataFlow().inputFrom()).isEqualTo(Expr.jq(".item"));   // the author's input.from: on the call
        assertThat(call.dataFlow().outputAs()).isNull();
        assertThat(call.withInput()).isEqualTo(Expr.jq("{\"kind\": \"choice\", \"options\": [{\"name\":\"approve\","
                + "\"description\":\"Approved\"},{\"name\":\"reject\",\"description\":\"Rejected\"}], "
                + "\"input\": (({text: .text}))}"));
        assertThat(call.outputContract()).isNotNull();
        assertThat(call.outputContract().title()).isEqualTo("Decision output out of contract");
        assertThat(call.outputContract().violations(Json.obj().put("value", "maybe")))
                .contains("value is not one of the options");

        SwitchNode route = (SwitchNode) def.at("/do/0/classify/route");
        assertThat(route.dataFlow().inputFrom()).isNull();
        assertThat(route.dataFlow().outputAs()).isEqualTo(Expr.jq("{item: .value}"));   // output.as and export.as:
        assertThat(route.dataFlow().exportAs()).isEqualTo(Expr.jq("$context + {decision: .value}")); // on the switch
        assertThat(route.cases()).extracting(SwitchNode.Case::name).containsExactly("uncertain", "approve", "reject");
        assertThat(route.cases()).extracting(SwitchNode.Case::when).containsExactly(
                Expr.jq(".uncertain == true"), Expr.jq(".value == \"approve\""), Expr.jq(".value == \"reject\""));
        assertThat(route.cases()).extracting(SwitchNode.Case::then).containsExactly(
                FlowDirective.goTo("humanReview"), FlowDirective.goTo("recordApproval"), FlowDirective.goTo("archive"));
    }

    @Test
    void boolean_and_score_decisions_compile_as_specified() {
        WorkflowDefinition def = load("""
                do:
                  - unanimous:
                      call: decision
                      with:
                        task: decide
                        kind: boolean
                        options:
                          - true: { then: end }
                          - false: {}
                  - yesNo:
                      call: decision
                      with: { task: decide, kind: boolean }
                  - relevance:
                      call: decision
                      with: { task: decide, kind: score }
                      then: exit
                """);

        SwitchNode unanimous = (SwitchNode) def.at("/do/0/unanimous/route");
        assertThat(unanimous.cases()).extracting(SwitchNode.Case::when)
                .containsExactly(Expr.jq(".value == true"), Expr.jq(".value == false"));
        assertThat(unanimous.cases()).extracting(SwitchNode.Case::then)
                .containsExactly(FlowDirective.END, FlowDirective.CONTINUE);
        assertThat(((CallRemoteNode) def.at("/do/1/yesNo")).withInput())   // no with.input: the task's input as is
                .isEqualTo(Expr.jq("{\"kind\": \"boolean\", \"options\": [], \"input\": (.)}"));
        SwitchNode yesNo = (SwitchNode) def.at("/do/1/yesNo/route");
        assertThat(yesNo.cases()).singleElement().satisfies(c -> {
            assertThat(c.when()).isNull();
            assertThat(c.then()).isEqualTo(FlowDirective.CONTINUE);
        });
        SwitchNode relevance = (SwitchNode) def.at("/do/2/relevance/route");
        assertThat(relevance.cases()).singleElement().satisfies(c -> assertThat(c.then()).isEqualTo(FlowDirective.EXIT));
    }

    @Test
    void inside_a_try_only_the_remote_call_is_guarded_and_the_switch_follows_the_try() {
        WorkflowDefinition def = load("""
                do:
                  - guarded:
                      try:
                        - classify:
                            call: decision
                            with:
                              task: decide
                              kind: choice
                              options:
                                - approve: { then: approved }
                                - reject: {}
                            onUncertain: review
                      catch:
                        errors:
                          with:
                            type: https://serverlessworkflow.io/spec/1.0.0/errors/validation
                        then: fallback
                      then: done
                """);

        assertThat(def.root().tasks()).extracting(TaskNode::name).containsExactly("guarded", "classify/route");
        TryNode guarded = (TryNode) def.at("/do/0/guarded");
        assertThat(guarded.body().pointer()).isEqualTo("/do/0/guarded/do/0/classify");
        assertThat(guarded.body().outputContract()).isNotNull();
        assertThat(guarded.then()).isEqualTo(FlowDirective.CONTINUE);   // on success: on to the switch
        assertThat(guarded.catchClause().then()).isEqualTo(FlowDirective.goTo("fallback"));
        SwitchNode route = (SwitchNode) def.at("/do/0/guarded/do/0/classify/route");
        assertThat(route.cases()).extracting(SwitchNode.Case::then).containsExactly(
                FlowDirective.goTo("review"), FlowDirective.goTo("approved"), FlowDirective.goTo("done"));
    }

    @Test
    void strict_rejects_unknown_keys_of_a_decision() {
        assertViolation(CLASSIFY.replace("      kind: choice", "      kind: choice\n        model: gpt"),
                "/do/0/classify/with/model", "'model' is declared but not supported");
        assertViolation(CLASSIFY.replace("{ description: \"Rejected\" }", "{ description: \"Rejected\", weight: 2 }"),
                "/do/0/classify/with/options/1/reject/weight", "'weight' is declared but not supported");
        assertViolation(CLASSIFY.replace("      onUncertain: humanReview", "      onUncertain: humanReview\n"
                + "      sensitivity: high"), "/do/0/classify/sensitivity", "'sensitivity' is declared but not supported");
    }

    @Test
    void on_uncertain_outside_a_decision_fails_strict() {
        assertViolation("""
                do:
                  - work:
                      call: remote
                      with: { task: route }
                      onUncertain: review
                """, "/do/0/work/onUncertain", "'onUncertain' is declared but not supported");
    }

    @Test
    void kind_and_options_are_validated_at_load() {
        assertViolation(CLASSIFY.replace("kind: choice", "kind: ranking"), "/do/0/classify/with/kind",
                "requires 'with.kind': choice, score or boolean");
        assertViolation(CLASSIFY.replace("          - reject: { description: \"Rejected\" }\n", ""),
                "/do/0/classify/with/options", "a choice needs at least two options");
        assertViolation(CLASSIFY.replace("- reject:", "- approve:"), "/do/0/classify/with/options",
                "option 'approve' is declared twice");
        assertViolation("""
                do:
                  - relevance:
                      call: decision
                      with:
                        task: decide
                        kind: score
                        options:
                          - high: {}
                """, "/do/0/relevance/with/options", "a score decision has no options");
        assertViolation("""
                do:
                  - unanimous:
                      call: decision
                      with:
                        task: decide
                        kind: boolean
                        options:
                          - yes: {}
                          - no: {}
                """, "/do/0/unanimous/with/options", "exactly 'true' and 'false'");
        assertViolation(CLASSIFY.replace("        task: decide-item\n", ""), "/do/0/classify/with/task",
                "requires 'with.task'");
        assertViolation(CLASSIFY.replace("{ description: \"Rejected\" }", "{ description: 3 }"),
                "/do/0/classify/with/options/1/reject/description", "an option's description is text");
    }

    @Test
    void a_task_named_like_a_generated_route_fails_the_load() {
        assertViolation(CLASSIFY + """
                  - classify/route:
                      set: '${ . }'
                """, "/do/1/classify/route", "is used twice in the same flow");
    }

    private static void assertViolation(String tasks, String pointer, String message) {
        assertThatThrownBy(() -> load(tasks))
                .isInstanceOfSatisfying(DslValidationException.class, e -> assertThat(e.violations())
                        .anySatisfy(v -> {
                            assertThat(v.pointer()).isEqualTo(pointer);
                            assertThat(v.message()).contains(message);
                        }));
    }
}
