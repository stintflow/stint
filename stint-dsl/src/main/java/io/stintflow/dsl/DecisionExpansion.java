package io.stintflow.dsl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.node.TextNode;

import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.CallRemoteNode;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.model.FlowDirective;
import io.stintflow.core.model.SwitchNode;
import io.stintflow.core.model.TaskNode;
import io.stintflow.wire.decision.DecisionKind;
import io.stintflow.wire.decision.DecisionOption;
import io.stintflow.wire.decision.DecisionRequest;
import io.stintflow.wire.decision.DecisionResult;

/**
 * SDD 3a.1, 5.2: what a {@code call: decision} compiles to — a {@code call: remote} followed by a {@code switch}
 * named {@code <task>/route}. Kept apart from {@link DslLoader}'s validation so the expansion reads in one place.
 */
final class DecisionExpansion {

    /** The Stint extension key on a {@code call: decision} task: where an uncertain decision goes. */
    static final String ON_UNCERTAIN = "onUncertain";
    /** The name and pointer suffix of the generated {@code switch}. */
    static final String ROUTE_SUFFIX = "/route";

    private DecisionExpansion() {
    }

    /** jq source of the worker's argument: the request, with {@code inputSource} (jq) as its input. */
    static String requestTemplate(DecisionRequest shape, String inputSource) {
        return "{" + text(DecisionRequest.KIND) + ": " + text(shape.kind().wireName()) + ", "
                + text(DecisionRequest.OPTIONS) + ": " + shape.optionsJson() + ", "
                + text(DecisionRequest.INPUT) + ": (" + inputSource + ")}";
    }

    /**
     * @param authored    the task's own data flow: {@code input.from} goes to the call, {@code output.as} and
     *                    {@code export.as} to the switch, which sees the decision's result
     * @param otherwise   where an option without its own {@code then} goes, and the default of a score or of a
     *                    boolean without options
     * @param onUncertain where an uncertain decision goes, or {@code null}: it then follows its value
     * @param optionThens each option's own {@code then}, or {@code null}, in the order of {@code shape.options()}
     */
    static List<TaskNode> expand(String name, String pointer, DecisionRequest shape, String routingKey,
            Duration timeout, Expr request, DataFlow authored, FlowDirective otherwise, FlowDirective onUncertain,
            List<FlowDirective> optionThens) {
        CallRemoteNode call = new CallRemoteNode(name, pointer, new DataFlow(authored.inputFrom(), null, null),
                FlowDirective.CONTINUE, routingKey, timeout, request, new DecisionOutputContract(shape));
        List<SwitchNode.Case> cases = new ArrayList<>();
        if (onUncertain != null) {
            cases.add(new SwitchNode.Case("uncertain", Expr.jq("." + DecisionResult.UNCERTAIN + " == true"),
                    onUncertain));
        }
        List<DecisionOption> options = shape.options();
        for (int i = 0; i < options.size(); i++) {
            String option = options.get(i).name();
            String literal = shape.kind() == DecisionKind.BOOLEAN ? option : text(option);
            FlowDirective then = optionThens.get(i) != null ? optionThens.get(i) : otherwise;
            cases.add(new SwitchNode.Case(option, Expr.jq("." + DecisionResult.VALUE + " == " + literal), then));
        }
        if (options.isEmpty()) {
            cases.add(new SwitchNode.Case("default", null, otherwise));
        }
        SwitchNode route = new SwitchNode(name + ROUTE_SUFFIX, pointer + ROUTE_SUFFIX,
                new DataFlow(null, authored.outputAs(), authored.exportAs()), FlowDirective.CONTINUE, cases);
        return List.of(call, route);
    }

    private static String text(String value) {
        return TextNode.valueOf(value).toString();
    }
}
