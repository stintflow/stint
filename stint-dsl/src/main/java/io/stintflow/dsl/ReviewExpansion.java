package io.stintflow.dsl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.TextNode;

import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.model.EmitNode;
import io.stintflow.core.model.FlowDirective;
import io.stintflow.core.model.ListenNode;
import io.stintflow.core.model.ReviewSpec;
import io.stintflow.core.model.SetNode;
import io.stintflow.core.model.SwitchNode;
import io.stintflow.core.model.TaskNode;
import io.stintflow.core.model.TryNode;
import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.wire.StintEvents;
import io.stintflow.wire.Json;
import io.stintflow.wire.decision.DecisionOption;
import io.stintflow.wire.review.ReviewContract;

/**
 * SDD 3a.4, 4.1: what a review step compiles to — {@code call: review}, or the {@code review} block of a decision's
 * {@code onUncertain}. Siblings in the same {@code do}, all DSL 1.0 plus the Stint's {@code call: remote}:
 * <ol>
 *   <li>{@code <task>}: {@code emit} of the request to the first role;</li>
 *   <li>{@code <task>/wait-1}: the first role's {@code listen}, until the reminder (or the deadline);</li>
 *   <li>with {@code remind}: {@code <task>/remind}, the reminder, and {@code <task>/wait-2}, the rest of the deadline;</li>
 *   <li>with {@code escalate}: {@code <task>/escalate}, the request to the next role, and {@code <task>/wait-3};</li>
 *   <li>with {@code onTimeout: {value}}: {@code <task>/timeout}, the default answer;</li>
 *   <li>{@code <task>/route}: a {@code switch} on the answer, with the task's (or the decision's) {@code then}s.</li>
 * </ol>
 * A phase waiting for a later one is a {@code try} whose {@code catch} of the {@code timeout} goes on; the last phase
 * of an {@code onTimeout: fail} is a plain {@code listen}, whose timeout fails the instance. Each phase correlates under
 * its own name ({@code review-1}…), so that switching phases never deletes and re-adds one wait key in one save.
 */
final class ReviewExpansion {

    static final String SOURCE = "stint://engine/review";
    static final String ROUTE_SUFFIX = "/route";

    /**
     * A review step's settings.
     *
     * @param onTimeoutValue the default answer when every phase timed out, or {@code null}: the instance fails
     */
    record Settings(String role, String material, String items, Duration deadline, Duration remind,
            String escalateRole, Duration escalateAfter, JsonNode onTimeoutValue) {
    }

    private ReviewExpansion() {
    }

    /**
     * @param kind            {@code choice} or {@code approval}
     * @param options         a choice's options (with descriptions), empty for an approval
     * @param decisionPointer the decision reviewed, or {@code null}
     * @param inputFrom       the author's {@code input.from}, for the request
     * @param routeFlow       the author's {@code output.as}/{@code export.as}, on the route
     * @param cases           the route's cases, over the answer
     */
    static List<TaskNode> expand(String name, String pointer, Settings s, String kind, List<DecisionOption> options,
            String decisionPointer, Set<String> sources, Expr inputFrom, DataFlow routeFlow,
            List<SwitchNode.Case> cases) {
        List<TaskNode> nodes = new ArrayList<>();
        List<String> optionNames = options.stream().map(DecisionOption::name).toList();
        String route = name + ROUTE_SUFFIX;
        String timeoutNode = s.onTimeoutValue() == null ? null : name + "/timeout";
        boolean remind = s.remind() != null;
        boolean escalate = s.escalateRole() != null;

        nodes.add(new EmitNode(name, pointer, new DataFlow(inputFrom, null, null), FlowDirective.CONTINUE,
                Expr.jq(requestEvent(pointer, s, kind, options, decisionPointer)), StintEvents.TYPE_REVIEW_REQUESTED));

        // phase 1: the first role, until the reminder or the deadline
        String afterPhase1 = remind ? name + "/remind" : escalate ? name + "/escalate" : timeoutNode;
        nodes.add(phase(name + "/wait-1", pointer + "/wait-1", 1, s.role(), List.of(),
                remind ? s.remind() : s.deadline(), afterPhase1, route, pointer, decisionPointer, kind, optionNames,
                sources));
        if (remind) {
            nodes.add(new EmitNode(name + "/remind", pointer + "/remind", DataFlow.NONE, FlowDirective.CONTINUE,
                    Expr.jq(phaseEvent(pointer, StintEvents.TYPE_REVIEW_REMINDER, "remind", s.role(), 0)),
                    StintEvents.TYPE_REVIEW_REMINDER));
            String afterPhase2 = escalate ? name + "/escalate" : timeoutNode;
            nodes.add(phase(name + "/wait-2", pointer + "/wait-2", 2, s.role(), List.of(),
                    s.deadline().minus(s.remind()), afterPhase2, route, pointer, decisionPointer, kind, optionNames,
                    sources));
        }
        if (escalate) {
            nodes.add(new EmitNode(name + "/escalate", pointer + "/escalate", DataFlow.NONE, FlowDirective.CONTINUE,
                    Expr.jq(phaseEvent(pointer, StintEvents.TYPE_REVIEW_REQUESTED, "escalate", s.escalateRole(),
                            s.escalateAfter().toSeconds())), StintEvents.TYPE_REVIEW_REQUESTED));
            nodes.add(phase(name + "/wait-3", pointer + "/wait-3", 3, s.escalateRole(), List.of(s.role()),
                    s.escalateAfter(), timeoutNode, route, pointer, decisionPointer, kind, optionNames, sources));
        }
        if (timeoutNode != null) {
            nodes.add(new SetNode(timeoutNode, pointer + "/timeout", DataFlow.NONE, FlowDirective.CONTINUE,
                    Expr.jq(defaultAnswer(kind, decisionPointer != null, s.onTimeoutValue()))));
        }
        nodes.add(new SwitchNode(route, pointer + ROUTE_SUFFIX, routeFlow, FlowDirective.CONTINUE, cases));
        return nodes;
    }

    /** A phase: a plain {@code listen} when nothing follows its timeout, else a {@code try} catching it. */
    private static TaskNode phase(String name, String pointer, int phase, String role, List<String> earlierRoles,
            Duration timeout, String onTimeout, String route, String reviewPointer, String decisionPointer,
            String kind, List<String> options, Set<String> sources) {
        ListenNode.Filter filter = new ListenNode.Filter(StintEvents.TYPE_REVIEW_COMPLETED,
                java.util.Map.of(StintEvents.EXT_ROLE, role),
                new java.util.TreeMap<>(java.util.Map.of("review-" + phase,
                        new ListenNode.Correlation(Expr.jq(".subject"), Expr.jq("\"the review id\"")))));
        ReviewSpec spec = new ReviewSpec(reviewPointer, decisionPointer, kind, role, earlierRoles, options, sources);
        if (onTimeout == null) {
            return new ListenNode(name, pointer, DataFlow.NONE, FlowDirective.goTo(route), ListenNode.Strategy.ONE,
                    List.of(filter), ListenNode.Read.DATA, timeout, spec);
        }
        ListenNode body = new ListenNode(name + "/listen", pointer + "/try", DataFlow.NONE, FlowDirective.CONTINUE,
                ListenNode.Strategy.ONE, List.of(filter), ListenNode.Read.DATA, timeout, spec);
        TryNode.Catch timedOut = new TryNode.Catch(Expr.jq(".type == " + text(ErrorInfo.TYPE_TIMEOUT.toString())),
                null, null, null, null, FlowDirective.goTo(onTimeout));
        return new TryNode(name, pointer, DataFlow.NONE, FlowDirective.goTo(route), body, timedOut);
    }

    private static String requestEvent(String pointer, Settings s, String kind, List<DecisionOption> options,
            String decisionPointer) {
        ArrayNode described = Json.MAPPER.createArrayNode();
        for (DecisionOption option : options) {
            described.addObject().put("name", option.name()).put("description", option.description());
        }
        return "{source: " + text(SOURCE) + ", type: " + text(StintEvents.TYPE_REVIEW_REQUESTED)
                + ", " + marks(pointer, "request", s.role())
                + ", data: {kind: " + text(kind) + ", options: " + described
                + ", items: (" + (s.items() == null ? "null" : s.items()) + ")"
                + ", materialRef: (" + s.material() + ")"
                + ", deadlineSeconds: " + s.deadline().toSeconds()
                + ", decisionPointer: " + (decisionPointer == null ? "null" : text(decisionPointer)) + "}}";
    }

    private static String phaseEvent(String pointer, String type, String phase, String role, long seconds) {
        return "{source: " + text(SOURCE) + ", type: " + text(type) + ", " + marks(pointer, phase, role)
                + ", data: {deadlineSeconds: " + seconds + "}}";
    }

    private static String marks(String pointer, String phase, String role) {
        return "stintreview: " + text(pointer) + ", stintreviewphase: " + text(phase) + ", "
                + StintEvents.EXT_ROLE + ": " + text(role);
    }

    /** The answer a review takes when no role answered in time: marked {@code defaulted}, never a correction. */
    private static String defaultAnswer(String kind, boolean ofDecision, JsonNode value) {
        if (ofDecision) {
            return "{value: " + value + ", confidence: null, uncertain: false, provenance: null, "
                    + "reviewed: {defaulted: true}}";
        }
        if (ReviewContract.APPROVAL.equals(kind)) {
            return "{verdict: " + value + ", rejectedItems: [], items: [], role: null, reviewer: null, defaulted: true}";
        }
        return "{value: " + value + ", role: null, reviewer: null, defaulted: true}";
    }

    /** A route case per option, on the answer's value, whatever its JSON type. */
    static SwitchNode.Case optionCase(String option, FlowDirective then) {
        return new SwitchNode.Case(option, Expr.jq("(.value | tostring) == " + text(option)), then);
    }

    static String text(String value) {
        return TextNode.valueOf(value).toString();
    }
}
