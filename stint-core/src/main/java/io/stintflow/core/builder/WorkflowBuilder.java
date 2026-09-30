package io.stintflow.core.builder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.CallRemoteNode;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.model.DoNode;
import io.stintflow.core.model.EmitNode;
import io.stintflow.core.model.FlowDirective;
import io.stintflow.core.model.ListenNode;
import io.stintflow.core.model.RetryPolicy;
import io.stintflow.core.model.SetNode;
import io.stintflow.core.model.SwitchNode;
import io.stintflow.core.model.TaskNode;
import io.stintflow.core.model.TryNode;
import io.stintflow.spi.WorkflowRef;

/**
 * Programmatic constructor (RF7) for a flat, root-level {@code do} sequence — for tests and
 * examples that don't need a YAML front-end (SDD 1.4). JSON Pointers are assigned automatically
 * from each task's position, matching the {@code /do/&lt;index&gt;/&lt;name&gt;} convention a parsed
 * document would produce.
 * <p>
 * Nested {@code do} blocks (needed only to exercise {@code then: exit}) aren't covered by this
 * builder; construct {@link DoNode}/{@link TaskNode} records directly for that — they're plain
 * public records, so there's no builder-only path.
 */
public final class WorkflowBuilder {

    private final List<TaskNode> tasks = new ArrayList<>();

    private WorkflowBuilder() {
    }

    public static WorkflowBuilder create() {
        return new WorkflowBuilder();
    }

    public WorkflowBuilder callRemote(String name, String routingKey, DataFlow dataFlow) {
        return callRemote(name, routingKey, dataFlow, FlowDirective.CONTINUE, null);
    }

    public WorkflowBuilder callRemote(String name, String routingKey, DataFlow dataFlow, FlowDirective then) {
        return callRemote(name, routingKey, dataFlow, then, null);
    }

    /** @param timeout {@code timeout.after}; {@code null} = engine default (SDD 1.3, RF3). */
    public WorkflowBuilder callRemote(String name, String routingKey, DataFlow dataFlow, FlowDirective then,
            Duration timeout) {
        tasks.add(new CallRemoteNode(name, pointerFor(name), dataFlow, then, routingKey, timeout));
        return this;
    }

    public WorkflowBuilder set(String name, Expr set, DataFlow dataFlow) {
        return set(name, set, dataFlow, FlowDirective.CONTINUE);
    }

    public WorkflowBuilder set(String name, Expr set, DataFlow dataFlow, FlowDirective then) {
        tasks.add(new SetNode(name, pointerFor(name), dataFlow, then, set));
        return this;
    }

    /**
     * SDD 2.2: an {@code emit}. {@code eventWith} evaluates (against the node input) to the
     * {@code emit.event.with} object; {@code declaredType} is its literal {@code type}, if any, so the
     * registry can reject a reserved one up front ({@code null} when the type is computed).
     */
    public WorkflowBuilder emit(String name, String declaredType, Expr eventWith, DataFlow dataFlow) {
        return emit(name, declaredType, eventWith, dataFlow, FlowDirective.CONTINUE);
    }

    public WorkflowBuilder emit(String name, String declaredType, Expr eventWith, DataFlow dataFlow,
            FlowDirective then) {
        tasks.add(new EmitNode(name, pointerFor(name), dataFlow, then, eventWith, declaredType));
        return this;
    }

    public WorkflowBuilder switchOn(String name, DataFlow dataFlow, List<SwitchNode.Case> cases) {
        // then() is unused on SwitchNode itself — control always transfers via the matched case.
        tasks.add(new SwitchNode(name, pointerFor(name), dataFlow, FlowDirective.CONTINUE, cases));
        return this;
    }

    /**
     * {@code try}/{@code catch}/{@code retry} (SDD 1.3, RF5) around a single remote call.
     *
     * @param bodyName    name of the inner {@code call: remote} (its own pointer nests under the try's)
     * @param routingKey  the remote call's routing key
     * @param bodyFlow    the remote call's own data flow (its {@code then} is unused — see {@link TryNode})
     * @param timeout     the remote call's {@code timeout.after}; {@code null} = engine default
     */
    public WorkflowBuilder tryRemote(String name, DataFlow dataFlow, String bodyName, String routingKey,
            DataFlow bodyFlow, Duration timeout, TryNode.Catch catchClause, FlowDirective then) {
        String tryPointer = pointerFor(name);
        CallRemoteNode body = new CallRemoteNode(bodyName, tryPointer + "/try", bodyFlow, FlowDirective.CONTINUE,
                routingKey, timeout);
        tasks.add(new TryNode(name, tryPointer, dataFlow, then, body, catchClause));
        return this;
    }

    /**
     * SDD 2.3: a {@code listen} — waits for {@code filters} according to {@code strategy}, reading each
     * consumed event as {@code read}; {@code timeout} is its {@code timeout.after} ({@code null} = none).
     */
    public WorkflowBuilder listen(String name, ListenNode.Strategy strategy, List<ListenNode.Filter> filters,
            ListenNode.Read read, Duration timeout, DataFlow dataFlow) {
        tasks.add(new ListenNode(name, pointerFor(name), dataFlow, FlowDirective.CONTINUE, strategy, filters, read,
                timeout));
        return this;
    }

    /** SDD 2.3: {@code try}/{@code catch} around a single {@code listen} — e.g. to handle its timeout. */
    public WorkflowBuilder tryListen(String name, DataFlow dataFlow, String bodyName, ListenNode.Strategy strategy,
            List<ListenNode.Filter> filters, ListenNode.Read read, Duration timeout, DataFlow bodyFlow,
            TryNode.Catch catchClause, FlowDirective then) {
        String tryPointer = pointerFor(name);
        ListenNode body = new ListenNode(bodyName, tryPointer + "/try", bodyFlow, FlowDirective.CONTINUE, strategy,
                filters, read, timeout);
        tasks.add(new TryNode(name, tryPointer, dataFlow, then, body, catchClause));
        return this;
    }

    public static TryNode.Catch catchAnyWithRetry(RetryPolicy retry, Expr compensation, FlowDirective then) {
        return new TryNode.Catch(null, null, null, retry, compensation, then);
    }

    public WorkflowDefinition build(WorkflowRef ref) {
        DoNode root = new DoNode("root", "", DataFlow.NONE, FlowDirective.END, List.copyOf(tasks));
        return new WorkflowDefinition(ref, root);
    }

    private String pointerFor(String name) {
        return "/do/" + tasks.size() + "/" + name;
    }
}
