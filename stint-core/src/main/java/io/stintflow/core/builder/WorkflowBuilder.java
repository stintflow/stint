package io.stintflow.core.builder;

import java.util.ArrayList;
import java.util.List;

import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.CallRemoteNode;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.model.DoNode;
import io.stintflow.core.model.FlowDirective;
import io.stintflow.core.model.SetNode;
import io.stintflow.core.model.SwitchNode;
import io.stintflow.core.model.TaskNode;
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
        return callRemote(name, routingKey, dataFlow, FlowDirective.CONTINUE);
    }

    public WorkflowBuilder callRemote(String name, String routingKey, DataFlow dataFlow, FlowDirective then) {
        tasks.add(new CallRemoteNode(name, pointerFor(name), dataFlow, then, routingKey));
        return this;
    }

    public WorkflowBuilder set(String name, Expr set, DataFlow dataFlow) {
        return set(name, set, dataFlow, FlowDirective.CONTINUE);
    }

    public WorkflowBuilder set(String name, Expr set, DataFlow dataFlow, FlowDirective then) {
        tasks.add(new SetNode(name, pointerFor(name), dataFlow, then, set));
        return this;
    }

    public WorkflowBuilder switchOn(String name, DataFlow dataFlow, List<SwitchNode.Case> cases) {
        // then() is unused on SwitchNode itself — control always transfers via the matched case.
        tasks.add(new SwitchNode(name, pointerFor(name), dataFlow, FlowDirective.CONTINUE, cases));
        return this;
    }

    public WorkflowDefinition build(WorkflowRef ref) {
        DoNode root = new DoNode("root", "", DataFlow.NONE, FlowDirective.END, List.copyOf(tasks));
        return new WorkflowDefinition(ref, root);
    }

    private String pointerFor(String name) {
        return "/do/" + tasks.size() + "/" + name;
    }
}
