package io.stintflow.core.model;

import java.util.List;

/**
 * {@code do}: an ordered sequence of tasks. The workflow root is always a {@code DoNode} with
 * {@link #pointer()} {@code ""}; nested {@code DoNode}s group sub-sequences (e.g. as future
 * {@code try}/{@code fork} bodies).
 * <p>
 * <b>Scope note (SDD 1.1):</b> only {@link #dataFlow()}'s {@code input.from} is applied, once, when
 * the interpreter enters the block (it seeds the first child's data). A {@code DoNode} is otherwise
 * transparent: reaching the end of its {@link #tasks()} bubbles directly to whatever follows the
 * {@code DoNode} itself in its enclosing flow (see {@link WorkflowDefinition#next}), without
 * separately applying this node's own {@code output.as}/{@code export.as}/{@link #then()}. Giving a
 * nested {@code DoNode} its own non-identity {@code output.as}/{@code export.as} is therefore a
 * no-op in this increment — not required by any SDD 1.1 acceptance criterion; revisit if a later
 * SDD needs it.
 */
public record DoNode(
        String name,
        String pointer,
        DataFlow dataFlow,
        FlowDirective then,
        List<TaskNode> tasks) implements TaskNode {
}
