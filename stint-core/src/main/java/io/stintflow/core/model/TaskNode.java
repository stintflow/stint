package io.stintflow.core.model;

/**
 * A node in the workflow's task tree (SDD 1.1, RF1; {@link TryNode} added in SDD 1.3, {@link EmitNode}
 * in SDD 2.2, {@link ListenNode} in SDD 2.3). {@code WaitNode} and {@code ForkNode} are extension points
 * for later SDDs, not implemented here.
 */
public sealed interface TaskNode permits DoNode, CallRemoteNode, SetNode, SwitchNode, TryNode, EmitNode,
        ListenNode {

    /** The task's name, as it appears as the single key of its {@code do} list entry. */
    String name();

    /** JSON Pointer of this node within the workflow document (e.g. {@code /do/1/formatFile}). */
    String pointer();

    DataFlow dataFlow();

    /** What runs after this node finishes. Unused/ignored for {@link DoNode} — see its Javadoc. */
    FlowDirective then();
}
