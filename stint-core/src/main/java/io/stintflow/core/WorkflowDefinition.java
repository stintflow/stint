package io.stintflow.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.stintflow.core.model.DoNode;
import io.stintflow.core.model.FlowDirective;
import io.stintflow.core.model.TaskNode;
import io.stintflow.spi.WorkflowRef;

/**
 * A workflow definition as a tree of {@link TaskNode}s (SDD 1.1, RF2), addressed by JSON Pointer
 * (e.g. {@code /do/1/formatFile}) — the same cursor format persisted opaquely in
 * {@link io.stintflow.spi.InstanceSnapshot#position()}.
 * <p>
 * Produced today by the programmatic builder ({@code WorkflowBuilder}); a YAML/JSON front-end
 * (SDD 1.4) is a swappable, later increment that compiles to this same tree.
 */
public record WorkflowDefinition(WorkflowRef ref, DoNode root) {

    static final String ROOT_POINTER = "";

    public WorkflowDefinition {
        if (!ROOT_POINTER.equals(root.pointer())) {
            throw new IllegalArgumentException(
                    "The root DoNode must have pointer \"\", got: " + root.pointer());
        }
    }

    /** @throws IllegalArgumentException if no node exists at {@code pointer} */
    public TaskNode at(String pointer) {
        return Index.build(root).at(pointer);
    }

    /**
     * Resolves a {@link FlowDirective} taken at {@code pointer} into the next pointer to run, or
     * {@link Optional#empty()} if the workflow instance is done (its root {@code do} flow ended, or
     * an {@code end}/an {@code exit} out of the root flow was reached).
     */
    public Optional<String> next(String pointer, FlowDirective directive) {
        return Index.build(root).next(pointer, directive);
    }

    /**
     * Structural index over the tree: which node lives at which pointer, which pointers are
     * siblings of which (a "flow"), and name→pointer lookup scoped to each flow (for {@code goto}).
     * Rebuilt on demand rather than cached on the record — trees are small (bounded by the local
     * node limit) and this keeps {@link WorkflowDefinition} a plain, cheap-to-construct record.
     */
    private static final class Index {
        private final Map<String, TaskNode> byPointer;
        private final Map<String, List<String>> siblingsOf;
        private final Map<String, Map<String, String>> nameIndexOf;
        private final Map<String, String> ownerOf;

        private Index(Map<String, TaskNode> byPointer, Map<String, List<String>> siblingsOf,
                      Map<String, Map<String, String>> nameIndexOf, Map<String, String> ownerOf) {
            this.byPointer = byPointer;
            this.siblingsOf = siblingsOf;
            this.nameIndexOf = nameIndexOf;
            this.ownerOf = ownerOf;
        }

        static Index build(DoNode root) {
            Map<String, TaskNode> byPointer = new HashMap<>();
            Map<String, List<String>> siblingsOf = new HashMap<>();
            Map<String, Map<String, String>> nameIndexOf = new HashMap<>();
            Map<String, String> ownerOf = new HashMap<>();
            registerFlow(root, root.pointer(), byPointer, siblingsOf, nameIndexOf, ownerOf);
            return new Index(byPointer, siblingsOf, nameIndexOf, ownerOf);
        }

        private static void registerFlow(DoNode owner, String ownerPointer,
                Map<String, TaskNode> byPointer, Map<String, List<String>> siblingsOf,
                Map<String, Map<String, String>> nameIndexOf, Map<String, String> ownerOf) {
            byPointer.put(ownerPointer, owner);

            List<String> childPointers = new ArrayList<>();
            Map<String, String> names = new HashMap<>();
            for (TaskNode child : owner.tasks()) {
                String childPointer = child.pointer();
                childPointers.add(childPointer);
                names.put(child.name(), childPointer);
                ownerOf.put(childPointer, ownerPointer);
                byPointer.put(childPointer, child);
                if (child instanceof DoNode nestedDo) {
                    registerFlow(nestedDo, childPointer, byPointer, siblingsOf, nameIndexOf, ownerOf);
                }
            }
            siblingsOf.put(ownerPointer, List.copyOf(childPointers));
            nameIndexOf.put(ownerPointer, Map.copyOf(names));
        }

        TaskNode at(String pointer) {
            TaskNode node = byPointer.get(pointer);
            if (node == null) {
                throw new IllegalArgumentException("No such node: " + pointer);
            }
            return node;
        }

        Optional<String> next(String pointer, FlowDirective directive) {
            if (directive instanceof FlowDirective.End) {
                return Optional.empty();
            }
            if (directive instanceof FlowDirective.GoTo go) {
                String owner = requireOwner(pointer);
                String target = nameIndexOf.getOrDefault(owner, Map.of()).get(go.taskName());
                if (target == null) {
                    throw new IllegalArgumentException(
                            "Unknown 'then' target task '" + go.taskName() + "' from " + pointer);
                }
                return Optional.of(target);
            }

            String owner = requireOwner(pointer);
            if (directive instanceof FlowDirective.Continue) {
                List<String> siblings = siblingsOf.get(owner);
                int idx = siblings.indexOf(pointer);
                if (idx + 1 < siblings.size()) {
                    return Optional.of(siblings.get(idx + 1));
                }
            }
            // FlowDirective.Exit, or a Continue that reached the end of its flow: bubble one level up.
            return bubbleUp(owner);
        }

        private Optional<String> bubbleUp(String doPointer) {
            if (ROOT_POINTER.equals(doPointer)) {
                return Optional.empty();
            }
            return next(doPointer, FlowDirective.CONTINUE);
        }

        private String requireOwner(String pointer) {
            String owner = ownerOf.get(pointer);
            if (owner == null) {
                throw new IllegalArgumentException(
                        "No such node (or it is the workflow root, which has no 'then'): " + pointer);
            }
            return owner;
        }
    }
}
