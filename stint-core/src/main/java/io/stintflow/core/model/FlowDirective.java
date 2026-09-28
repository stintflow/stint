package io.stintflow.core.model;

/**
 * A task's {@code then} directive (DSL 1.0, RF3): what runs next once the task finishes.
 * {@code continue} moves to the next sibling task (or, if last, bubbles up — see
 * {@link WorkflowDefinition#next}); {@code end} terminates the whole workflow instance regardless
 * of nesting; {@code exit} terminates only the current {@code do} block, then continues in the
 * enclosing flow; {@code goTo(name)} jumps to a named sibling task in the same flow.
 */
public sealed interface FlowDirective {

    FlowDirective CONTINUE = new Continue();
    FlowDirective END = new End();
    FlowDirective EXIT = new Exit();

    record Continue() implements FlowDirective {
    }

    record End() implements FlowDirective {
    }

    record Exit() implements FlowDirective {
    }

    record GoTo(String taskName) implements FlowDirective {
        public GoTo {
            if (taskName == null || taskName.isBlank()) {
                throw new IllegalArgumentException("taskName must not be blank");
            }
        }
    }

    static FlowDirective goTo(String taskName) {
        return new GoTo(taskName);
    }
}
