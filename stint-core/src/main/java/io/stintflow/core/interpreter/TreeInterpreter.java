package io.stintflow.core.interpreter;

import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.expr.EvalScope;
import io.stintflow.core.expr.ExpressionEvaluator;
import io.stintflow.core.model.CallRemoteNode;
import io.stintflow.core.model.DoNode;
import io.stintflow.core.model.SetNode;
import io.stintflow.core.model.SwitchNode;
import io.stintflow.core.model.TaskNode;

/**
 * The local interpreter (SDD 1.1, RF6): walks local nodes ({@code set}, {@code switch}, {@code do})
 * in sequence within one activation, applying each node's data flow, and stops at the first
 * {@link CallRemoteNode} (to suspend for dispatch) or at the end of the tree (to complete).
 * <p>
 * Pure and synchronous — no I/O, no suspension state of its own. {@link io.stintflow.core.WorkflowEngine}
 * owns dispatch, checkpointing and resumption; this class only ever answers "given this pointer,
 * data and context, what happens next".
 */
public final class TreeInterpreter {

    /** RNF2 default: guards against an infinite local loop (e.g. a {@code goto} cycle with no remote task). */
    public static final int DEFAULT_LOCAL_NODE_LIMIT = 1000;

    private final ExpressionEvaluator evaluator;
    private final int localNodeLimit;

    public TreeInterpreter(ExpressionEvaluator evaluator) {
        this(evaluator, DEFAULT_LOCAL_NODE_LIMIT);
    }

    public TreeInterpreter(ExpressionEvaluator evaluator, int localNodeLimit) {
        if (localNodeLimit <= 0) {
            throw new IllegalArgumentException("localNodeLimit must be positive: " + localNodeLimit);
        }
        this.evaluator = evaluator;
        this.localNodeLimit = localNodeLimit;
    }

    /**
     * Runs from {@code pointer} until suspension, completion or failure.
     *
     * @param data    the current flowing data ({@code .}) entering {@code pointer}
     * @param context the current {@code $context}
     */
    public InterpretResult run(WorkflowDefinition def, String pointer, JsonNode data, JsonNode context) {
        int steps = 0;
        while (true) {
            if (steps++ >= localNodeLimit) {
                return InterpretResult.failed(pointer, context, "Local node execution limit exceeded (" + localNodeLimit
                        + " nodes) at " + pointer + " — likely an infinite loop (a 'goto' cycle with no remote task)");
            }

            TaskNode node = def.at(pointer);
            JsonNode effectiveInput = DataFlowSupport.applyExpr(evaluator, node.dataFlow().inputFrom(), data, context, def.ref());

            if (node instanceof CallRemoteNode remote) {
                return InterpretResult.suspend(remote, pointer, effectiveInput, context);
            }

            if (node instanceof DoNode doNode) {
                if (doNode.tasks().isEmpty()) {
                    Optional<String> nextPointer = def.next(pointer, doNode.then());
                    if (nextPointer.isEmpty()) {
                        return InterpretResult.complete(pointer, context);
                    }
                    pointer = nextPointer.get();
                    data = effectiveInput;
                    continue;
                }
                pointer = doNode.tasks().get(0).pointer();
                data = effectiveInput;
                continue;
            }

            if (node instanceof SetNode setNode) {
                JsonNode rawOutput = setNode.set() == null ? effectiveInput
                        : evaluator.eval(setNode.set(), effectiveInput, new EvalScope(context, def.ref()));
                JsonNode outputData = DataFlowSupport.applyExpr(evaluator, node.dataFlow().outputAs(), rawOutput, context, def.ref());
                JsonNode newContext = DataFlowSupport.applyExportAs(evaluator, node.dataFlow().exportAs(), outputData, context, def.ref());

                Optional<String> nextPointer = def.next(pointer, setNode.then());
                if (nextPointer.isEmpty()) {
                    return InterpretResult.complete(pointer, newContext);
                }
                pointer = nextPointer.get();
                data = outputData;
                context = newContext;
                continue;
            }

            if (node instanceof SwitchNode switchNode) {
                SwitchNode.Case chosen = chooseCase(switchNode, effectiveInput, context, def);
                JsonNode outputData = DataFlowSupport.applyExpr(evaluator, node.dataFlow().outputAs(), effectiveInput, context, def.ref());
                JsonNode newContext = DataFlowSupport.applyExportAs(evaluator, node.dataFlow().exportAs(), outputData, context, def.ref());

                Optional<String> nextPointer = def.next(pointer, chosen.then());
                if (nextPointer.isEmpty()) {
                    return InterpretResult.complete(pointer, newContext);
                }
                pointer = nextPointer.get();
                data = outputData;
                context = newContext;
                continue;
            }

            throw new WorkflowExecutionException("Unhandled node kind at " + pointer + ": " + node.getClass());
        }
    }

    /**
     * Applies a just-completed {@link CallRemoteNode}'s {@code output.as}/{@code export.as} to its
     * raw remote result, then continues the walk (or completes) from {@code def.next(pointer, ...)}.
     * Used by {@code WorkflowEngine} on {@code onResult}, so all data-flow semantics stay in one place.
     */
    public InterpretResult resume(WorkflowDefinition def, String pointer, CallRemoteNode node,
            JsonNode rawOutput, JsonNode context) {
        JsonNode outputData = DataFlowSupport.applyExpr(evaluator, node.dataFlow().outputAs(), rawOutput, context, def.ref());
        JsonNode newContext = DataFlowSupport.applyExportAs(evaluator, node.dataFlow().exportAs(), outputData, context, def.ref());

        Optional<String> nextPointer = def.next(pointer, node.then());
        if (nextPointer.isEmpty()) {
            return InterpretResult.complete(pointer, newContext);
        }
        return run(def, nextPointer.get(), outputData, newContext);
    }

    private SwitchNode.Case chooseCase(SwitchNode switchNode, JsonNode data, JsonNode context, WorkflowDefinition def) {
        for (SwitchNode.Case c : switchNode.cases()) {
            if (c.when() == null) {
                return c;
            }
            JsonNode result = evaluator.eval(c.when(), data, new EvalScope(context, def.ref()));
            if (isTruthy(result)) {
                return c;
            }
        }
        throw new WorkflowExecutionException(
                "No 'switch' case matched and no default case (when: null) at " + switchNode.pointer());
    }

    /** jq truthiness: only {@code false} and {@code null} are falsy — {@code 0} and {@code ""} are truthy. */
    private static boolean isTruthy(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return false;
        }
        return !node.isBoolean() || node.booleanValue();
    }

}
