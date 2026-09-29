package io.stintflow.core.interpreter;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.expr.EvalScope;
import io.stintflow.core.expr.ExpressionEvaluator;
import io.stintflow.core.expr.WorkflowDescriptor;
import io.stintflow.core.model.CallRemoteNode;
import io.stintflow.core.model.DoNode;
import io.stintflow.core.model.EmitNode;
import io.stintflow.core.model.SetNode;
import io.stintflow.core.model.SwitchNode;
import io.stintflow.core.model.TaskNode;
import io.stintflow.core.model.TryNode;
import io.stintflow.spi.wire.StintEvents;

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
        return run(def, WorkflowDescriptor.of(def.ref()), pointer, data, context);
    }

    /** As {@link #run(WorkflowDefinition, String, JsonNode, JsonNode)}, with {@code $workflow} describing a running instance (SDD 2.1, RF6). */
    public InterpretResult run(WorkflowDefinition def, WorkflowDescriptor wf, String pointer, JsonNode data, JsonNode context) {
        List<Emitted> emitted = new ArrayList<>();
        return walk(def, wf, pointer, data, context, emitted).withEmitted(emitted);
    }

    /** The local walk; every {@code emit} on the way is appended to {@code emitted} (SDD 2.2). */
    private InterpretResult walk(WorkflowDefinition def, WorkflowDescriptor wf, String pointer, JsonNode data,
            JsonNode context, List<Emitted> emitted) {
        int steps = 0;
        while (true) {
            if (steps++ >= localNodeLimit) {
                return InterpretResult.failed(pointer, context, "Local node execution limit exceeded (" + localNodeLimit
                        + " nodes) at " + pointer + " — likely an infinite loop (a 'goto' cycle with no remote task)");
            }

            TaskNode node = def.at(pointer);
            JsonNode effectiveInput = DataFlowSupport.applyExpr(evaluator, node.dataFlow().inputFrom(), data, context, wf);

            if (node instanceof CallRemoteNode remote) {
                return InterpretResult.suspend(remote, pointer, effectiveInput, context);
            }

            if (node instanceof TryNode tryNode) {
                return InterpretResult.suspendInTry(tryNode, effectiveInput, context);
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
                        : evaluator.eval(setNode.set(), effectiveInput, new EvalScope(context, wf));
                JsonNode outputData = DataFlowSupport.applyExpr(evaluator, node.dataFlow().outputAs(), rawOutput, context, wf);
                JsonNode newContext = DataFlowSupport.applyExportAs(evaluator, node.dataFlow().exportAs(), outputData, context, wf);

                Optional<String> nextPointer = def.next(pointer, setNode.then());
                if (nextPointer.isEmpty()) {
                    return InterpretResult.complete(pointer, newContext);
                }
                pointer = nextPointer.get();
                data = outputData;
                context = newContext;
                continue;
            }

            if (node instanceof EmitNode emitNode) {
                JsonNode with = evaluator.eval(emitNode.event(), effectiveInput, new EvalScope(context, wf));
                String problem = emitProblem(with);
                if (problem != null) {
                    return InterpretResult.failed(pointer, context, "emit at " + pointer + ": " + problem);
                }
                String emitPointer = pointer;
                int occurrence = (int) emitted.stream().filter(e -> e.pointer().equals(emitPointer)).count();
                emitted.add(new Emitted(pointer, occurrence, with));

                // The spec doesn't define an emit's output: its raw output is its input, unchanged.
                JsonNode outputData = DataFlowSupport.applyExpr(evaluator, node.dataFlow().outputAs(), effectiveInput, context, wf);
                JsonNode newContext = DataFlowSupport.applyExportAs(evaluator, node.dataFlow().exportAs(), outputData, context, wf);

                Optional<String> nextPointer = def.next(pointer, emitNode.then());
                if (nextPointer.isEmpty()) {
                    return InterpretResult.complete(pointer, newContext);
                }
                pointer = nextPointer.get();
                data = outputData;
                context = newContext;
                continue;
            }

            if (node instanceof SwitchNode switchNode) {
                SwitchNode.Case chosen = chooseCase(switchNode, effectiveInput, context, wf);
                JsonNode outputData = DataFlowSupport.applyExpr(evaluator, node.dataFlow().outputAs(), effectiveInput, context, wf);
                JsonNode newContext = DataFlowSupport.applyExportAs(evaluator, node.dataFlow().exportAs(), outputData, context, wf);

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
    public InterpretResult resume(WorkflowDefinition def, WorkflowDescriptor wf, String pointer, CallRemoteNode node,
            JsonNode rawOutput, JsonNode context) {
        JsonNode outputData = DataFlowSupport.applyExpr(evaluator, node.dataFlow().outputAs(), rawOutput, context, wf);
        JsonNode newContext = DataFlowSupport.applyExportAs(evaluator, node.dataFlow().exportAs(), outputData, context, wf);

        Optional<String> nextPointer = def.next(pointer, node.then());
        if (nextPointer.isEmpty()) {
            return InterpretResult.complete(pointer, newContext);
        }
        return run(def, wf, nextPointer.get(), outputData, newContext);
    }

    /**
     * SDD 1.3: applies a just-succeeded {@link TryNode} body's {@code output.as}/{@code export.as}
     * and continues via the {@code TryNode}'s own {@code then} (not the body's, which is unused for
     * a try'd call — mirrors {@link #resume}).
     */
    public InterpretResult resumeTry(WorkflowDefinition def, WorkflowDescriptor wf, TryNode tryNode, JsonNode rawOutput, JsonNode context) {
        CallRemoteNode body = tryNode.body();
        JsonNode outputData = DataFlowSupport.applyExpr(evaluator, body.dataFlow().outputAs(), rawOutput, context, wf);
        JsonNode newContext = DataFlowSupport.applyExportAs(evaluator, body.dataFlow().exportAs(), outputData, context, wf);

        Optional<String> nextPointer = def.next(tryNode.pointer(), tryNode.then());
        if (nextPointer.isEmpty()) {
            return InterpretResult.complete(tryNode.pointer(), newContext);
        }
        return run(def, wf, nextPointer.get(), outputData, newContext);
    }

    /** Continues from {@code catchClause.then()} once a caught error has been handled (SDD 1.3). */
    public InterpretResult resumeFromCatch(WorkflowDefinition def, WorkflowDescriptor wf, TryNode tryNode, JsonNode data, JsonNode context) {
        Optional<String> nextPointer = def.next(tryNode.pointer(), tryNode.catchClause().then());
        if (nextPointer.isEmpty()) {
            return InterpretResult.complete(tryNode.pointer(), context);
        }
        return run(def, wf, nextPointer.get(), data, context);
    }

    /** Exposed so {@code WorkflowEngine} can evaluate a {@code TryNode}'s catch filter/compensation
     *  with the exact same {@link io.stintflow.core.expr.ExpressionEvaluator} the rest of the tree uses. */
    public ExpressionEvaluator evaluator() {
        return evaluator;
    }

    private SwitchNode.Case chooseCase(SwitchNode switchNode, JsonNode data, JsonNode context, WorkflowDescriptor wf) {
        for (SwitchNode.Case c : switchNode.cases()) {
            if (c.when() == null) {
                return c;
            }
            JsonNode result = evaluator.eval(c.when(), data, new EvalScope(context, wf));
            if (isTruthy(result)) {
                return c;
            }
        }
        throw new WorkflowExecutionException(
                "No 'switch' case matched and no default case (when: null) at " + switchNode.pointer());
    }

    /**
     * SDD 2.2, RF2/sec. 8e: {@code emit.event.with} must evaluate to an object with a {@code source} and
     * a {@code type} (DSL 1.0: "Required when emitting an event using emit.event.with"), and the type may
     * not be one of the internal protocol's — checked here because a computed type is only known now.
     */
    private static String emitProblem(JsonNode with) {
        if (with == null || !with.isObject()) {
            return "event.with must evaluate to an object";
        }
        for (String required : List.of("source", "type")) {
            JsonNode value = with.get(required);
            if (value == null || !value.isTextual() || value.asText().isBlank()) {
                return "event.with." + required + " is required";
            }
        }
        String type = with.get("type").asText();
        if (StintEvents.isReservedType(type)) {
            return "type '" + type + "' is reserved for the engine's internal protocol "
                    + StintEvents.RESERVED_TYPE_PREFIXES;
        }
        var fields = with.fields();
        while (fields.hasNext()) {
            var field = fields.next();
            if (EMIT_ATTRIBUTES.contains(field.getKey())) {
                continue;
            }
            // Extension attribute: CloudEvents names are lowercase letters/digits; values are scalars.
            if (!field.getKey().matches("[a-z0-9]{1,20}")) {
                return "extension attribute name '" + field.getKey() + "' is not a valid CloudEvents name "
                        + "(lowercase letters and digits, at most 20)";
            }
            JsonNode value = field.getValue();
            if (!(value.isTextual() || value.isBoolean() || value.canConvertToInt())) {
                return "extension attribute '" + field.getKey() + "' must be a string, boolean or integer";
            }
        }
        return null;
    }

    private static final Set<String> EMIT_ATTRIBUTES = Set.of(
            "id", "source", "type", "time", "subject", "datacontenttype", "dataschema", "data", "specversion");

    /** jq truthiness: only {@code false} and {@code null} are falsy — {@code 0} and {@code ""} are truthy. */
    private static boolean isTruthy(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return false;
        }
        return !node.isBoolean() || node.booleanValue();
    }

}
