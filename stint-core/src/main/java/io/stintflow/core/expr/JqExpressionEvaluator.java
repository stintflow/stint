package io.stintflow.core.expr;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.wire.Json;
import io.stintflow.spi.WorkflowRef;
import net.thisptr.jackson.jq.BuiltinFunctionLoader;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.Scope;
import net.thisptr.jackson.jq.Version;
import net.thisptr.jackson.jq.Versions;
import net.thisptr.jackson.jq.exception.JsonQueryException;

/**
 * Default {@link ExpressionEvaluator}: jq (RF5), the DSL 1.0's standard expression language.
 * <p>
 * Exposes {@code .} as the current data, {@code $context} as {@link EvalScope#context()} and
 * {@code $workflow} as the {@link WorkflowDescriptor} ({@code id}, {@code input}, {@code startedAt},
 * plus namespace/name/version). Compiled queries are cached by source
 * string since {@link Expr.Jq} instances are typically re-evaluated many times (once per node
 * visit across many instances).
 */
public final class JqExpressionEvaluator implements ExpressionEvaluator {

    private static final Version JQ_VERSION = Versions.JQ_1_6;

    private final Scope rootScope = Scope.newEmptyScope();
    private final ConcurrentMap<String, JsonQuery> compiled = new ConcurrentHashMap<>();

    public JqExpressionEvaluator() {
        BuiltinFunctionLoader.getInstance()
                .loadFunctions(JqExpressionEvaluator.class.getClassLoader(), JQ_VERSION, rootScope);
    }

    @Override
    public JsonNode eval(Expr expr, JsonNode input, EvalScope scope) {
        if (expr instanceof Expr.Lambda lambda) {
            JsonNode context = scope.context() == null ? NullNode.getInstance() : scope.context();
            return lambda.fn().apply(context, input);
        }
        Expr.Jq jq = (Expr.Jq) expr;
        JsonQuery query = compiled.computeIfAbsent(jq.source(), this::compile);

        Scope callScope = Scope.newChildScope(rootScope);
        callScope.setValue("context", scope.context() == null ? NullNode.getInstance() : scope.context());
        callScope.setValue("input", input);
        if (scope.workflow() != null) {
            callScope.setValue("workflow", workflowNode(scope.workflow()));
        }

        List<JsonNode> results = new ArrayList<>(1);
        try {
            query.apply(callScope, input, results::add);
        } catch (JsonQueryException e) {
            throw new ExpressionEvaluationException("Failed to evaluate jq expression: " + jq.source(), e);
        }
        if (results.isEmpty()) {
            throw new ExpressionEvaluationException("jq expression produced no result: " + jq.source());
        }
        return results.get(0);
    }

    @Override
    public void validate(Expr expr) {
        if (expr instanceof Expr.Jq jq) {
            compiled.computeIfAbsent(jq.source(), this::compile);
        }
        // Expr.Lambda is plain Java, already compiled by the JVM — nothing to validate.
    }

    private JsonQuery compile(String source) {
        try {
            return JsonQuery.compile(source, JQ_VERSION);
        } catch (JsonQueryException e) {
            throw new ExpressionEvaluationException("Invalid jq expression: " + source, e);
        }
    }

    private static JsonNode workflowNode(WorkflowDescriptor workflow) {
        ObjectNode node = Json.obj();
        if (workflow.id() != null) {
            node.put("id", workflow.id());
        }
        if (workflow.input() != null) {
            node.set("input", workflow.input());
        }
        if (workflow.startedAt() != null) {
            node.set("startedAt", dateTimeDescriptor(workflow.startedAt()));
        }
        WorkflowRef ref = workflow.ref();
        if (ref != null) {
            node.put("namespace", ref.namespace());
            node.put("name", ref.name());
            node.put("version", ref.version());
        }
        return node;
    }

    /** DSL 1.0 {@code dsl.md}, DateTime Descriptor: {@code iso8601} plus {@code epoch.seconds}/{@code epoch.milliseconds}. */
    private static JsonNode dateTimeDescriptor(Instant instant) {
        ObjectNode node = Json.obj();
        node.put("iso8601", instant.toString());
        ObjectNode epoch = node.putObject("epoch");
        epoch.put("seconds", instant.getEpochSecond());
        epoch.put("milliseconds", instant.toEpochMilli());
        return node;
    }
}
