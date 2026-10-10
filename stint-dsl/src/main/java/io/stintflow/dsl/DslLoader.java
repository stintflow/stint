package io.stintflow.dsl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.expr.ExpressionEvaluationException;
import io.stintflow.core.expr.ExpressionEvaluator;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.model.CallRemoteNode;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.model.DoNode;
import io.stintflow.core.model.FlowDirective;
import io.stintflow.core.model.RetryPolicy;
import io.stintflow.core.model.SetNode;
import io.stintflow.core.model.SwitchNode;
import io.stintflow.core.model.TaskNode;
import io.stintflow.core.model.TryNode;
import io.stintflow.spi.WorkflowRef;

/**
 * Compiles a CNCF Serverless Workflow DSL 1.0 YAML/JSON document into a {@link WorkflowDefinition}
 * (SDD 1.4, RF1). Pure Jackson (no CNCF sdk-java, see sec. 8e); expressions are compiled at load
 * time with the same {@link ExpressionEvaluator} the engine uses at runtime (sec. 8b); a construct
 * this phase does not implement fails the load unless {@link LoadOptions#ignoreUnsupported()} is
 * set, in which case it is dropped with a logged warning (RF3).
 * <p>
 * {@code stint-dsl} depends on {@code stint-core}; the reverse is forbidden (RNF2, enforced by
 * {@code ArchitectureTest}) — this class never touches a connector or the {@code WorkflowRegistry}.
 * Timer/routing validation stays there (SDD 1.3, sec. 8c/8d), on purpose.
 */
public final class DslLoader {

    private static final Logger LOG = System.getLogger(DslLoader.class.getName());
    private static final YAMLMapper YAML = new YAMLMapper();

    private final ExpressionEvaluator evaluator;

    public DslLoader() {
        this(new JqExpressionEvaluator());
    }

    public DslLoader(ExpressionEvaluator evaluator) {
        this.evaluator = evaluator;
    }

    public WorkflowDefinition load(InputStream in, String sourceName, LoadOptions opts) {
        byte[] bytes = readAll(in, sourceName);
        JsonNode root = parseYaml(bytes, sourceName);
        YamlLocationIndex locations = tryBuildLocationIndex(bytes);
        return new Compiler(sourceName, opts, evaluator, locations).compileDocument(root);
    }

    public List<WorkflowDefinition> loadClasspath(String pattern, LoadOptions opts) {
        List<String> resources = ClasspathManifest.resolve(pattern, opts);
        List<WorkflowDefinition> defs = new ArrayList<>(resources.size());
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) {
            cl = DslLoader.class.getClassLoader();
        }
        for (String resource : resources) {
            try (InputStream in = cl.getResourceAsStream(resource)) {
                if (in == null) {
                    throw new DslValidationException(resource, List.of(new DslValidationException.Violation(
                            "", "Resource listed in the manifest was not found on the classpath", null)));
                }
                defs.add(load(in, resource, opts));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return defs;
    }

    private static byte[] readAll(InputStream in, String sourceName) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            in.transferTo(out);
            return out.toByteArray();
        } catch (IOException e) {
            throw new DslValidationException(sourceName, List.of(
                    new DslValidationException.Violation("", "Failed to read the document: " + e.getMessage(), null)));
        }
    }

    private static JsonNode parseYaml(byte[] bytes, String sourceName) {
        try {
            return YAML.readTree(bytes);
        } catch (IOException e) {
            throw new DslValidationException(sourceName, List.of(
                    new DslValidationException.Violation("", "Invalid YAML: " + e.getMessage(), null)));
        }
    }

    private static YamlLocationIndex tryBuildLocationIndex(byte[] bytes) {
        try {
            return YamlLocationIndex.build(bytes);
        } catch (IOException e) {
            LOG.log(Level.DEBUG, "Could not build a line index for error messages; continuing without line numbers", e);
            return YamlLocationIndex.EMPTY;
        }
    }

    /** Per-load mutable compiler state — a fresh instance backs every {@link #load} call. */
    private static final class Compiler {
        private final String sourceName;
        private final LoadOptions opts;
        private final ExpressionEvaluator evaluator;
        private final YamlLocationIndex locations;
        private final List<DslValidationException.Violation> hardErrors = new ArrayList<>();

        Compiler(String sourceName, LoadOptions opts, ExpressionEvaluator evaluator, YamlLocationIndex locations) {
            this.sourceName = sourceName;
            this.opts = opts;
            this.evaluator = evaluator;
            this.locations = locations;
        }

        WorkflowDefinition compileDocument(JsonNode root) {
            JsonNode document = require(root, "document", "/");
            String namespace = document.path("namespace").asText("default");
            String name = requireText(document, "name", "/document/name");
            String version = requireText(document, "version", "/document/version");
            WorkflowRef ref = new WorkflowRef(namespace, name, version);

            if (root.has("schedule")) {
                unsupported("/schedule", "top-level 'schedule' is not supported in this phase (Fase 2)");
            }

            JsonNode doArray = root.get("do");
            List<TaskNode> tasks = doArray != null && doArray.isArray()
                    ? compileTaskList(doArray, "")
                    : List.of();

            failIfAnyHardErrors();

            DoNode rootNode = new DoNode("root", "", DataFlow.NONE, FlowDirective.END, tasks);
            return new WorkflowDefinition(ref, rootNode);
        }

        /** Compiles a {@code do}/{@code try}-style list of single-key task maps into pointered nodes. */
        private List<TaskNode> compileTaskList(JsonNode listNode, String parentPointer) {
            List<TaskNode> tasks = new ArrayList<>();
            for (int i = 0; i < listNode.size(); i++) {
                JsonNode entry = listNode.get(i);
                Iterator<String> names = entry.fieldNames();
                if (!names.hasNext()) {
                    hardError(parentPointer + "/do/" + i, "empty task entry", null);
                    continue;
                }
                String taskName = names.next();
                JsonNode body = entry.get(taskName);
                String pointer = parentPointer + "/do/" + i + "/" + taskName;
                TaskNode compiled = compileTask(taskName, body, pointer);
                if (compiled != null) {
                    tasks.add(compiled);
                }
            }
            return tasks;
        }

        private TaskNode compileTask(String name, JsonNode body, String pointer) {
            if (body.has("call")) {
                return compileCallRemote(name, body, pointer);
            }
            if (body.has("set")) {
                Expr setExpr = compileAndValidate(TemplateCompiler.compileTemplate(body.get("set")), pointer + "/set");
                return new SetNode(name, pointer, compileDataFlow(body, pointer, null, null), compileThen(body), setExpr);
            }
            if (body.has("switch")) {
                return compileSwitch(name, body, pointer);
            }
            if (body.has("try")) {
                return compileTry(name, body, pointer);
            }
            for (String unsupportedKey : List.of("emit", "listen", "fork", "wait", "for")) {
                if (body.has(unsupportedKey)) {
                    unsupported(pointer + "/" + unsupportedKey,
                            "task construct '" + unsupportedKey + "' is not supported in this phase (Fase 2)");
                    return null;
                }
            }
            hardError(pointer, "task has no recognized construct (call/set/switch/try)", null);
            return null;
        }

        private TaskNode compileCallRemote(String name, JsonNode body, String pointer) {
            String callType = body.get("call").asText();
            if (!"remote".equals(callType)) {
                unsupported(pointer + "/call",
                        "call type '" + callType + "' is not implemented yet — only the Stint extension "
                                + "'remote' is supported this phase (SDD 1.4, sec. 8d)");
                return null;
            }
            JsonNode with = body.get("with");
            if (with == null || !with.hasNonNull("task")) {
                hardError(pointer + "/with", "'call: remote' requires 'with.task' (the routing key)", null);
                return null;
            }
            String routingKey = with.get("task").asText();
            Duration timeout = compileTimeout(body, pointer);
            JsonNode remoteInput = with.get("input");
            DataFlow dataFlow = compileDataFlow(body, pointer, remoteInput, pointer + "/with/input");
            return new CallRemoteNode(name, pointer, dataFlow, compileThen(body), routingKey, timeout);
        }

        private TaskNode compileSwitch(String name, JsonNode body, String pointer) {
            JsonNode casesArr = body.get("switch");
            List<SwitchNode.Case> cases = new ArrayList<>();
            for (int i = 0; i < casesArr.size(); i++) {
                JsonNode entry = casesArr.get(i);
                Iterator<String> names = entry.fieldNames();
                if (!names.hasNext()) {
                    continue;
                }
                String caseName = names.next();
                JsonNode caseBody = entry.get(caseName);
                String casePointer = pointer + "/switch/" + i;
                Expr when = caseBody.has("when")
                        ? compileAndValidate(TemplateCompiler.compileBareExpression(caseBody.get("when").asText()), casePointer + "/when")
                        : null;
                cases.add(new SwitchNode.Case(caseName, when, compileThen(caseBody)));
            }
            return new SwitchNode(name, pointer, compileDataFlow(body, pointer, null, null), FlowDirective.CONTINUE, cases);
        }

        private TaskNode compileTry(String name, JsonNode body, String pointer) {
            JsonNode tryList = body.get("try");
            if (tryList == null || !tryList.isArray() || tryList.size() != 1) {
                hardError(pointer + "/try",
                        "'try' bodies with other than exactly one task are not supported in this phase "
                                + "(SDD 1.3 scope note: the try body is restricted to a single 'call: remote')",
                        null);
                return null;
            }
            List<TaskNode> bodyTasks = compileTaskList(tryList, pointer);
            if (bodyTasks.size() != 1 || !(bodyTasks.get(0) instanceof CallRemoteNode bodyCall)) {
                hardError(pointer + "/try",
                        "'try' body must compile to a single 'call: remote' task (SDD 1.3 scope note)", null);
                return null;
            }

            JsonNode catchNode = body.get("catch");
            if (catchNode == null) {
                hardError(pointer + "/catch", "'try' requires a 'catch' clause", null);
                return null;
            }
            TryNode.Catch catchClause = compileCatch(catchNode, pointer + "/catch");
            return new TryNode(name, pointer, compileDataFlow(body, pointer, null, null), compileThen(body), bodyCall, catchClause);
        }

        private TryNode.Catch compileCatch(JsonNode catchNode, String pointer) {
            Expr errorFilter = compileErrorFilter(catchNode.get("errors"), pointer + "/errors");
            Expr when = catchNode.has("when")
                    ? compileAndValidate(TemplateCompiler.compileBareExpression(catchNode.get("when").asText()), pointer + "/when")
                    : null;
            Expr exceptWhen = catchNode.has("exceptWhen")
                    ? compileAndValidate(TemplateCompiler.compileBareExpression(catchNode.get("exceptWhen").asText()), pointer + "/exceptWhen")
                    : null;

            RetryPolicy retry = compileRetry(catchNode.get("retry"), pointer + "/retry");
            Expr compensation = compileCompensation(catchNode.get("do"), pointer + "/do");
            FlowDirective then = compileThen(catchNode);

            if (catchNode.has("as") && !"error".equals(catchNode.get("as").asText())) {
                LOG.log(Level.WARNING, "{0}/as: custom error variable names are not supported yet — "
                        + "the caught error is always exposed as '.' to errors/when/exceptWhen/retry/do", pointer);
            }

            return new TryNode.Catch(errorFilter, when, exceptWhen, retry, compensation, then);
        }

        private Expr compileErrorFilter(JsonNode errors, String pointer) {
            if (errors == null) {
                return null;
            }
            JsonNode with = errors.get("with");
            if (with == null || with.isNull()) {
                return null;
            }
            StringBuilder jq = new StringBuilder();
            Iterator<String> fields = with.fieldNames();
            boolean first = true;
            while (fields.hasNext()) {
                String field = fields.next();
                if (!first) {
                    jq.append(" and ");
                }
                first = false;
                jq.append('(').append('.').append(field).append(" == ").append(TemplateCompiler.compileTemplate(with.get(field))).append(')');
            }
            if (jq.isEmpty()) {
                return null;
            }
            return compileAndValidate(jq.toString(), pointer + "/with");
        }

        private RetryPolicy compileRetry(JsonNode retry, String pointer) {
            if (retry == null || retry.isNull()) {
                return null;
            }
            if (retry.isTextual()) {
                unsupported(pointer, "named retry policy references (use.retries catalogs) are not supported yet");
                return null;
            }
            if (retry.has("when") || retry.has("exceptWhen")) {
                unsupported(pointer, "retry-level 'when'/'exceptWhen' guards are not supported yet (ignored)");
            }

            Duration delay = retry.has("delay") ? compileDurationSpec(retry.get("delay")) : Duration.ZERO;
            RetryPolicy.Backoff backoff = RetryPolicy.Backoff.CONSTANT;
            JsonNode backoffNode = retry.get("backoff");
            if (backoffNode != null) {
                if (backoffNode.has("linear")) {
                    backoff = RetryPolicy.Backoff.LINEAR;
                } else if (backoffNode.has("exponential")) {
                    backoff = RetryPolicy.Backoff.EXPONENTIAL;
                }
            }

            int maxAttempts = 1;
            Duration maxDuration = null;
            JsonNode limit = retry.get("limit");
            if (limit != null) {
                JsonNode attempt = limit.get("attempt");
                if (attempt != null && attempt.has("count")) {
                    maxAttempts = attempt.get("count").asInt();
                }
                if (limit.has("duration")) {
                    maxDuration = compileDurationSpec(limit.get("duration"));
                } else if (attempt != null && attempt.has("duration")) {
                    maxDuration = compileDurationSpec(attempt.get("duration"));
                }
            }

            double jitterRatio = 0.0;
            if (retry.has("jitter")) {
                LOG.log(Level.WARNING, "{0}/jitter: jitter.from/to is not supported yet (no jitter applied)", pointer);
            }

            return new RetryPolicy(delay, backoff, maxAttempts, maxDuration, jitterRatio);
        }

        /** SDD 1.3, {@code TryNode} javadoc scope note: compensation is a single local expression —
         *  only a lone {@code set} task in {@code catch.do} is supported. */
        private Expr compileCompensation(JsonNode doNode, String pointer) {
            if (doNode == null || doNode.isNull()) {
                return null;
            }
            if (!doNode.isArray() || doNode.size() != 1 || !doNode.get(0).fieldNames().hasNext()) {
                unsupported(pointer, "'catch.do' with anything other than a single 'set' task is not "
                        + "supported yet (TryNode's compensation is a single local expression)");
                return null;
            }
            JsonNode entry = doNode.get(0);
            String taskName = entry.fieldNames().next();
            JsonNode taskBody = entry.get(taskName);
            if (!taskBody.has("set")) {
                unsupported(pointer, "'catch.do' with anything other than a single 'set' task is not "
                        + "supported yet (TryNode's compensation is a single local expression)");
                return null;
            }
            return compileAndValidate(TemplateCompiler.compileTemplate(taskBody.get("set")), pointer + "/0/" + taskName + "/set");
        }

        private DataFlow compileDataFlow(JsonNode body, String pointer, JsonNode fallbackInputSource, String fallbackPointer) {
            Expr inputFrom;
            JsonNode inputHolder = body.get("input");
            if (inputHolder != null && inputHolder.has("from")) {
                inputFrom = compileAndValidate(
                        TemplateCompiler.compileBareExpression(inputHolder.get("from").asText()), pointer + "/input/from");
            } else if (fallbackInputSource != null) {
                inputFrom = compileAndValidate(TemplateCompiler.compileTemplate(fallbackInputSource), fallbackPointer);
            } else {
                inputFrom = null;
            }

            Expr outputAs = compileOptionalBareExpression(body, "output", "as", pointer);
            Expr exportAs = compileOptionalBareExpression(body, "export", "as", pointer);
            return new DataFlow(inputFrom, outputAs, exportAs);
        }

        private Expr compileOptionalBareExpression(JsonNode body, String holderKey, String valueKey, String pointer) {
            JsonNode holder = body.get(holderKey);
            if (holder == null || !holder.hasNonNull(valueKey)) {
                return null;
            }
            return compileAndValidate(TemplateCompiler.compileBareExpression(holder.get(valueKey).asText()),
                    pointer + "/" + holderKey + "/" + valueKey);
        }

        private FlowDirective compileThen(JsonNode body) {
            JsonNode then = body.get("then");
            if (then == null || then.isNull()) {
                return FlowDirective.CONTINUE;
            }
            String v = then.asText();
            return switch (v) {
                case "continue" -> FlowDirective.CONTINUE;
                case "end" -> FlowDirective.END;
                case "exit" -> FlowDirective.EXIT;
                default -> FlowDirective.goTo(v);
            };
        }

        private Duration compileTimeout(JsonNode body, String pointer) {
            JsonNode timeout = body.get("timeout");
            if (timeout == null || timeout.isNull()) {
                return null;
            }
            JsonNode after = timeout.get("after");
            if (after == null) {
                return null;
            }
            return compileDurationSpec(after);
        }

        private static Duration compileDurationSpec(JsonNode node) {
            if (node.isTextual()) {
                return Duration.parse(node.asText());
            }
            Duration d = Duration.ZERO;
            if (node.has("days")) {
                d = d.plusDays(node.get("days").asLong());
            }
            if (node.has("hours")) {
                d = d.plusHours(node.get("hours").asLong());
            }
            if (node.has("minutes")) {
                d = d.plusMinutes(node.get("minutes").asLong());
            }
            if (node.has("seconds")) {
                d = d.plusSeconds(node.get("seconds").asLong());
            }
            if (node.has("milliseconds")) {
                d = d.plusMillis(node.get("milliseconds").asLong());
            }
            return d;
        }

        /** Compiles jq source and validates it eagerly (SDD 1.4, sec. 8b) — syntax errors fail the load. */
        private Expr compileAndValidate(String jqSource, String pointer) {
            Expr expr = Expr.jq(jqSource);
            try {
                evaluator.validate(expr);
            } catch (ExpressionEvaluationException e) {
                hardError(pointer, "invalid expression: " + e.getMessage(), null);
            }
            return expr;
        }

        private void unsupported(String pointer, String message) {
            if (opts.ignoreUnsupported()) {
                LOG.log(Level.WARNING, "{0}: {1} (ignored)", pointer, message);
            } else {
                hardError(pointer, message, null);
            }
        }

        private void hardError(String pointer, String message, Integer explicitLine) {
            Integer line = explicitLine != null ? explicitLine : locations.lineOf(pointer);
            hardErrors.add(new DslValidationException.Violation(pointer, message, line));
        }

        private void failIfAnyHardErrors() {
            if (!hardErrors.isEmpty()) {
                throw new DslValidationException(sourceName, hardErrors);
            }
        }

        private JsonNode require(JsonNode node, String field, String pointer) {
            JsonNode value = node.get(field);
            if (value == null) {
                hardError(pointer.equals("/") ? "/" + field : pointer + "/" + field, "missing required field '" + field + "'", null);
                failIfAnyHardErrors();
            }
            return value;
        }

        private String requireText(JsonNode node, String field, String pointer) {
            JsonNode value = node.get(field);
            if (value == null || value.asText().isBlank()) {
                hardError(pointer, "missing required field '" + field + "'", null);
                failIfAnyHardErrors();
                return "";
            }
            return value.asText();
        }
    }
}
