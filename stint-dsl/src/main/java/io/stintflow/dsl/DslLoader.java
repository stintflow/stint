package io.stintflow.dsl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.TreeMap;

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
import io.stintflow.core.model.EmitNode;
import io.stintflow.core.model.FlowDirective;
import io.stintflow.core.model.ListenNode;
import io.stintflow.core.model.RetryPolicy;
import io.stintflow.core.model.Schedule;
import io.stintflow.core.model.SetNode;
import io.stintflow.core.model.SwitchNode;
import io.stintflow.core.model.TaskNode;
import io.stintflow.core.model.TryNode;
import io.stintflow.core.trigger.EventFilter;
import io.stintflow.spi.CronExpression;
import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.spi.wire.StintEvents;
import io.stintflow.wire.decision.DecisionKind;
import io.stintflow.wire.decision.DecisionOption;
import io.stintflow.wire.decision.DecisionRequest;

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
        // SDD 2.9, RF4: the keys the compiler uses, per object (DSL 1.0 informative keys included).
        private static final Set<String> ROOT_KEYS = Set.of("document", "schedule", "do");
        private static final Set<String> DOCUMENT_KEYS =
                Set.of("dsl", "namespace", "name", "version", "title", "summary", "tags", "metadata");
        private static final Set<String> SCHEDULE_KEYS = Set.of("cron", "every", "on", "after");
        private static final List<String> TASK_CONSTRUCTS =
                List.of("call", "set", "switch", "try", "emit", "listen", "fork", "wait", "for", "raise", "run", "do");
        private static final Set<String> COMMON_TASK_KEYS = Set.of("input", "output", "export", "then", "metadata");
        private static final Map<String, Set<String>> TASK_KEYS = Map.of(
                "call", Set.of("call", "with", "timeout"),
                "set", Set.of("set"),
                "switch", Set.of("switch"),
                "try", Set.of("try", "catch"),
                "emit", Set.of("emit"),
                "listen", Set.of("listen", "timeout"));
        private static final Set<String> CALL_REMOTE_WITH_KEYS = Set.of("task", "input");
        /** SDD 3a.1: {@code call: decision} (Stint extension) — its own task key, {@code with} keys and option keys. */
        private static final Set<String> DECISION_WITH_KEYS = Set.of("task", "kind", "input", "options");
        private static final Set<String> DECISION_OPTION_KEYS = Set.of("description", "then");
        private static final Set<String> TIMEOUT_KEYS = Set.of("after");
        private static final Set<String> DURATION_KEYS = Set.of("days", "hours", "minutes", "seconds", "milliseconds");
        private static final Set<String> INPUT_KEYS = Set.of("from");
        private static final Set<String> OUTPUT_KEYS = Set.of("as");
        private static final Set<String> EMIT_KEYS = Set.of("event");
        private static final Set<String> EMIT_EVENT_KEYS = Set.of("with");
        private static final Set<String> LISTEN_KEYS = Set.of("to", "read", "foreach");
        private static final Set<String> LISTEN_TO_KEYS = Set.of("one", "any", "all", "until");
        private static final Set<String> EVENT_FILTER_KEYS = Set.of("with", "correlate");
        private static final Set<String> CORRELATION_KEYS = Set.of("from", "expect");
        private static final Set<String> SWITCH_CASE_KEYS = Set.of("when", "then");
        private static final Set<String> CATCH_KEYS = Set.of("errors", "as", "when", "exceptWhen", "retry", "do", "then");
        private static final Set<String> CATCH_ERRORS_KEYS = Set.of("with");
        private static final Set<String> RETRY_KEYS = Set.of("delay", "backoff", "limit", "jitter", "when", "exceptWhen");
        private static final Set<String> BACKOFF_KEYS = Set.of("constant", "linear", "exponential");
        private static final Set<String> RETRY_LIMIT_KEYS = Set.of("attempt", "duration");
        private static final Set<String> RETRY_ATTEMPT_KEYS = Set.of("count", "duration");

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
            checkKeys(root, "", ROOT_KEYS);
            JsonNode document = require(root, "document", "/");
            checkKeys(document, "/document", DOCUMENT_KEYS);
            String namespace = document.path("namespace").asText("default");
            String name = requireText(document, "name", "/document/name");
            String version = requireText(document, "version", "/document/version");
            WorkflowRef ref = new WorkflowRef(namespace, name, version);

            Schedule schedule = root.has("schedule") ? compileSchedule(root.get("schedule"), document) : null;

            JsonNode doArray = root.get("do");
            List<TaskNode> tasks = doArray != null && doArray.isArray()
                    ? compileTaskList(doArray, "")
                    : List.of();

            failIfAnyHardErrors();

            DoNode rootNode = new DoNode("root", "", DataFlow.NONE, FlowDirective.END, tasks);
            return new WorkflowDefinition(ref, rootNode, schedule);
        }

        /**
         * SDD 2.4, RF1: DSL 1.0 {@code schedule} — "Configures the schedule of a workflow". {@code cron} is a
         * five-field expression in the portable subset of {@link CronExpression} (sec. 8b); {@code every} must
         * have a cron equivalent; {@code on} takes {@code one} or {@code any} of exact-match event filters,
         * started through the SDD 2.1 bindings. {@code after} and {@code on} with {@code all}, {@code until} or
         * {@code correlate} are reported as unsupported. The time zone comes from the Stint extension
         * {@code document.metadata["stint.schedule.timezone"]} (IANA id; default UTC, sec. 8c).
         */
        private Schedule compileSchedule(JsonNode schedule, JsonNode document) {
            if (schedule.has("after")) {
                unsupported("/schedule/after", "'schedule.after' is not supported in this phase");
                return null;
            }
            ZoneId zone = ZoneId.of("UTC");
            JsonNode tz = document.path("metadata").get(Schedule.TIMEZONE_METADATA);
            if (tz != null) {
                try {
                    zone = ZoneId.of(tz.asText());
                } catch (DateTimeException e) {
                    hardError("/document/metadata/" + Schedule.TIMEZONE_METADATA,
                            "unknown time zone '" + tz.asText() + "' (use an IANA id such as America/Sao_Paulo)", null);
                    return null;
                }
            }
            CronExpression cron = null;
            try {
                if (schedule.has("cron")) {
                    cron = CronExpression.parse(schedule.get("cron").asText());
                } else if (schedule.has("every")) {
                    cron = CronExpression.every(compileDurationSpec(schedule.get("every")));
                }
            } catch (IllegalArgumentException | DateTimeException e) {
                hardError(schedule.has("cron") ? "/schedule/cron" : "/schedule/every", e.getMessage(), null);
                return null;
            }
            checkKeys(schedule, "/schedule", SCHEDULE_KEYS);
            if (schedule.has("every")) {
                checkDuration(schedule.get("every"), "/schedule/every");
            }
            List<EventFilter> on = new ArrayList<>();
            if (schedule.has("on")) {
                JsonNode to = schedule.get("on");
                checkKeys(to, "/schedule/on", LISTEN_TO_KEYS);
                if (to.has("all") || to.has("until")) {
                    unsupported("/schedule/on", "'schedule.on' with 'all' or 'until' is not supported in this phase");
                    return null;
                }
                List<JsonNode> filters = new ArrayList<>();
                String filtersPointer;
                if (to.has("one")) {
                    filters.add(to.get("one"));
                    filtersPointer = "/schedule/on/one";
                } else if (to.has("any") && to.get("any").isArray() && !to.get("any").isEmpty()) {
                    to.get("any").forEach(filters::add);
                    filtersPointer = "/schedule/on/any";
                } else {
                    unsupported("/schedule/on", "'schedule.on' needs 'one' or a non-empty 'any' in this phase");
                    return null;
                }
                for (int i = 0; i < filters.size(); i++) {
                    String pointer = to.has("one") ? filtersPointer : filtersPointer + "/" + i;
                    EventFilter filter = compileStartFilter(filters.get(i), pointer);
                    if (filter == null) {
                        return null;
                    }
                    on.add(filter);
                }
            }
            if (cron == null && on.isEmpty()) {
                hardError("/schedule", "'schedule' needs 'cron', 'every' or 'on'", null);
                return null;
            }
            return new Schedule(cron, zone, on);
        }

        /** An exact-match filter that starts a definition (SDD 2.1 {@link EventFilter}); no {@code correlate}. */
        private EventFilter compileStartFilter(JsonNode filter, String pointer) {
            checkKeys(filter, pointer, EVENT_FILTER_KEYS);
            if (filter.has("correlate")) {
                unsupported(pointer + "/correlate", "'correlate' when starting a workflow is not supported in this phase");
                return null;
            }
            JsonNode with = filter.path("with");
            if (!with.isObject() || !with.hasNonNull("type")) {
                hardError(pointer + "/with/type", "an event filter requires 'with.type'", null);
                return null;
            }
            Map<String, String> attributes = new LinkedHashMap<>();
            for (Iterator<String> it = with.fieldNames(); it.hasNext(); ) {
                String attribute = it.next();
                JsonNode value = with.get(attribute);
                if (!value.isValueNode() || isExpression(value)) {
                    unsupported(pointer + "/with/" + attribute, "only exact values are supported in an event "
                            + "filter's 'with' in this phase (no runtime expressions)");
                    return null;
                }
                attributes.put(attribute, value.asText());
            }
            String type = attributes.remove("type");
            return new EventFilter(type, attributes);
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
                if (isDecision(body)) {
                    checkTaskKeys(body, pointer);
                    tasks.addAll(compileDecision(taskName, body, pointer, null));
                } else if (isDecisionTry(body)) {
                    tasks.addAll(compileTryDecision(taskName, body, pointer));
                } else {
                    TaskNode compiled = compileTask(taskName, body, pointer);
                    if (compiled != null) {
                        tasks.add(compiled);
                    }
                }
            }
            checkUniqueNames(tasks);
            return tasks;
        }

        private static boolean isDecision(JsonNode body) {
            return body != null && body.isObject() && "decision".equals(body.path("call").asText(null));
        }

        private static boolean isDecisionTry(JsonNode body) {
            JsonNode tryList = body == null ? null : body.get("try");
            if (tryList == null || !tryList.isArray() || tryList.size() != 1) {
                return false;
            }
            Iterator<String> names = tryList.get(0).fieldNames();
            return names.hasNext() && isDecision(tryList.get(0).get(names.next()));
        }

        /** SDD 3a.1, 5.2: the generated {@code <task>/route} must not shadow a task the author named the same. */
        private void checkUniqueNames(List<TaskNode> tasks) {
            Set<String> seen = new java.util.HashSet<>();
            for (TaskNode task : tasks) {
                if (task.name().endsWith(DecisionExpansion.ROUTE_SUFFIX) && !seen.add(task.name())) {
                    hardError(task.pointer(), "task name '" + task.name() + "' is used twice in the same flow "
                            + "(a 'call: decision' generates a switch with this name, SDD 3a.1)", null);
                } else {
                    seen.add(task.name());
                }
            }
        }

        /**
         * SDD 3a.1, RF2/RF3: {@code call: decision} (Stint extension, like {@code call: remote} — DSL 1.0, Call:
         * {@code call} is "The name of the function to call", {@code with} "A name/value mapping of the parameters to
         * call the function with"). Compiles to two siblings, so {@code then}, {@code goTo} and {@code continue} work
         * as anywhere in the flow:
         * <ol>
         *   <li>a {@code call: remote} to {@code with.task}, whose argument is the decision request (kind, options and
         *       {@code with.input}), whose output is checked against the decision contract, and which carries the
         *       author's {@code input.from};</li>
         *   <li>a {@code switch} named {@code <task>/route} over the decision's result: {@code onUncertain} first, then
         *       one case per option, then — for a score, or a boolean without options — a default. It carries the
         *       author's {@code output.as} and {@code export.as}.</li>
         * </ol>
         *
         * @param defaultThen where a case without its own {@code then} goes when the decision is a try's body (the
         *                    try's {@code then}); {@code null} = the task's own {@code then}
         */
        private List<TaskNode> compileDecision(String name, JsonNode body, String pointer, FlowDirective defaultThen) {
            JsonNode with = body.get("with");
            if (with == null || !with.isObject()) {
                hardError(pointer + "/with", "'call: decision' requires 'with' (task, kind, options, input)", null);
                return List.of();
            }
            checkKeys(with, pointer + "/with", DECISION_WITH_KEYS);
            boolean valid = true;
            if (!with.hasNonNull("task") || with.get("task").asText().isBlank()) {
                hardError(pointer + "/with/task", "'call: decision' requires 'with.task' (the routing key)", null);
                valid = false;
            }
            DecisionKind kind = DecisionKind.fromWireName(with.path("kind").asText(null)).orElse(null);
            if (kind == null) {
                hardError(pointer + "/with/kind", "'call: decision' requires 'with.kind': choice, score or boolean",
                        null);
                valid = false;
            }
            List<DecisionOption> options = new ArrayList<>();
            List<FlowDirective> optionThens = new ArrayList<>();
            JsonNode optionsNode = with.get("options");
            if (optionsNode != null && !optionsNode.isArray()) {
                hardError(pointer + "/with/options", "'with.options' must be a list of single-key maps", null);
                valid = false;
            } else if (optionsNode != null) {
                for (int i = 0; i < optionsNode.size(); i++) {
                    JsonNode entry = optionsNode.get(i);
                    String optionPointer = pointer + "/with/options/" + i;
                    if (!entry.isObject() || entry.size() != 1) {
                        hardError(optionPointer, "an option is a single-key map: '<name>: {description, then}'", null);
                        valid = false;
                        continue;
                    }
                    String optionName = entry.fieldNames().next();
                    JsonNode optionBody = entry.get(optionName);
                    String bodyPointer = optionPointer + "/" + optionName;
                    if (optionBody != null && !optionBody.isNull() && !optionBody.isObject()) {
                        hardError(bodyPointer, "an option's body is a map (description, then)", null);
                        valid = false;
                        continue;
                    }
                    checkKeys(optionBody, bodyPointer, DECISION_OPTION_KEYS);
                    JsonNode description = optionBody == null ? null : optionBody.get("description");
                    if (description != null && !description.isTextual()) {
                        hardError(bodyPointer + "/description", "an option's description is text", null);
                        valid = false;
                    }
                    options.add(new DecisionOption(optionName, description == null ? null : description.asText()));
                    optionThens.add(optionBody != null && optionBody.has("then") ? compileThen(optionBody) : null);
                }
            }
            if (kind != null) {
                for (String problem : DecisionRequest.optionProblems(kind, options)) {
                    hardError(pointer + "/with/options", problem, null);
                    valid = false;
                }
            }
            JsonNode onUncertain = body.get(DecisionExpansion.ON_UNCERTAIN);
            if (onUncertain != null && !onUncertain.isTextual()) {
                hardError(pointer + "/" + DecisionExpansion.ON_UNCERTAIN,
                        "'onUncertain' is a task name or a flow directive, like 'then'", null);
                valid = false;
            }
            if (!valid) {
                return List.of();
            }
            DecisionRequest shape = DecisionRequest.shape(kind, options);
            String input = with.has("input") ? TemplateCompiler.compileTemplate(with.get("input")) : ".";
            Expr request = compileAndValidate(DecisionExpansion.requestTemplate(shape, input), pointer + "/with");
            DataFlow authored = compileDataFlow(body, pointer, null, null);
            FlowDirective otherwise = defaultThen != null ? defaultThen : compileThen(body);
            List<TaskNode> expanded = DecisionExpansion.expand(name, pointer, shape, with.path("task").asText(),
                    compileTimeout(body, pointer), request, authored, otherwise,
                    onUncertain == null ? null : directive(onUncertain.asText()), optionThens);
            LOG.log(Level.DEBUG, () -> sourceName + " " + pointer + ": 'call: decision' expands to\n"
                    + DefinitionPrinter.print(expanded));
            return expanded;
        }

        /**
         * SDD 3a.1, 5.2: a {@code try} whose body is a {@code call: decision}. The try wraps the remote call only (so
         * its timeout, retry and catch are those of SDD 1.3); the decision's switch becomes the try's next sibling, and
         * the try's own {@code then} becomes the switch's default.
         */
        private List<TaskNode> compileTryDecision(String name, JsonNode body, String pointer) {
            checkTaskKeys(body, pointer);
            JsonNode bodyEntry = body.get("try").get(0);
            String bodyName = bodyEntry.fieldNames().next();
            JsonNode decision = bodyEntry.get(bodyName);
            String bodyPointer = pointer + "/do/0/" + bodyName;
            checkTaskKeys(decision, bodyPointer);
            if (decision.has("then")) {
                unsupported(bodyPointer + "/then", "'then' of a try's body is not used: the try's own 'then' "
                        + "applies (SDD 2.9)");
            }
            List<TaskNode> expanded = compileDecision(bodyName, decision, bodyPointer, compileThen(body));
            JsonNode catchNode = body.get("catch");
            if (catchNode == null) {
                hardError(pointer + "/catch", "'try' requires a 'catch' clause", null);
                return List.of();
            }
            TryNode.Catch catchClause = compileCatch(catchNode, pointer + "/catch");
            if (expanded.size() != 2) {
                return List.of();
            }
            TryNode tryNode = new TryNode(name, pointer, compileDataFlow(body, pointer, null, null),
                    FlowDirective.CONTINUE, expanded.get(0), catchClause);
            return List.of(tryNode, expanded.get(1));
        }

        /** A flow directive written as text: {@code then}'s syntax, also used by {@code onUncertain} (SDD 3a.1). */
        private static FlowDirective directive(String value) {
            return switch (value) {
                case "continue" -> FlowDirective.CONTINUE;
                case "end" -> FlowDirective.END;
                case "exit" -> FlowDirective.EXIT;
                default -> FlowDirective.goTo(value);
            };
        }

        private TaskNode compileTask(String name, JsonNode body, String pointer) {
            checkTaskKeys(body, pointer);
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
            if (body.has("emit")) {
                return compileEmit(name, body, pointer);
            }
            if (body.has("listen")) {
                return compileListen(name, body, pointer);
            }
            for (String unsupportedKey : List.of("fork", "wait", "for")) {
                if (body.has(unsupportedKey)) {
                    unsupported(pointer + "/" + unsupportedKey,
                            "task construct '" + unsupportedKey + "' is not supported in this phase (Fase 2)");
                    return null;
                }
            }
            hardError(pointer, "task has no recognized construct (call/set/switch/try/emit/listen)", null);
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
            checkKeys(with, pointer + "/with", CALL_REMOTE_WITH_KEYS);
            String routingKey = with.get("task").asText();
            Duration timeout = compileTimeout(body, pointer);
            // SDD 2.9, RF4: input.from transforms the task's input, then with.input (the call's argument) is
            // evaluated over it — both apply, inside a try or not (DSL 1.0 data flow, then the call's 'with').
            JsonNode remoteInput = with.get("input");
            Expr withInput = remoteInput == null ? null
                    : compileAndValidate(TemplateCompiler.compileTemplate(remoteInput), pointer + "/with/input");
            DataFlow dataFlow = compileDataFlow(body, pointer, null, null);
            return new CallRemoteNode(name, pointer, dataFlow, compileThen(body), routingKey, timeout, withInput);
        }

        /**
         * SDD 2.2, RF2: {@code emit.event.with} (DSL 1.0 Event Properties). {@code source} and {@code type}
         * are required ("Required when emitting an event using emit.event.with"); a literal {@code type}
         * in the engine's reserved namespace fails the load (sec. 8e). {@code id} is optional here — the
         * spec's own Emit example omits it; the engine then generates a stable one (sec. 8b, a Stint choice).
         */
        private TaskNode compileEmit(String name, JsonNode body, String pointer) {
            String withPointer = pointer + "/emit/event/with";
            checkKeys(body.path("emit"), pointer + "/emit", EMIT_KEYS);
            checkKeys(body.path("emit").path("event"), pointer + "/emit/event", EMIT_EVENT_KEYS);
            JsonNode with = body.path("emit").path("event").path("with");
            if (!with.isObject()) {
                hardError(withPointer, "'emit' requires an 'event.with' object", null);
                return null;
            }
            boolean valid = true;
            for (String required : List.of("source", "type")) {
                if (!with.hasNonNull(required)) {
                    hardError(withPointer + "/" + required,
                            "'emit.event.with." + required + "' is required when emitting an event", null);
                    valid = false;
                }
            }
            if (!valid) {
                return null;
            }
            String declaredType = isExpression(with.get("type")) ? null : with.get("type").asText();
            if (StintEvents.isReservedType(declaredType)) {
                hardError(withPointer + "/type", "emit type '" + declaredType + "' is reserved for the engine's "
                        + "internal protocol " + StintEvents.RESERVED_TYPE_PREFIXES, null);
                return null;
            }
            Expr event = compileAndValidate(TemplateCompiler.compileTemplate(with), withPointer);
            return new EmitNode(name, pointer, compileDataFlow(body, pointer, null, null), compileThen(body), event,
                    declaredType);
        }

        /**
         * SDD 2.3, RF1: {@code listen} (DSL 1.0 Listen). {@code listen.to} is {@code one} (an Event Filter) or
         * {@code any}/{@code all} (lists of them); {@code listen.read} is {@code data} (default), {@code envelope}
         * or {@code raw}. Each Event Filter needs a literal {@code with.type}; other {@code with} entries are
         * matched exactly; {@code correlate} maps a name to {@code from} (a runtime expression over the event)
         * and {@code expect} (a constant or a runtime expression). What this phase leaves out — {@code until},
         * {@code foreach}, an empty {@code any}, expressions in {@code with}, a filter without {@code correlate}
         * or a correlation without {@code expect} — is reported as unsupported (sec. 8f), dropping the task.
         */
        private TaskNode compileListen(String name, JsonNode body, String pointer) {
            String listenPointer = pointer + "/listen";
            JsonNode listen = body.get("listen");
            checkKeys(listen, listenPointer, LISTEN_KEYS);
            JsonNode to = listen.path("to");
            checkKeys(to, listenPointer + "/to", LISTEN_TO_KEYS);
            if (listen.has("foreach")) {
                unsupported(listenPointer + "/foreach", "'listen.foreach' is not supported in this phase");
                return null;
            }
            if (to.has("until")) {
                unsupported(listenPointer + "/to/until", "'listen.to.until' is not supported in this phase");
                return null;
            }
            ListenNode.Strategy strategy;
            List<JsonNode> filterNodes = new ArrayList<>();
            String filtersPointer;
            if (to.has("one")) {
                strategy = ListenNode.Strategy.ONE;
                filterNodes.add(to.get("one"));
                filtersPointer = listenPointer + "/to/one";
            } else if (to.has("any") || to.has("all")) {
                strategy = to.has("any") ? ListenNode.Strategy.ANY : ListenNode.Strategy.ALL;
                String key = to.has("any") ? "any" : "all";
                filtersPointer = listenPointer + "/to/" + key;
                if (!to.get(key).isArray() || to.get(key).isEmpty()) {
                    unsupported(filtersPointer, "'listen.to." + key + "' needs at least one event filter "
                            + "(listening to every event is not supported in this phase)");
                    return null;
                }
                to.get(key).forEach(filterNodes::add);
            } else {
                hardError(listenPointer + "/to", "'listen.to' requires one of 'one', 'any' or 'all'", null);
                return null;
            }

            List<ListenNode.Filter> filters = new ArrayList<>();
            for (int i = 0; i < filterNodes.size(); i++) {
                String filterPointer = strategy == ListenNode.Strategy.ONE ? filtersPointer : filtersPointer + "/" + i;
                ListenNode.Filter filter = compileEventFilter(filterNodes.get(i), filterPointer);
                if (filter == null) {
                    return null;
                }
                filters.add(filter);
            }

            ListenNode.Read read = ListenNode.Read.DATA;
            if (listen.has("read")) {
                switch (listen.get("read").asText()) {
                    case "data" -> read = ListenNode.Read.DATA;
                    case "envelope" -> read = ListenNode.Read.ENVELOPE;
                    case "raw" -> read = ListenNode.Read.RAW;
                    default -> {
                        hardError(listenPointer + "/read", "'listen.read' must be one of data, envelope, raw", null);
                        return null;
                    }
                }
            }
            return new ListenNode(name, pointer, compileDataFlow(body, pointer, null, null), compileThen(body),
                    strategy, filters, read, compileTimeout(body, pointer));
        }

        private ListenNode.Filter compileEventFilter(JsonNode filter, String pointer) {
            checkKeys(filter, pointer, EVENT_FILTER_KEYS);
            JsonNode with = filter.path("with");
            if (!with.isObject() || !with.hasNonNull("type")) {
                hardError(pointer + "/with/type", "an event filter requires 'with.type'", null);
                return null;
            }
            Map<String, String> attributes = new LinkedHashMap<>();
            for (Iterator<String> it = with.fieldNames(); it.hasNext(); ) {
                String attribute = it.next();
                JsonNode value = with.get(attribute);
                if (!value.isValueNode() || isExpression(value)) {
                    unsupported(pointer + "/with/" + attribute, "only exact values are supported in an event "
                            + "filter's 'with' in this phase (no runtime expressions)");
                    return null;
                }
                attributes.put(attribute, value.asText());
            }
            String type = attributes.remove("type");

            JsonNode correlate = filter.get("correlate");
            if (correlate == null || !correlate.isObject() || correlate.isEmpty()) {
                unsupported(pointer + "/correlate", "a listen event filter without 'correlate' is not supported in "
                        + "this phase (SDD 2.3, sec. 8a)");
                return null;
            }
            TreeMap<String, ListenNode.Correlation> correlations = new TreeMap<>();
            for (Iterator<String> it = correlate.fieldNames(); it.hasNext(); ) {
                String correlationName = it.next();
                JsonNode correlation = correlate.get(correlationName);
                String correlationPointer = pointer + "/correlate/" + correlationName;
                checkKeys(correlation, correlationPointer, CORRELATION_KEYS);
                if (!correlation.hasNonNull("from")) {
                    hardError(correlationPointer + "/from", "a correlation requires 'from'", null);
                    return null;
                }
                if (!correlation.has("expect")) {
                    unsupported(correlationPointer + "/expect", "a correlation without 'expect' is not supported "
                            + "in this phase (SDD 2.3, sec. 8a)");
                    return null;
                }
                Expr from = compileAndValidate(TemplateCompiler.compileBareExpression(
                        correlation.get("from").asText()), correlationPointer + "/from");
                Expr expect = compileAndValidate(TemplateCompiler.compileTemplate(correlation.get("expect")),
                        correlationPointer + "/expect");
                correlations.put(correlationName, new ListenNode.Correlation(from, expect));
            }
            return new ListenNode.Filter(type, attributes, correlations);
        }

        private static boolean isExpression(JsonNode node) {
            return node.isTextual() && node.textValue().trim().startsWith("${");
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
                checkKeys(caseBody, casePointer + "/" + caseName, SWITCH_CASE_KEYS);
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
                                + "(SDD 1.3 scope note: the try body is restricted to a single 'call: remote' "
                                + "or, SDD 2.3, a single 'listen')",
                        null);
                return null;
            }
            List<TaskNode> bodyTasks = compileTaskList(tryList, pointer);
            if (bodyTasks.size() != 1
                    || !(bodyTasks.get(0) instanceof CallRemoteNode || bodyTasks.get(0) instanceof ListenNode)) {
                hardError(pointer + "/try",
                        "'try' body must compile to a single 'call: remote' or 'listen' task (SDD 1.3 scope note)",
                        null);
                return null;
            }
            TaskNode bodyCall = bodyTasks.get(0);
            JsonNode bodyEntry = tryList.get(0);
            String bodyName = bodyEntry.fieldNames().next();
            if (bodyEntry.get(bodyName).has("then")) {
                unsupported(bodyCall.pointer() + "/then", "'then' of a try's body is not used: the try's own 'then' "
                        + "applies (SDD 2.9)");
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
            checkKeys(catchNode, pointer, CATCH_KEYS);
            if (catchNode.has("errors")) {
                checkKeys(catchNode.get("errors"), pointer + "/errors", CATCH_ERRORS_KEYS);
            }
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
                // SDD 2.9: a declared name that would be ignored fails STRICT (was a warning).
                unsupported(pointer + "/as", "custom error variable names are not supported yet — the caught error "
                        + "is always exposed as '.' to errors/when/exceptWhen/retry/do");
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
                JsonNode value = with.get(field);
                if ("type".equals(field) && value.isTextual() && !isExpression(value)) {
                    // SDD 2.8: stint://errors/* is an alias of the DSL 1.0 standard type the engine raises.
                    value = com.fasterxml.jackson.databind.node.TextNode.valueOf(
                            ErrorInfo.normalize(java.net.URI.create(value.textValue())).toString());
                }
                jq.append('(').append('.').append(field).append(" == ").append(TemplateCompiler.compileTemplate(value)).append(')');
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

            checkKeys(retry, pointer, RETRY_KEYS);
            if (retry.has("delay")) {
                checkDuration(retry.get("delay"), pointer + "/delay");
            }
            if (retry.has("backoff")) {
                checkKeys(retry.get("backoff"), pointer + "/backoff", BACKOFF_KEYS);
            }
            if (retry.has("limit")) {
                checkKeys(retry.get("limit"), pointer + "/limit", RETRY_LIMIT_KEYS);
                if (retry.get("limit").has("attempt")) {
                    checkKeys(retry.get("limit").get("attempt"), pointer + "/limit/attempt", RETRY_ATTEMPT_KEYS);
                }
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
                // SDD 2.9: declared but not applied fails STRICT (was a warning).
                unsupported(pointer + "/jitter", "jitter.from/to is not supported yet (no jitter would be applied)");
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
            checkKeys(body.get("input"), pointer + "/input", INPUT_KEYS);
            checkKeys(body.get("output"), pointer + "/output", OUTPUT_KEYS);
            checkKeys(body.get("export"), pointer + "/export", OUTPUT_KEYS);
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
            return directive(then.asText());
        }

        private Duration compileTimeout(JsonNode body, String pointer) {
            JsonNode timeout = body.get("timeout");
            if (timeout == null || timeout.isNull()) {
                return null;
            }
            checkKeys(timeout, pointer + "/timeout", TIMEOUT_KEYS);
            JsonNode after = timeout.get("after");
            if (after == null) {
                return null;
            }
            checkDuration(after, pointer + "/timeout/after");
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

        /**
         * SDD 2.9, RF4: every key a definition declares is either used by the compiler or reported — STRICT fails,
         * PERMISSIVE warns. Nothing declared is ignored in silence.
         */
        private void checkKeys(JsonNode node, String pointer, Set<String> allowed) {
            if (node == null || !node.isObject()) {
                return;
            }
            for (Iterator<String> it = node.fieldNames(); it.hasNext(); ) {
                String key = it.next();
                if (!allowed.contains(key)) {
                    unsupported(pointer + "/" + key, "'" + key + "' is declared but not supported by this version of "
                            + "Stint (SDD 2.9: nothing declared is ignored in silence)");
                }
            }
        }

        private void checkDuration(JsonNode duration, String pointer) {
            checkKeys(duration, pointer, DURATION_KEYS);
        }

        /** A task: one construct, and only the keys that construct and the common task properties use. */
        private void checkTaskKeys(JsonNode body, String pointer) {
            if (body == null || !body.isObject()) {
                return;
            }
            List<String> constructs = TASK_CONSTRUCTS.stream().filter(body::has).toList();
            if (constructs.size() > 1) {
                unsupported(pointer, "a task declares more than one construct " + constructs
                        + "; only one would be used (SDD 2.9)");
                return;
            }
            Set<String> specific = constructs.isEmpty() ? null : TASK_KEYS.get(constructs.get(0));
            if (specific == null) {
                return; // reported by compileTask (an unsupported construct, or none)
            }
            Set<String> allowed = new java.util.HashSet<>(COMMON_TASK_KEYS);
            allowed.addAll(specific);
            if (isDecision(body)) {
                allowed.add(DecisionExpansion.ON_UNCERTAIN); // SDD 3a.1: a Stint extension key, only on 'call: decision'
            }
            checkKeys(body, pointer, allowed);
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
