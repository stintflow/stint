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
import io.stintflow.core.policy.Policy;
import io.stintflow.core.trigger.EventFilter;
import io.stintflow.spi.CronExpression;
import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.spi.wire.StintEvents;
import io.stintflow.wire.decision.DecisionKind;
import io.stintflow.wire.decision.DecisionOption;
import io.stintflow.wire.decision.DecisionRequest;
import io.stintflow.wire.review.ReviewContract;

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
        private Set<String> reviewSources = Set.of();
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
        /** SDD 3a.5: {@code call: redact} (Stint extension) — its {@code with} keys and where a doubtful one goes. */
        private static final Set<String> REDACT_WITH_KEYS = Set.of("task", "input", "lowersTo", "types");
        private static final String ON_DOUBT = "onDoubt";
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
        /** SDD 3a.5: {@code document.metadata.x-stint-policy} (Stint extension) and a task's own. */
        private static final Set<String> POLICY_KEYS = Set.of("sensitivity", "routes", "retention", "review");
        private static final Set<String> POLICY_REVIEW_KEYS = Set.of("sources");
        // SDD 3a.4, 4.1: the review step (Stint extension)
        private static final Set<String> REVIEW_WITH_KEYS = Set.of("kind", "role", "material", "items", "options",
                "deadline", "remind", "escalate", "onTimeout");
        private static final Set<String> UNCERTAIN_REVIEW_KEYS = Set.of("role", "material", "deadline", "remind",
                "escalate", "onTimeout");
        private static final Set<String> ESCALATE_KEYS = Set.of("role", "after");
        private static final String ON_REJECT = "onReject";
        private static final java.util.regex.Pattern ROLE = java.util.regex.Pattern.compile("[a-z0-9][a-z0-9-]*");
        private static final Set<String> POLICY_SENSITIVITY_KEYS = Set.of("from");
        private static final Set<String> POLICY_RETENTION_KEYS = Set.of("days");
        private static final Set<String> TASK_POLICY_KEYS = Set.of("maxSensitivity");
        /** SDD 3a.5: a task's {@code metadata.x-stint-policy.maxSensitivity}, by the pointer of its remote call. */
        private final Map<String, Sensitivity> taskMax = new java.util.HashMap<>();

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

            reviewSources = reviewSourcesOf(document.path("metadata").path(Policy.METADATA_KEY));
            JsonNode doArray = root.get("do");
            List<TaskNode> tasks = doArray != null && doArray.isArray()
                    ? compileTaskList(doArray, "")
                    : List.of();

            Policy policy = compilePolicy(document.path("metadata").get(Policy.METADATA_KEY));
            if (policy == null && !taskMax.isEmpty()) {
                hardError("/document/metadata/" + Policy.METADATA_KEY, "a task declares metadata."
                        + Policy.METADATA_KEY + " but the workflow has no document.metadata." + Policy.METADATA_KEY,
                        null);
            }

            failIfAnyHardErrors();

            DoNode rootNode = new DoNode("root", "", DataFlow.NONE, FlowDirective.END, tasks);
            return new WorkflowDefinition(ref, rootNode, schedule, policy);
        }

        /**
         * SDD 3a.5, RF3: the Stint extension {@code document.metadata.x-stint-policy} — not part of the CNCF DSL, and
         * kept under {@code document.metadata}, which the DSL leaves free, so the document stays valid for it. STRICT
         * fails on any key it doesn't know.
         */
        private Policy compilePolicy(JsonNode node) {
            if (node == null) {
                return null;
            }
            String base = "/document/metadata/" + Policy.METADATA_KEY;
            if (!node.isObject()) {
                hardError(base, "'" + Policy.METADATA_KEY + "' is an object (sensitivity, routes, retention)", null);
                return null;
            }
            checkKeys(node, base, POLICY_KEYS);
            Expr from = null;
            JsonNode sensitivity = node.get("sensitivity");
            if (sensitivity != null) {
                checkKeys(sensitivity, base + "/sensitivity", POLICY_SENSITIVITY_KEYS);
                if (sensitivity.has("from")) {
                    from = compileAndValidate(TemplateCompiler.compileTemplate(sensitivity.get("from")),
                            base + "/sensitivity/from");
                }
            }
            Map<Sensitivity, Set<String>> routes = new java.util.EnumMap<>(Sensitivity.class);
            JsonNode routesNode = node.get("routes");
            if (routesNode != null) {
                if (!routesNode.isObject()) {
                    hardError(base + "/routes", "'routes' maps a level to a list of routing keys", null);
                } else {
                    routesNode.properties().forEach(e -> {
                        String pointer = base + "/routes/" + e.getKey();
                        Sensitivity level = Sensitivity.parse(e.getKey()).orElse(null);
                        if (level == null) {
                            unsupported(pointer, "'" + e.getKey() + "' is not a level (internal, confidential, "
                                    + "restricted)");
                            return;
                        }
                        if (!e.getValue().isArray()) {
                            hardError(pointer, "a level's routes are a list of routing keys ('*' = any)", null);
                            return;
                        }
                        Set<String> keys = new java.util.LinkedHashSet<>();
                        for (JsonNode key : e.getValue()) {
                            if (!key.isTextual() || key.asText().isBlank()) {
                                hardError(pointer, "a route is a routing key (text) or '*'", null);
                            } else {
                                keys.add(key.asText());
                            }
                        }
                        routes.put(level, keys);
                    });
                }
            }
            Map<String, java.time.Duration> retention = new java.util.LinkedHashMap<>();
            JsonNode retentionNode = node.get("retention");
            if (retentionNode != null) {
                if (!retentionNode.isObject()) {
                    hardError(base + "/retention", "'retention' maps a blob class to {days}", null);
                } else {
                    retentionNode.properties().forEach(e -> {
                        String pointer = base + "/retention/" + e.getKey();
                        checkKeys(e.getValue(), pointer, POLICY_RETENTION_KEYS);
                        JsonNode days = e.getValue().get("days");
                        if (days == null || !days.canConvertToInt() || days.asInt() < 1) {
                            hardError(pointer + "/days", "a retention is a whole number of days, at least 1", null);
                        } else {
                            retention.put(e.getKey(), java.time.Duration.ofDays(days.asInt()));
                        }
                    });
                }
            }
            JsonNode reviewNode = node.get("review");
            if (reviewNode != null) {
                checkKeys(reviewNode, base + "/review", POLICY_REVIEW_KEYS);
                JsonNode sources = reviewNode.get("sources");
                if (sources == null || !sources.isArray() || sources.isEmpty()) {
                    hardError(base + "/review/sources", "'review.sources' lists the sources trusted to answer reviews",
                            null);
                } else {
                    for (JsonNode source : sources) {
                        if (!source.isTextual() || source.asText().isBlank()) {
                            hardError(base + "/review/sources", "a review source is the event 'source' (a URI)", null);
                        }
                    }
                }
            }
            return new Policy(from, routes, retention, taskMax);
        }

        /** SDD 3a.5: a task's {@code metadata.x-stint-policy.maxSensitivity}, kept for each remote call it compiled to. */
        private void collectTaskPolicy(JsonNode body, String pointer, List<TaskNode> compiled) {
            JsonNode policy = body == null ? null : body.path("metadata").get(Policy.METADATA_KEY);
            if (policy == null) {
                return;
            }
            String base = pointer + "/metadata/" + Policy.METADATA_KEY;
            checkKeys(policy, base, TASK_POLICY_KEYS);
            JsonNode max = policy.get("maxSensitivity");
            if (max == null) {
                return;
            }
            Sensitivity level = Sensitivity.parse(max.asText()).orElse(null);
            if (level == null) {
                hardError(base + "/maxSensitivity", "'" + max.asText() + "' is not a level (internal, confidential, "
                        + "restricted)", null);
                return;
            }
            boolean remote = false;
            for (TaskNode node : compiled) {
                if (node instanceof CallRemoteNode call) {
                    taskMax.put(call.pointer(), level);
                    remote = true;
                } else if (node instanceof TryNode tryNode && tryNode.body() != null) {
                    taskMax.put(tryNode.body().pointer(), level);
                    remote = true;
                }
            }
            if (!remote) {
                hardError(base, "only a task that dispatches (call) takes a maxSensitivity", null);
            }
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
                List<TaskNode> compiled;
                if (isDecision(body)) {
                    checkTaskKeys(body, pointer);
                    compiled = compileDecision(taskName, body, pointer, null);
                } else if (isRedact(body)) {
                    checkTaskKeys(body, pointer);
                    compiled = compileRedact(taskName, body, pointer);
                } else if (isReview(body)) {
                    checkTaskKeys(body, pointer);
                    compiled = compileReview(taskName, body, pointer);
                } else if (isDecisionTry(body)) {
                    compiled = compileTryDecision(taskName, body, pointer);
                } else {
                    TaskNode single = compileTask(taskName, body, pointer);
                    compiled = single == null ? List.of() : List.of(single);
                }
                tasks.addAll(compiled);
                collectTaskPolicy(body, pointer, compiled);
            }
            checkUniqueNames(tasks);
            return tasks;
        }

        private static boolean isReview(JsonNode body) {
            return body != null && body.isObject() && "review".equals(body.path("call").asText(null));
        }

        /** SDD 3a.4: the trusted answer sources, read before the tasks (the policy itself is compiled after). */
        private static Set<String> reviewSourcesOf(JsonNode policy) {
            Set<String> sources = new java.util.LinkedHashSet<>();
            policy.path("review").path("sources").forEach(s -> {
                if (s.isTextual() && !s.asText().isBlank()) {
                    sources.add(s.asText());
                }
            });
            return sources;
        }

        /**
         * SDD 3a.4, 4.1: {@code call: review} — an approval of a document (or of its items) or a choice, asked of a
         * role, with a deadline, a reminder and an escalation; compiled by {@link ReviewExpansion}. Only directly in
         * a flow.
         */
        private List<TaskNode> compileReview(String name, JsonNode body, String pointer) {
            JsonNode with = body.get("with");
            if (with == null || !with.isObject()) {
                hardError(pointer + "/with", "'call: review' requires 'with' (kind, role, material, deadline)", null);
                return List.of();
            }
            checkKeys(with, pointer + "/with", REVIEW_WITH_KEYS);
            String kind = with.path("kind").asText("");
            if (!ReviewContract.APPROVAL.equals(kind) && !ReviewContract.CHOICE.equals(kind)) {
                hardError(pointer + "/with/kind", "'call: review' requires 'with.kind': approval or choice", null);
                return List.of();
            }
            List<io.stintflow.wire.decision.DecisionOption> options = new ArrayList<>();
            List<FlowDirective> optionThens = new ArrayList<>();
            if (ReviewContract.CHOICE.equals(kind)) {
                JsonNode optionsNode = with.get("options");
                if (optionsNode == null || !optionsNode.isArray() || optionsNode.size() < 2) {
                    hardError(pointer + "/with/options", "a review choice needs at least two options", null);
                    return List.of();
                }
                for (int i = 0; i < optionsNode.size(); i++) {
                    JsonNode entry = optionsNode.get(i);
                    String optionPointer = pointer + "/with/options/" + i;
                    if (!entry.isObject() || entry.size() != 1) {
                        hardError(optionPointer, "an option is a single-key map: '<name>: {description, then}'", null);
                        return List.of();
                    }
                    String optionName = entry.fieldNames().next();
                    JsonNode optionBody = entry.get(optionName);
                    checkKeys(optionBody, optionPointer + "/" + optionName, DECISION_OPTION_KEYS);
                    JsonNode description = optionBody == null ? null : optionBody.get("description");
                    options.add(new io.stintflow.wire.decision.DecisionOption(optionName,
                            description == null ? null : description.asText()));
                    optionThens.add(optionBody != null && optionBody.has("then") ? compileThen(optionBody) : null);
                }
            } else if (with.has("options")) {
                hardError(pointer + "/with/options", "an approval has no options (approved or rejected)", null);
            }
            if (body.has(ON_REJECT) && !ReviewContract.APPROVAL.equals(kind)) {
                hardError(pointer + "/" + ON_REJECT, "'onReject' is only for 'kind: approval'", null);
            }
            if (with.has("items") && !ReviewContract.APPROVAL.equals(kind)) {
                hardError(pointer + "/with/items", "'items' is only for 'kind: approval'", null);
            }
            List<String> answers = ReviewContract.APPROVAL.equals(kind)
                    ? List.of(ReviewContract.APPROVED, ReviewContract.REJECTED)
                    : options.stream().map(io.stintflow.wire.decision.DecisionOption::name).toList();
            ReviewExpansion.Settings settings = reviewSettings(with, pointer + "/with", answers);
            if (settings == null) {
                return List.of();
            }
            DataFlow authored = compileDataFlow(body, pointer, null, null);
            FlowDirective then = compileThen(body);
            List<SwitchNode.Case> cases = new ArrayList<>();
            if (ReviewContract.APPROVAL.equals(kind)) {
                cases.add(new SwitchNode.Case(ReviewContract.APPROVED,
                        Expr.jq(".verdict == " + ReviewExpansion.text(ReviewContract.APPROVED)), then));
                cases.add(new SwitchNode.Case(ReviewContract.REJECTED, null,
                        body.has(ON_REJECT) ? directive(body.get(ON_REJECT).asText()) : then));
            } else {
                for (int i = 0; i < options.size(); i++) {
                    cases.add(ReviewExpansion.optionCase(options.get(i).name(),
                            optionThens.get(i) != null ? optionThens.get(i) : then));
                }
            }
            return ReviewExpansion.expand(name, pointer, settings, kind, options, null, reviewSources,
                    authored.inputFrom(), new DataFlow(null, authored.outputAs(), authored.exportAs()), cases);
        }

        /**
         * The settings of a review: in {@code call: review}'s {@code with}, or in a decision's
         * {@code onUncertain.review}. {@code answers} are what {@code onTimeout.value} may be.
         */
        private ReviewExpansion.Settings reviewSettings(JsonNode node, String pointer, List<String> answers) {
            boolean valid = true;
            if (reviewSources.isEmpty()) {
                hardError("/document/metadata/" + Policy.METADATA_KEY + "/review/sources", "a review needs the "
                        + "sources trusted to answer it: document.metadata." + Policy.METADATA_KEY
                        + ".review.sources", null);
                valid = false;
            }
            String role = node.path("role").asText("");
            if (!ROLE.matcher(role).matches()) {
                hardError(pointer + "/role", "'role' is a role name (lowercase letters, digits and dashes)", null);
                valid = false;
            }
            if (!node.has("material")) {
                hardError(pointer + "/material", "'material' is required: a pointer to the redacted material", null);
                valid = false;
            }
            Duration deadline = null;
            if (!node.has("deadline")) {
                hardError(pointer + "/deadline", "'deadline' is required", null);
                valid = false;
            } else {
                checkDuration(node.get("deadline"), pointer + "/deadline");
                deadline = compileDurationSpec(node.get("deadline"));
            }
            Duration remind = null;
            if (node.has("remind")) {
                checkDuration(node.get("remind"), pointer + "/remind");
                remind = compileDurationSpec(node.get("remind"));
                if (deadline != null && (remind.isZero() || remind.compareTo(deadline) >= 0)) {
                    hardError(pointer + "/remind", "'remind' comes before the 'deadline'", null);
                    valid = false;
                }
            }
            String escalateRole = null;
            Duration escalateAfter = null;
            JsonNode escalate = node.get("escalate");
            if (escalate != null) {
                checkKeys(escalate, pointer + "/escalate", ESCALATE_KEYS);
                escalateRole = escalate.path("role").asText("");
                if (!ROLE.matcher(escalateRole).matches() || escalateRole.equals(role)) {
                    hardError(pointer + "/escalate/role", "'escalate.role' is another role", null);
                    valid = false;
                }
                if (!escalate.has("after")) {
                    hardError(pointer + "/escalate/after", "'escalate.after' is required", null);
                    valid = false;
                } else {
                    checkDuration(escalate.get("after"), pointer + "/escalate/after");
                    escalateAfter = compileDurationSpec(escalate.get("after"));
                }
            }
            JsonNode onTimeoutValue = null;
            JsonNode onTimeout = node.get("onTimeout");
            if (onTimeout != null && !(onTimeout.isTextual() && "fail".equals(onTimeout.asText()))) {
                if (!onTimeout.isObject() || onTimeout.size() != 1 || !onTimeout.has("value")) {
                    hardError(pointer + "/onTimeout", "'onTimeout' is 'fail' or {value: <answer>}", null);
                    valid = false;
                } else if (!answers.contains(onTimeout.get("value").asText())) {
                    hardError(pointer + "/onTimeout/value", "'onTimeout.value' is one of " + answers, null);
                    valid = false;
                } else {
                    onTimeoutValue = onTimeout.get("value");
                }
            }
            if (!valid) {
                return null;
            }
            String material = TemplateCompiler.compileTemplate(node.get("material"));
            String items = node.has("items") ? TemplateCompiler.compileTemplate(node.get("items")) : null;
            compileAndValidate(material, pointer + "/material");
            if (items != null) {
                compileAndValidate(items, pointer + "/items");
            }
            return new ReviewExpansion.Settings(role, material, items, deadline, remind, escalateRole, escalateAfter,
                    onTimeoutValue);
        }

        private static boolean isDecision(JsonNode body) {
            return body != null && body.isObject() && "decision".equals(body.path("call").asText(null));
        }

        private static boolean isRedact(JsonNode body) {
            return body != null && body.isObject() && "redact".equals(body.path("call").asText(null));
        }

        /**
         * SDD 3a.5, RF5: {@code call: redact} (Stint extension) — a {@code call: remote} to {@code with.task}, the
         * redactor, whose argument is {@code {input, types}} and whose output is checked against the redaction contract
         * of {@code stint-wire}. A valid result without doubt lowers the instance's level to {@code with.lowersTo}; with
         * {@code onDoubt}, a generated {@code <task>/route} switch sends a doubtful result there (the author's
         * {@code output.as}/{@code export.as} then go on the switch, as for a decision). Only directly in a flow, not as
         * a try's body.
         */
        private List<TaskNode> compileRedact(String name, JsonNode body, String pointer) {
            JsonNode with = body.get("with");
            if (with == null || !with.isObject()) {
                hardError(pointer + "/with", "'call: redact' requires 'with' (task, input, lowersTo)", null);
                return List.of();
            }
            checkKeys(with, pointer + "/with", REDACT_WITH_KEYS);
            if (!with.hasNonNull("task") || with.get("task").asText().isBlank()) {
                hardError(pointer + "/with/task", "'call: redact' requires 'with.task' (the routing key)", null);
                return List.of();
            }
            Sensitivity lowersTo = null;
            if (with.has("lowersTo")) {
                lowersTo = Sensitivity.parse(with.get("lowersTo").asText()).orElse(null);
                if (lowersTo == null) {
                    hardError(pointer + "/with/lowersTo", "'" + with.get("lowersTo").asText()
                            + "' is not a level (internal, confidential, restricted)", null);
                    return List.of();
                }
            }
            StringBuilder types = new StringBuilder("[");
            JsonNode typesNode = with.get("types");
            if (typesNode != null) {
                if (!typesNode.isArray()) {
                    hardError(pointer + "/with/types", "'with.types' is a list of data types (email, phone, ...)", null);
                    return List.of();
                }
                for (int i = 0; i < typesNode.size(); i++) {
                    types.append(i == 0 ? "" : ", ").append(com.fasterxml.jackson.databind.node.TextNode
                            .valueOf(typesNode.get(i).asText()));
                }
            }
            types.append(']');
            JsonNode onDoubt = body.get(ON_DOUBT);
            if (onDoubt != null && !onDoubt.isTextual()) {
                hardError(pointer + "/" + ON_DOUBT, "'onDoubt' is a task name or a flow directive, like 'then'", null);
                return List.of();
            }
            String input = with.has("input") ? TemplateCompiler.compileTemplate(with.get("input")) : ".";
            Expr request = compileAndValidate("{\"" + io.stintflow.wire.redaction.RedactionContract.INPUT + "\": ("
                    + input + "), \"" + io.stintflow.wire.redaction.RedactionContract.TYPES + "\": " + types + "}",
                    pointer + "/with");
            DataFlow authored = compileDataFlow(body, pointer, null, null);
            RedactionOutputContract contract = new RedactionOutputContract(lowersTo);
            String routingKey = with.path("task").asText();
            Duration timeout = compileTimeout(body, pointer);
            if (onDoubt == null) {
                return List.of(new CallRemoteNode(name, pointer, authored, compileThen(body), routingKey, timeout,
                        request, contract));
            }
            CallRemoteNode call = new CallRemoteNode(name, pointer, new DataFlow(authored.inputFrom(), null, null),
                    FlowDirective.CONTINUE, routingKey, timeout, request, contract);
            SwitchNode route = new SwitchNode(name + DecisionExpansion.ROUTE_SUFFIX,
                    pointer + DecisionExpansion.ROUTE_SUFFIX, new DataFlow(null, authored.outputAs(),
                            authored.exportAs()), FlowDirective.CONTINUE, List.of(
                            new SwitchNode.Case("doubt", Expr.jq("." + io.stintflow.wire.redaction.RedactionContract
                                    .DOUBT + " == true"), directive(onDoubt.asText())),
                            new SwitchNode.Case("default", null, compileThen(body))));
            return List.of(call, route);
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
            JsonNode uncertainReview = onUncertain != null && onUncertain.isObject() ? onUncertain.get("review") : null;
            if (onUncertain != null && !onUncertain.isTextual()
                    && (uncertainReview == null || onUncertain.size() != 1 || !uncertainReview.isObject())) {
                hardError(pointer + "/" + DecisionExpansion.ON_UNCERTAIN,
                        "'onUncertain' is a task name, a flow directive like 'then', or {review: {...}} (SDD 3a.4)",
                        null);
                valid = false;
            }
            if (uncertainReview != null && kind == DecisionKind.SCORE) {
                hardError(pointer + "/" + DecisionExpansion.ON_UNCERTAIN + "/review",
                        "a review chooses one of the options: not for 'kind: score'", null);
                valid = false;
            }
            ReviewExpansion.Settings reviewSettings = null;
            if (uncertainReview != null && valid) {
                checkKeys(uncertainReview, pointer + "/" + DecisionExpansion.ON_UNCERTAIN + "/review",
                        UNCERTAIN_REVIEW_KEYS);
                List<String> answers = options.isEmpty() ? List.of("true", "false")
                        : options.stream().map(DecisionOption::name).toList();
                reviewSettings = reviewSettings(uncertainReview, pointer + "/" + DecisionExpansion.ON_UNCERTAIN
                        + "/review", answers);
                valid = reviewSettings != null;
            }
            if (!valid) {
                return List.of();
            }
            DecisionRequest shape = DecisionRequest.shape(kind, options);
            String input = with.has("input") ? TemplateCompiler.compileTemplate(with.get("input")) : ".";
            Expr request = compileAndValidate(DecisionExpansion.requestTemplate(shape, input), pointer + "/with");
            DataFlow authored = compileDataFlow(body, pointer, null, null);
            FlowDirective otherwise = defaultThen != null ? defaultThen : compileThen(body);
            FlowDirective uncertainThen = reviewSettings != null ? FlowDirective.goTo(name + "/review")
                    : onUncertain == null ? null : directive(onUncertain.asText());
            List<TaskNode> expanded = DecisionExpansion.expand(name, pointer, shape, with.path("task").asText(),
                    compileTimeout(body, pointer), request, authored, otherwise, uncertainThen, optionThens);
            if (reviewSettings != null) {
                // SDD 3a.4 (8b): the review follows the decision's own branches with the human answer
                List<DecisionOption> reviewOptions = options.isEmpty()
                        ? List.of(new DecisionOption("true", null), new DecisionOption("false", null)) : options;
                List<SwitchNode.Case> cases = new ArrayList<>();
                for (int i = 0; i < reviewOptions.size(); i++) {
                    FlowDirective then = i < optionThens.size() && optionThens.get(i) != null ? optionThens.get(i)
                            : otherwise;
                    cases.add(ReviewExpansion.optionCase(reviewOptions.get(i).name(), then));
                }
                List<TaskNode> all = new ArrayList<>(expanded);
                all.addAll(ReviewExpansion.expand(name + "/review", pointer + "/review", reviewSettings,
                        ReviewContract.CHOICE, reviewOptions, pointer, reviewSources, null,
                        new DataFlow(null, authored.outputAs(), authored.exportAs()), cases));
                expanded = all;
            }
            List<TaskNode> result = expanded;
            LOG.log(Level.DEBUG, () -> sourceName + " " + pointer + ": 'call: decision' expands to\n"
                    + DefinitionPrinter.print(result));
            return result;
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
            if (declaredType != null && declaredType.startsWith(StintEvents.REVIEW_TYPE_PREFIX)) {
                hardError(withPointer + "/type", "emit type '" + declaredType + "' is reserved for review steps "
                        + "(SDD 3a.4): use 'call: review'", null);
            }
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
            if (isRedact(body)) {
                allowed.add(ON_DOUBT); // SDD 3a.5: a Stint extension key, only on 'call: redact'
            }
            if (isReview(body)) {
                allowed.add(ON_REJECT); // SDD 3a.4: a Stint extension key, only on 'call: review'
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
