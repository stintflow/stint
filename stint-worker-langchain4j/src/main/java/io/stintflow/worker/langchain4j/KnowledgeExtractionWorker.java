package io.stintflow.worker.langchain4j;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.Capability;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.request.ResponseFormat;
import dev.langchain4j.model.chat.request.ResponseFormatType;
import dev.langchain4j.model.chat.request.json.JsonObjectSchema;
import dev.langchain4j.model.chat.request.json.JsonSchema;
import dev.langchain4j.model.chat.response.ChatResponse;
import io.stintflow.knowledge.EntityMatch;
import io.stintflow.knowledge.EntityQuery;
import io.stintflow.knowledge.KnowledgeBatchJson;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.KnowledgeStore;
import io.stintflow.knowledge.Passage;
import io.stintflow.knowledge.Source;
import io.stintflow.knowledge.extraction.BatchAssembler;
import io.stintflow.knowledge.extraction.EntityResolver;
import io.stintflow.knowledge.extraction.ExtractionLimits;
import io.stintflow.knowledge.extraction.Glossary;
import io.stintflow.knowledge.extraction.GlossarySelector;
import io.stintflow.knowledge.extraction.PassageSplitter;
import io.stintflow.knowledge.schema.KnowledgeSchema;
import io.stintflow.knowledge.schema.SchemaRegistry;
import io.stintflow.knowledge.schema.SchemaValidator;
import io.stintflow.spi.ErrorInfo.StandardType;
import io.stintflow.spi.ExecutionTracer;
import io.stintflow.spi.ModelProvider;
import io.stintflow.spi.Sensitivity;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;
import io.stintflow.worker.decision.DecisionFailures;
import io.stintflow.worker.decision.SensitivityPolicy;

/**
 * Extracts the entities and relations of a meeting into a knowledge batch (SDD 3b.2, 4.3 to 4.7): the handler of the route
 * {@code extract-knowledge}.
 * <p>
 * <b>Input.</b> {@code {"meeting": {"meetingId", "heldOn"?}, "turns": [{"speaker", "text"}] | "passages": ["text"],
 * "schema": "meetings@1", "glossary": "product@1"?, "scope": {"namespace", "base"}?, "language": "en"?}}. The text is already
 * redacted ({@code call: redact}): a person is a pseudonym in it, and the schema refuses anything else as a mention.
 * <p>
 * <b>What it does.</b> Splits the turns into passages; groups them into windows; for each window picks only the glossary terms
 * the window can be about and asks the model for the structured answer (a JSON Schema generated from the knowledge schema when the
 * model supports one); checks the answer against the schema; resolves each mention to an existing id before it creates anything;
 * and assembles one {@code KnowledgeBatch}. The answer of the task is that batch (as JSON) beside the duplicate candidates, the
 * resolution counts and the estimated tokens. It writes nothing: the write is a step of its own (SDD 3b.3).
 * <p>
 * <b>Failure.</b> An answer out of the schema fails the task with {@code validation} 400 — no partial batch, no repair call —
 * naming fields and rules, never values. A budget over {@link ExtractionLimits#maxTokensPerDocument()} stops with
 * {@code runtime} 500 before any call that would pass it. A restricted instance with a remote model is refused, as for any
 * LLM worker of SDD 3a.5.
 */
public final class KnowledgeExtractionWorker implements TaskHandler {

    /** Goes into the audit of the call: a change of the prompt is a change of the extraction. */
    public static final String PROMPT_VERSION = "knowledge-extraction/1";
    /** The most an answer may take, in tokens (estimated), counted in the budget of each call. */
    static final int ANSWER_TOKENS = 1_500;
    /** Instructions and the description of the schema, in estimated tokens (counted in the budget of each call). */
    static final int FIXED_TOKENS = 1_000;

    private static final Logger LOG = LoggerFactory.getLogger(KnowledgeExtractionWorker.class);

    private final ChatModel chatModel;
    private final ModelProvider provider;
    private final SensitivityPolicy policy;
    private final SchemaRegistry schemas;
    private final Function<String, Glossary> glossaries;
    private final KnowledgeStore store;
    private final ExtractionLimits limits;
    private final ExecutionTracer tracer;
    private final InstantSource clock;
    private final Function<Throwable, Optional<Duration>> retryAfter;
    private final Duration rateLimitRetryAfter;
    private final int passageChars;

    private KnowledgeExtractionWorker(Builder b) {
        this.chatModel = Objects.requireNonNull(b.chatModel, "chatModel is required");
        this.provider = b.provider != null ? b.provider : new ModelProvider(
                chatModel.provider() == null ? "undeclared" : chatModel.provider().name().toLowerCase(Locale.ROOT), null,
                true);   // SDD 3a.5: without a declaration the model counts as remote
        this.policy = b.policy;
        this.schemas = Objects.requireNonNull(b.schemas, "schemas is required");
        this.glossaries = b.glossaries;
        this.store = b.store;
        this.limits = b.limits;
        this.tracer = b.tracer;
        this.clock = b.clock;
        this.retryAfter = b.retryAfter;
        this.rateLimitRetryAfter = b.rateLimitRetryAfter;
        this.passageChars = b.passageChars;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext ctx) {
        try (TaskIdentifiers ids = TaskIdentifiers.inMdc(ctx)) {
            refuseByPolicy(ctx);
            return CompletableFuture.completedFuture(run(ctx));
        } catch (TaskFailure failure) {
            return CompletableFuture.failedFuture(failure);
        } catch (RuntimeException e) {
            LOG.warn("Extraction of task {} in instance {} failed ({})", ctx.taskId(), ctx.instanceId(),
                    e.getClass().getName());
            return CompletableFuture.failedFuture(new TaskFailure(StandardType.RUNTIME.uri(), 500,
                    "The extraction failed (" + e.getClass().getSimpleName() + ")", null));
        }
    }

    private void refuseByPolicy(TaskContext ctx) {
        if (policy == null) {
            return;
        }
        Optional<String> refusal = policy.refusal(ctx.sensitivity(), provider);
        if (refusal.isPresent()) {
            LOG.warn("Task {} of instance {}: the sensitivity policy refused the model: {}", ctx.taskId(),
                    ctx.instanceId(), refusal.get());
            throw DecisionFailures.refused("The sensitivity policy refused the model: " + refusal.get());
        }
    }

    private JsonNode run(TaskContext ctx) {
        JsonNode in = ctx.input();
        KnowledgeSchema schema = schemas.resolve(in.path("schema").asText("")).orElseThrow(() ->
                DecisionFailures.configuration(400, "The schema asked for is not one this worker holds"));
        Glossary glossary = in.hasNonNull("glossary") ? glossaries.apply(in.get("glossary").asText())
                : new Glossary("none", "0", List.of());
        if (glossary == null) {
            throw DecisionFailures.configuration(400, "The glossary asked for is not one this worker holds");
        }
        String meetingId = in.path("meeting").path("meetingId").asText("");
        if (meetingId.isBlank()) {
            throw DecisionFailures.validation("meeting.meetingId is required");
        }
        String language = in.path("language").asText(schema.language());
        KnowledgeScope scope = in.has("scope")
                ? KnowledgeScope.named(in.path("scope").path("namespace").asText(), in.path("scope").path("base").asText())
                : KnowledgeScope.of(ctx.definition());
        Sensitivity level = ctx.sensitivity() == null ? Sensitivity.RESTRICTED : ctx.sensitivity();

        List<String> texts = passageTexts(in);
        BatchAssembler assembler = new BatchAssembler(scope, schema, meetingId,
                in.path("meeting").hasNonNull("heldOn") ? in.path("meeting").get("heldOn").asText() : null, level,
                new EntityResolver(glossary, existing(scope, level)));
        List<Passage> passages = PassageSplitter.passages(scope, assembler.documentId(), texts, language, level);
        List<List<Passage>> windows = PassageSplitter.windows(passages, limits.maxWindowTokens());

        // the budget is checked before the first call: a document that cannot fit is refused whole, with nothing called
        int estimated = windows.stream().mapToInt(w -> callTokens(glossary, w, limits)).sum();
        if (estimated > limits.maxTokensPerDocument()) {
            throw budget(estimated);
        }

        ctx.report().provider(provider);   // SDD 3a.5, RF8: the model is about to see the text
        MeteredChatModel model = new MeteredChatModel(chatModel, tracer, ctx, clock);
        boolean constrained = chatModel.supportedCapabilities().contains(Capability.RESPONSE_FORMAT_JSON_SCHEMA);
        int spent = 0;
        int truncated = 0;
        for (List<Passage> window : windows) {
            String windowText = windowText(window);
            GlossarySelector.Selection terms = GlossarySelector.select(glossary, windowText, limits);
            truncated += terms.truncated() ? 1 : 0;
            int cost = FIXED_TOKENS + terms.estimatedTokens() + ExtractionLimits.tokens(windowText) + ANSWER_TOKENS;
            if (spent + cost > limits.maxTokensPerDocument()) {
                throw budget(spent + cost);
            }
            spent += cost;
            JsonNode answer = ask(model, schema, terms, windowText, window.size(), constrained);
            List<String> problems = SchemaValidator.violations(schema, answer, window.size());
            if (!problems.isEmpty()) {
                LOG.warn("Task {} of instance {}: the extraction is out of the schema {} ({} violation(s))", ctx.taskId(),
                        ctx.instanceId(), schema.fullVersion(), problems.size());
                throw DecisionFailures.validation("The extraction is out of the schema " + schema.fullVersion() + " ("
                        + problems.size() + " violation(s)): " + String.join("; ", problems.subList(0,
                                Math.min(10, problems.size()))));
            }
            assembler.addWindow(window, answer);
        }

        BatchAssembler.Assembly assembly = assembler.build("extract:" + ctx.instanceId() + ":" + ctx.taskId(),
                new Source(ctx.instanceId(), ctx.chainId(), ctx.definition()));
        ObjectNode result = Json.obj();
        result.set("batch", KnowledgeBatchJson.toJson(assembly.batch()));
        ArrayNode candidates = result.putArray("duplicateCandidates");
        for (BatchAssembler.DuplicateCandidate c : assembly.duplicateCandidates()) {
            ObjectNode node = candidates.addObject();
            node.put("entityId", c.entityId()).put("otherId", c.otherId()).put("score", c.score())
                    .put("criterion", c.criterionVersion());
            ArrayNode reasons = node.putArray("reasons");
            c.reasons().forEach(reasons::add);
        }
        ObjectNode resolutions = result.putObject("resolutions");
        assembly.resolutions().forEach(resolutions::put);
        LlmUsage used = model.usage(0);
        ObjectNode usage = result.putObject("usage");
        usage.put("windows", windows.size()).put("calls", used.calls()).put("inputTokens", used.inputTokens())
                .put("outputTokens", used.outputTokens()).put("estimatedTokens", spent)
                .put("glossaryTruncatedWindows", truncated).put("promptVersion", PROMPT_VERSION);
        return result;
    }

    private TaskFailure budget(int estimated) {
        return new TaskFailure(StandardType.RUNTIME.uri(), 500, "The extraction would take about " + estimated
                + " tokens, over the budget of " + limits.maxTokensPerDocument() + " per document: nothing was written",
                null);
    }

    private static int callTokens(Glossary glossary, List<Passage> window, ExtractionLimits limits) {
        String text = windowText(window);
        return FIXED_TOKENS + GlossarySelector.select(glossary, text, limits).estimatedTokens()
                + ExtractionLimits.tokens(text) + ANSWER_TOKENS;
    }

    private List<String> passageTexts(JsonNode in) {
        List<String> texts = new ArrayList<>();
        if (in.has("passages")) {
            in.get("passages").forEach(p -> texts.add(p.asText()));
        } else if (in.has("turns")) {
            List<PassageSplitter.Turn> turns = new ArrayList<>();
            in.get("turns").forEach(t -> turns.add(new PassageSplitter.Turn(t.path("speaker").asText("speaker"),
                    t.path("text").asText(""))));
            texts.addAll(PassageSplitter.split(turns, passageChars));
        }
        if (texts.isEmpty()) {
            throw DecisionFailures.validation("The input has no 'turns' or 'passages' to extract from");
        }
        return texts;
    }

    /** The existing entities the resolver reads: exact lookups in the base, if the worker has one. */
    private EntityResolver.Existing existing(KnowledgeScope scope, Sensitivity ceiling) {
        if (store == null) {
            return EntityResolver.Existing.NONE;
        }
        return new EntityResolver.Existing() {
            @Override
            public List<EntityResolver.Known> exact(String type, String normalizedName) {
                List<EntityResolver.Known> found = new ArrayList<>();
                for (EntityMatch m : store.entities(scope, new EntityQuery(type, normalizedName, ceiling, 10))
                        .toCompletableFuture().join()) {
                    found.add(new EntityResolver.Known(m.entityId(), m.type(), m.label(), m.aliases(), null));
                }
                return found;
            }

            @Override
            public List<EntityResolver.Known> ofType(String type) {
                return List.of();   // the base has no listing by type: candidates come from the glossary and exact lookups
            }
        };
    }

    // --- the prompt and the call -------------------------------------------------------------------------------

    private static String windowText(List<Passage> window) {
        StringBuilder text = new StringBuilder();
        for (int i = 0; i < window.size(); i++) {
            text.append('[').append(i).append("] ").append(window.get(i).text()).append('\n');
        }
        return text.toString();
    }

    private JsonNode ask(MeteredChatModel model, KnowledgeSchema schema, GlossarySelector.Selection terms, String windowText,
            int passages, boolean constrained) {
        ChatRequest.Builder chat = ChatRequest.builder().messages(
                SystemMessage.from(systemMessage(schema, terms, !constrained)), UserMessage.from(windowText));
        if (constrained) {
            chat.responseFormat(ResponseFormat.builder().type(ResponseFormatType.JSON).jsonSchema(jsonSchema(schema)).build());
        }
        ChatResponse response;
        try {
            response = model.chat(chat.build());
        } catch (RuntimeException e) {
            throw ProviderErrors.toTaskFailure(e, retryAfter, rateLimitRetryAfter);
        }
        String text = response.aiMessage() == null ? null : response.aiMessage().text();
        try {
            return Json.read(unfenced(text).getBytes(StandardCharsets.UTF_8));
        } catch (RuntimeException e) {
            throw DecisionFailures.validation("The extraction is out of the schema " + schema.fullVersion()
                    + ": the model's answer is not JSON");
        }
    }

    static String systemMessage(KnowledgeSchema schema, GlossarySelector.Selection terms, boolean spellOutSchema) {
        StringBuilder prompt = new StringBuilder("You extract a knowledge graph from a redacted meeting transcript. ")
                .append("The user sends numbered passages: [0], [1], ... Answer with one JSON object, nothing else.\n")
                .append("Use only these node types and relations (schema ").append(schema.fullVersion()).append(").\n");
        for (KnowledgeSchema.NodeType type : schema.nodes().values()) {
            if (type.name().equals("Meeting")) {
                continue;   // created by the worker, not by the model
            }
            prompt.append("Node ").append(type.name()).append(": the mention is its ")
                    .append(type.key().equals("pseudonym") ? "pseudonym" : "short canonical name");
            List<String> props = new ArrayList<>();
            type.properties().values().forEach(p -> {
                if (!p.name().equals(type.key())) {
                    props.add(p.name() + (p.required() ? " (required)" : "")
                            + (p.type() == KnowledgeSchema.PropertyType.ENUM ? " one of " + new java.util.TreeSet<>(p.values())
                                    : ""));
                }
            });
            if (!props.isEmpty()) {
                prompt.append("; properties: ").append(String.join(", ", props));
            }
            prompt.append('\n');
        }
        for (KnowledgeSchema.RelationType r : schema.relations().values()) {
            prompt.append("Relation ").append(r.name()).append(": from ").append(new java.util.TreeSet<>(r.from()))
                    .append(" to ").append(new java.util.TreeSet<>(r.to())).append('\n');
        }
        prompt.append("A person appears only as a pseudonym such as PERSON_ab12cd: use it as the mention; never write a name.\n")
                .append("Give every entity a short unique ref, the ordinals of the passages that mention it, and its type. ")
                .append("Relations name entity refs; the meeting itself is the ref \"")
                .append(SchemaValidator.MEETING_REF).append("\".\n");
        if (!terms.terms().isEmpty()) {
            prompt.append("Known terms (id | name | aliases). If a mention is one of them, set glossaryId to its id ")
                    .append("and use its name as the mention:\n");
            terms.terms().forEach(t -> prompt.append(GlossarySelector.line(t)).append('\n'));
        }
        if (spellOutSchema) {
            prompt.append("Answer only with {\"entities\": [{\"ref\": \"e1\", \"type\": \"...\", \"mention\": \"...\", ")
                    .append("\"glossaryId\": \"...\" (optional), \"properties\": {...} (optional), \"passages\": [0]}], ")
                    .append("\"relations\": [{\"type\": \"...\", \"from\": \"e1\", \"to\": \"e2\"}]}\n");
        }
        return prompt.toString();
    }

    static JsonSchema jsonSchema(KnowledgeSchema schema) {
        List<String> types = schema.nodes().keySet().stream().filter(t -> !t.equals("Meeting")).sorted().toList();
        List<String> relations = schema.relations().keySet().stream().sorted().toList();
        JsonObjectSchema entity = JsonObjectSchema.builder()
                .addStringProperty("ref").addEnumProperty("type", types).addStringProperty("mention")
                .addStringProperty("glossaryId")
                .addProperty("properties", JsonObjectSchema.builder().additionalProperties(true).build())
                .addProperty("passages", dev.langchain4j.model.chat.request.json.JsonArraySchema.builder()
                        .items(new dev.langchain4j.model.chat.request.json.JsonIntegerSchema()).build())
                .required("ref", "type", "mention", "passages").additionalProperties(false).build();
        JsonObjectSchema relation = JsonObjectSchema.builder()
                .addEnumProperty("type", relations).addStringProperty("from").addStringProperty("to")
                .required("type", "from", "to").additionalProperties(false).build();
        JsonObjectSchema root = JsonObjectSchema.builder()
                .addProperty("entities", dev.langchain4j.model.chat.request.json.JsonArraySchema.builder().items(entity).build())
                .addProperty("relations", dev.langchain4j.model.chat.request.json.JsonArraySchema.builder().items(relation).build())
                .required("entities", "relations").additionalProperties(false).build();
        return JsonSchema.builder().name("knowledge").rootElement(root).build();
    }

    /** Some models wrap JSON in a Markdown fence even when told not to. */
    private static String unfenced(String text) {
        if (text == null) {
            return "";
        }
        String trimmed = text.strip();
        if (trimmed.startsWith("```")) {
            int start = trimmed.indexOf('\n');
            int end = trimmed.lastIndexOf("```");
            if (start > 0 && end > start) {
                return trimmed.substring(start + 1, end).strip();
            }
        }
        return trimmed;
    }

    public static final class Builder {
        private ChatModel chatModel;
        private ModelProvider provider;
        private SensitivityPolicy policy;
        private SchemaRegistry schemas = SchemaRegistry.withReferenceSchemas();
        private Function<String, Glossary> glossaries = reference -> null;
        private KnowledgeStore store;
        private ExtractionLimits limits = ExtractionLimits.DEFAULT;
        private ExecutionTracer tracer = ExecutionTracer.NOOP;
        private InstantSource clock = InstantSource.system();
        private Function<Throwable, Optional<Duration>> retryAfter = t -> Optional.empty();
        private Duration rateLimitRetryAfter;
        private int passageChars = 1_500;

        private Builder() {
        }

        public Builder chatModel(ChatModel chatModel) {
            this.chatModel = chatModel;
            return this;
        }

        /** Who runs the model, for the policy and the audit (SDD 3a.5); without it the model counts as remote. */
        public Builder provider(String name, boolean remote) {
            this.provider = new ModelProvider(name, null, remote);
            return this;
        }

        public Builder policy(SensitivityPolicy policy) {
            this.policy = policy;
            return this;
        }

        /** The schemas this worker knows; default: the reference ones ({@code meetings@1}). */
        public Builder schemas(SchemaRegistry schemas) {
            this.schemas = Objects.requireNonNull(schemas, "schemas");
            return this;
        }

        /** The glossaries by reference ({@code product@1}); an unknown reference answers {@code null}. */
        public Builder glossaries(Function<String, Glossary> glossaries) {
            this.glossaries = Objects.requireNonNull(glossaries, "glossaries");
            return this;
        }

        /** The base, read to resolve a mention to an entity it already holds; none: only the glossary resolves. */
        public Builder store(KnowledgeStore store) {
            this.store = store;
            return this;
        }

        public Builder limits(ExtractionLimits limits) {
            this.limits = Objects.requireNonNull(limits, "limits");
            return this;
        }

        public Builder tracer(ExecutionTracer tracer) {
            this.tracer = Objects.requireNonNull(tracer, "tracer");
            return this;
        }

        public Builder clock(InstantSource clock) {
            this.clock = Objects.requireNonNull(clock, "clock");
            return this;
        }

        public Builder retryAfter(Function<Throwable, Optional<Duration>> extractor) {
            this.retryAfter = Objects.requireNonNull(extractor, "extractor");
            return this;
        }

        public Builder rateLimitRetryAfter(Duration retryAfter) {
            this.rateLimitRetryAfter = retryAfter;
            return this;
        }

        /** The most characters of a passage when the input gives turns to split. Default 1 500. */
        public Builder passageChars(int passageChars) {
            this.passageChars = passageChars;
            return this;
        }

        public KnowledgeExtractionWorker build() {
            return new KnowledgeExtractionWorker(this);
        }
    }
}
