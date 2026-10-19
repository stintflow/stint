package io.stintflow.worker.langchain4j;

import static io.stintflow.worker.langchain4j.Fakes.answer;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.Capability;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.request.json.JsonEnumSchema;
import dev.langchain4j.model.chat.request.json.JsonObjectSchema;
import dev.langchain4j.model.chat.response.ChatResponse;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.knowledge.DeterministicEmbedder;
import io.stintflow.knowledge.KnowledgeBatch;
import io.stintflow.knowledge.KnowledgeBatchJson;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.WriteReceipt;
import io.stintflow.knowledge.extraction.ExtractionLimits;
import io.stintflow.knowledge.extraction.Glossary;
import io.stintflow.knowledge.inmemory.CompositeKnowledgeStore;
import io.stintflow.spi.ErrorInfo.StandardType;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.RetentionClass;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.decision.SensitivityPolicy;

/**
 * SDD 3b.2, CA3 and CA8 to CA10, over a scripted model: the extraction asks for the schema's structured answer with only the
 * glossary terms the text can be about, refuses what is out of the schema without echoing it, resolves to ids that exist, stays
 * inside its token budget and never writes a partial batch. No network, no sleep.
 */
class KnowledgeExtractionWorkerTest {

    static final String MARKER = "SYNTH-PII-0801";
    static final WorkflowRef DEFINITION = new WorkflowRef("meetings", "notes", "2.0.0");
    static final KnowledgeScope SCOPE = KnowledgeScope.of(DEFINITION);

    /** A model that may claim JSON Schema support. */
    static final class Model extends Fakes.ScriptedModel {
        private final boolean schema;

        Model(boolean schema, Function<ChatRequest, ChatResponse> rule) {
            super(rule);
            this.schema = schema;
        }

        @Override
        public Set<Capability> supportedCapabilities() {
            return schema ? Set.of(Capability.RESPONSE_FORMAT_JSON_SCHEMA) : Set.of();
        }
    }

    private static final String ANSWER = "{\"entities\":["
            + "{\"ref\":\"s1\",\"type\":\"System\",\"mention\":\"the checkout svc\",\"passages\":[0]},"
            + "{\"ref\":\"p1\",\"type\":\"Person\",\"mention\":\"PERSON_a1b2c3\",\"passages\":[0,1]},"
            + "{\"ref\":\"d1\",\"type\":\"Decision\",\"mention\":\"move the review to Friday\","
            + "\"properties\":{\"summary\":\"The review moves to Friday\",\"status\":\"accepted\"},\"passages\":[1]}],"
            + "\"relations\":[{\"type\":\"participated\",\"from\":\"p1\",\"to\":\"meeting\"},"
            + "{\"type\":\"decided\",\"from\":\"meeting\",\"to\":\"d1\"},{\"type\":\"affects\",\"from\":\"d1\",\"to\":\"s1\"}]}";

    private static Glossary glossary() {
        return new Glossary("product", "1", List.of(
                new Glossary.Term("sys-checkout", "System", "Checkout Service", List.of("checkout", "the checkout svc"), null),
                new Glossary.Term("sys-payments", "System", "Payments API"),
                new Glossary.Term("term-sla", "Term", "SLA", List.of("service level agreement"), "uptime promise")));
    }

    private static ObjectNode input() {
        ObjectNode in = Json.obj();
        in.putObject("meeting").put("meetingId", "m-001").put("heldOn", "2026-10-19");
        ArrayNode turns = in.putArray("turns");
        turns.addObject().put("speaker", "s1").put("text", "PERSON_a1b2c3 said the checkout svc needs a fix. " + MARKER);
        turns.addObject().put("speaker", "s2").put("text", "Agreed: move the review to Friday.");
        in.put("schema", "meetings@1").put("glossary", "product@1");
        return in;
    }

    private static TaskContext task(JsonNode input, Sensitivity level) {
        return new TaskContext("/do/1/extract", "i-1", "c-1", 1, input, DEFINITION, "chn-1", Lineage.NONE, null, level,
                null);
    }

    private static KnowledgeExtractionWorker.Builder worker(Fakes.ScriptedModel model) {
        return KnowledgeExtractionWorker.builder().chatModel(model).provider("scripted", false)
                .passageChars(80).glossaries(ref -> ref.equals("product@1") ? glossary() : null);
    }

    private static JsonNode run(KnowledgeExtractionWorker worker, TaskContext ctx) {
        return worker.execute(ctx).toCompletableFuture().join();
    }

    private static TaskFailure failure(KnowledgeExtractionWorker worker, TaskContext ctx) {
        try {
            worker.execute(ctx).toCompletableFuture().join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof TaskFailure failure) {
                return failure;
            }
            throw e;
        }
        throw new AssertionError("the extraction did not fail");
    }

    // --- CA3: the call and the batch -------------------------------------------------------------------------------

    @Test
    void the_model_is_asked_in_the_schema_with_only_the_terms_the_text_names_and_the_answer_becomes_a_batch() {
        Model model = new Model(true, r -> answer(ANSWER));

        JsonNode result = run(worker(model).build(), task(input(), Sensitivity.CONFIDENTIAL));

        ChatRequest request = model.requests.getFirst();
        JsonObjectSchema root = (JsonObjectSchema) request.responseFormat().jsonSchema().rootElement();
        assertThat(root.properties()).containsKeys("entities", "relations");
        assertThat(root.required()).containsExactly("entities", "relations");
        JsonObjectSchema entity = (JsonObjectSchema) ((dev.langchain4j.model.chat.request.json.JsonArraySchema) root
                .properties().get("entities")).items();
        assertThat(((JsonEnumSchema) entity.properties().get("type")).enumValues())
                .containsExactlyInAnyOrder("Decision", "ExternalReference", "Person", "Requirement", "System", "Term");
        String system = ((SystemMessage) request.messages().getFirst()).text();
        assertThat(system).contains("schema meetings@1.0.0").contains("sys-checkout | Checkout Service | checkout, the checkout svc")
                .doesNotContain("sys-payments").doesNotContain("term-sla")   // not named in the text
                .contains("never write a name").doesNotContain("Answer only with {");
        assertThat(((UserMessage) request.messages().get(1)).singleText()).startsWith("[0] s1: PERSON_a1b2c3");

        KnowledgeBatch batch = KnowledgeBatchJson.fromJson(result.get("batch"));
        assertThat(batch.schemaVersion()).isEqualTo("meetings@1.0.0");
        assertThat(batch.documentId()).isEqualTo("meeting:m-001");
        assertThat(batch.source().instanceId()).isEqualTo("i-1");
        assertThat(batch.source().definition()).isEqualTo(DEFINITION);
        assertThat(batch.entities()).extracting(e -> e.type()).containsExactlyInAnyOrder("Meeting", "System", "Person",
                "Decision");
        assertThat(batch.entities().stream().filter(e -> e.type().equals("System")).findFirst().get().entityId())
                .isEqualTo("sys-checkout");   // the alias resolved to the glossary
        assertThat(batch.entities().stream().filter(e -> e.type().equals("Person")).findFirst().get().sensitivity())
                .isEqualTo(Sensitivity.CONFIDENTIAL);
        assertThat(batch.relations()).hasSize(3);
        assertThat(result.path("resolutions").path("ALIAS").asInt()).isEqualTo(1);
        assertThat(result.path("usage").path("calls").asInt()).isEqualTo(1);
        assertThat(result.path("usage").path("promptVersion").asText()).isEqualTo(KnowledgeExtractionWorker.PROMPT_VERSION);
        assertThat(KnowledgeBatchJson.toJson(batch)).isEqualTo(result.get("batch"));   // the trip loses nothing
    }

    @Test
    void without_json_schema_support_the_shape_is_spelled_out_and_a_fenced_answer_is_read() {
        Model model = new Model(false, r -> answer("```json\n" + ANSWER + "\n```"));

        JsonNode result = run(worker(model).build(), task(input(), Sensitivity.CONFIDENTIAL));

        assertThat(model.requests.getFirst().responseFormat()).isNull();
        assertThat(((SystemMessage) model.requests.getFirst().messages().getFirst()).text())
                .contains("Answer only with {\"entities\"");
        assertThat(result.path("batch").path("entities")).isNotEmpty();
    }

    @Test
    void the_same_extraction_twice_gives_the_same_batch_and_writing_it_twice_changes_nothing() throws Exception {
        Model model = new Model(true, r -> answer(ANSWER));
        KnowledgeExtractionWorker worker = worker(model).build();
        KnowledgeBatch first = KnowledgeBatchJson.fromJson(run(worker, task(input(), Sensitivity.CONFIDENTIAL)).get("batch"));
        KnowledgeBatch second = KnowledgeBatchJson.fromJson(run(worker, task(input(), Sensitivity.CONFIDENTIAL)).get("batch"));
        CompositeKnowledgeStore store = new CompositeKnowledgeStore(new DeterministicEmbedder(),
                new FilesystemBlobStore(Files.createTempDirectory("stint-3b2-worker")), new RetentionClass("restricted",
                        Duration.ofDays(30)), java.time.InstantSource.system());

        assertThat(first).isEqualTo(second);
        assertThat(store.write(SCOPE, first).toCompletableFuture().get(10, TimeUnit.SECONDS).status())
                .isEqualTo(WriteReceipt.Status.APPLIED);
        assertThat(store.write(SCOPE, second).toCompletableFuture().get(10, TimeUnit.SECONDS).status())
                .isEqualTo(WriteReceipt.Status.UNCHANGED);
    }

    @Test
    void a_mention_the_base_already_holds_resolves_to_its_id_and_a_new_one_is_reported_beside_its_near_twin() throws Exception {
        Model model = new Model(true, r -> answer(ANSWER));
        CompositeKnowledgeStore store = new CompositeKnowledgeStore(new DeterministicEmbedder(),
                new FilesystemBlobStore(Files.createTempDirectory("stint-3b2-existing")), new RetentionClass("restricted",
                        Duration.ofDays(30)), java.time.InstantSource.system());
        KnowledgeExtractionWorker worker = worker(model).store(store).build();
        KnowledgeBatch first = KnowledgeBatchJson.fromJson(run(worker, task(input(), Sensitivity.CONFIDENTIAL)).get("batch"));
        store.write(SCOPE, first).toCompletableFuture().get(10, TimeUnit.SECONDS);
        String decisionId = first.entities().stream().filter(e -> e.type().equals("Decision")).findFirst().get().entityId();

        // another meeting says the same decision, in other words of the same name: it resolves to the same id
        ObjectNode other = input();
        other.with("meeting").put("meetingId", "m-002");
        JsonNode result = run(worker, task(other, Sensitivity.CONFIDENTIAL));

        KnowledgeBatch second = KnowledgeBatchJson.fromJson(result.get("batch"));
        assertThat(second.entities().stream().filter(e -> e.type().equals("Decision")).findFirst().get().entityId())
                .isEqualTo(decisionId);
        assertThat(result.path("resolutions").path("EXISTING").asInt()).isGreaterThanOrEqualTo(1);
    }

    // --- CA3: out of the schema ------------------------------------------------------------------------------------

    @Test
    void an_answer_out_of_the_schema_fails_with_validation_400_naming_fields_and_never_values_and_writes_nothing() {
        String bad = "{\"entities\":[{\"ref\":\"x\",\"type\":\"Spaceship\",\"mention\":\"" + MARKER + "\",\"passages\":[0]},"
                + "{\"ref\":\"y\",\"type\":\"Person\",\"mention\":\"Dana Rivera\",\"passages\":[5]}],\"relations\":[]}";
        Model model = new Model(true, r -> answer(bad));

        TaskFailure failure = failure(worker(model).build(), task(input(), Sensitivity.CONFIDENTIAL));

        assertThat(failure.toErrorInfo().type()).isEqualTo(StandardType.VALIDATION.uri());
        assertThat(failure.toErrorInfo().status()).isEqualTo(400);
        assertThat(failure.toErrorInfo().detail()).contains("out of the schema meetings@1.0.0").contains("violation(s)")
                .contains("$.entities[0].type").contains("$.entities[1].mention").contains("$.entities[1].passages[0]")
                .doesNotContain(MARKER).doesNotContain("Rivera").doesNotContain("Spaceship");
        assertThat(model.requests).hasSize(1);   // no repair call
    }

    @Test
    void an_answer_that_is_not_json_is_a_validation_error_too() {
        Model model = new Model(true, r -> answer("I could not find anything, sorry."));

        TaskFailure failure = failure(worker(model).build(), task(input(), Sensitivity.CONFIDENTIAL));

        assertThat(failure.toErrorInfo().status()).isEqualTo(400);
        assertThat(failure.toErrorInfo().detail()).contains("not JSON");
    }

    // --- CA9: the budget -----------------------------------------------------------------------------------------

    @Test
    void a_document_over_the_budget_is_refused_before_any_call_and_nothing_is_written() {
        Model model = new Model(true, r -> answer(ANSWER));

        TaskFailure failure = failure(worker(model).limits(new ExtractionLimits(40, 1_500, 3_000, 2_000)).build(),
                task(input(), Sensitivity.CONFIDENTIAL));

        assertThat(failure.toErrorInfo().type()).isEqualTo(StandardType.RUNTIME.uri());
        assertThat(failure.toErrorInfo().status()).isEqualTo(500);
        assertThat(failure.toErrorInfo().detail()).contains("over the budget of 2000");
        assertThat(model.requests).isEmpty();
    }

    @Test
    void a_long_transcript_goes_in_windows_and_each_window_gets_only_its_own_terms() {
        List<String> prompts = new ArrayList<>();
        Model model = new Model(true, r -> {
            prompts.add(((SystemMessage) r.messages().getFirst()).text());
            return answer("{\"entities\":[],\"relations\":[]}");
        });
        ObjectNode in = Json.obj();
        in.putObject("meeting").put("meetingId", "m-long");
        ArrayNode passages = in.putArray("passages");
        passages.add("The checkout service was slow. " + "x".repeat(2_000));
        passages.add("Nothing about systems here. " + "y".repeat(2_000));
        passages.add("The SLA was discussed. " + "z".repeat(2_000));
        in.put("schema", "meetings@1").put("glossary", "product@1");

        JsonNode result = run(worker(model).limits(new ExtractionLimits(40, 1_500, 600, 40_000)).build(),
                task(in, Sensitivity.INTERNAL));

        assertThat(result.path("usage").path("windows").asInt()).isEqualTo(3);
        assertThat(prompts).hasSize(3);
        assertThat(prompts.get(0)).contains("sys-checkout").doesNotContain("term-sla");
        assertThat(prompts.get(1)).doesNotContain("sys-checkout").doesNotContain("term-sla");
        assertThat(prompts.get(2)).contains("term-sla").doesNotContain("sys-checkout");
        assertThat(result.path("usage").path("calls").asInt()).isEqualTo(3);
    }

    // --- sensitivity and configuration -------------------------------------------------------------------------------

    @Test
    void a_restricted_instance_is_not_given_to_a_remote_model() {
        Model model = new Model(true, r -> answer(ANSWER));
        KnowledgeExtractionWorker remote = KnowledgeExtractionWorker.builder().chatModel(model)
                .provider("hosted", true).policy(new SensitivityPolicy(Set.of()))
                .passageChars(80).glossaries(ref -> glossary()).build();

        TaskFailure failure = failure(remote, task(input(), Sensitivity.RESTRICTED));

        assertThat(failure.toErrorInfo().type()).isEqualTo(StandardType.AUTHORIZATION.uri());
        assertThat(failure.toErrorInfo().status()).isEqualTo(403);
        assertThat(model.requests).isEmpty();
        // a local model may
        KnowledgeExtractionWorker local = KnowledgeExtractionWorker.builder().chatModel(new Model(true, r -> answer(ANSWER)))
                .provider("litjev", false).policy(new SensitivityPolicy(Set.of())).passageChars(80).glossaries(ref -> glossary()).build();
        assertThat(run(local, task(input(), Sensitivity.RESTRICTED)).path("batch").path("entities")).isNotEmpty();
    }

    @Test
    void an_unknown_schema_or_glossary_and_an_input_without_text_are_refused_without_calling_the_model() {
        Model model = new Model(true, r -> answer(ANSWER));
        KnowledgeExtractionWorker worker = worker(model).build();

        ObjectNode unknownSchema = input().put("schema", "meetings@9");
        ObjectNode unknownGlossary = input().put("glossary", "other@1");
        ObjectNode noText = input();
        noText.remove("turns");
        ObjectNode noMeeting = input();
        noMeeting.remove("meeting");

        assertThat(failure(worker, task(unknownSchema, Sensitivity.INTERNAL)).toErrorInfo().type())
                .isEqualTo(StandardType.CONFIGURATION.uri());
        assertThat(failure(worker, task(unknownGlossary, Sensitivity.INTERNAL)).toErrorInfo().type())
                .isEqualTo(StandardType.CONFIGURATION.uri());
        assertThat(failure(worker, task(noText, Sensitivity.INTERNAL)).toErrorInfo().status()).isEqualTo(400);
        assertThat(failure(worker, task(noMeeting, Sensitivity.INTERNAL)).toErrorInfo().status()).isEqualTo(400);
        assertThat(model.requests).isEmpty();
    }
}
