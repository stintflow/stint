package io.stintflow.example.meeting;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.knowledge.DeleteRequest;
import io.stintflow.knowledge.DeterministicEmbedder;
import io.stintflow.knowledge.EntityIds;
import io.stintflow.knowledge.KnowledgeBatch;
import io.stintflow.knowledge.KnowledgeBatchJson;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.extraction.Glossary;
import io.stintflow.knowledge.inmemory.CompositeKnowledgeStore;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.RetentionClass;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;
import io.stintflow.wire.redaction.RedactionContract;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.langchain4j.KnowledgeExtractionWorker;
import io.stintflow.worker.redaction.LayeredRedactor;
import io.stintflow.worker.redaction.PatternLayer;
import io.stintflow.worker.redaction.Pseudonyms;
import io.stintflow.worker.redaction.RedactionHandler;
import io.stintflow.worker.redaction.TermListLayer;

/**
 * SDD 3b.2, CA10 and CA11, end to end over the synthetic corpus: a meeting is redacted (with stable pseudonyms), extracted by a
 * scripted model into a batch, written to the base and — for one person — erased. Nothing of the planted personal data reaches the
 * batch or the base; the person is a pseudonym that is the same in every meeting of the base, and the id an erasure needs is
 * computed from who the person is, never looked up.
 */
class KnowledgePipelineTest {

    static final WorkflowRef DEFINITION = new WorkflowRef("meetings", "notes", "2.0.0");
    static final KnowledgeScope SCOPE = KnowledgeScope.of(DEFINITION);
    static final Pseudonyms PSEUDONYMS = new Pseudonyms("synthetic-secret-0123456789".getBytes(StandardCharsets.UTF_8));
    static final Pattern PSEUDONYM = Pattern.compile("PERSON_[0-9a-f]+");

    /** The model's answer for a window: the person named in it (a pseudonym) and the system, if any. */
    private static final ChatModel MODEL = new ChatModel() {
        @Override
        public ChatResponse doChat(ChatRequest request) {
            String text = request.messages().stream().filter(m -> m instanceof UserMessage)
                    .map(m -> ((UserMessage) m).singleText()).reduce("", (a, b) -> a + b);
            ArrayNode entities = Json.MAPPER.createArrayNode();
            Matcher person = PSEUDONYM.matcher(text);
            if (person.find()) {
                entities.addObject().put("ref", "p1").put("type", "Person").put("mention", person.group())
                        .set("passages", Json.MAPPER.createArrayNode().add(0));
            }
            if (text.contains("Checkout Service")) {
                entities.addObject().put("ref", "s1").put("type", "System").put("mention", "Checkout Service")
                        .set("passages", Json.MAPPER.createArrayNode().add(0));
            }
            ObjectNode answer = Json.obj();
            answer.set("entities", entities);
            ArrayNode relations = answer.putArray("relations");
            if (person.reset().find()) {
                relations.addObject().put("type", "participated").put("from", "p1").put("to", "meeting");
            }
            return ChatResponse.builder().aiMessage(AiMessage.from(answer.toString())).modelName("scripted").build();
        }
    };

    private static RedactionHandler redactor() {
        List<String> names = Corpus.personMarkers(Corpus.load());
        return new RedactionHandler(new LayeredRedactor("reference", "1", List.of(new PatternLayer(),
                new TermListLayer("person", names))), PSEUDONYMS).allowingStablePseudonyms();
    }

    private static TaskContext redactionTask(String instance, String text) {
        ObjectNode request = Json.obj();
        request.put(RedactionContract.INPUT, text);
        request.putArray(RedactionContract.TYPES);
        request.putObject(RedactionContract.PSEUDONYMS).put(RedactionContract.PSEUDONYMS_MODE, RedactionContract.MODE_SCOPE)
                .put(RedactionContract.PSEUDONYMS_SCOPE, SCOPE.key());
        return new TaskContext("/do/0/clean", instance, "c-" + instance, 1, request, DEFINITION, "chn-" + instance,
                Lineage.NONE);
    }

    private static String redacted(String instance, String text) {
        return redactor().execute(redactionTask(instance, text)).toCompletableFuture().join()
                .path(RedactionContract.OUTPUT).asText();
    }

    private static JsonNode extract(String meetingId, String instance, String redactedText) {
        ObjectNode in = Json.obj();
        in.putObject("meeting").put("meetingId", meetingId);
        ArrayNode turns = in.putArray("turns");
        for (String line : redactedText.split("\n")) {
            turns.addObject().put("speaker", "s").put("text", line);
        }
        in.put("schema", "meetings@1").put("glossary", "product@1");
        Glossary glossary = new Glossary("product", "1", List.of(new Glossary.Term("sys-checkout", "System",
                "Checkout Service", List.of("checkout"), null)));
        KnowledgeExtractionWorker worker = KnowledgeExtractionWorker.builder().chatModel(MODEL).provider("scripted", false)
                .glossaries(ref -> glossary).passageChars(400).build();
        TaskContext ctx = new TaskContext("/do/1/extract", instance, "c-x-" + instance, 1, in, DEFINITION, "chn-" + instance,
                Lineage.NONE, null, Sensitivity.CONFIDENTIAL, null);
        return worker.execute(ctx).toCompletableFuture().join();
    }

    @Test
    void ca10_ca11_a_redacted_meeting_gives_a_batch_without_personal_data_and_a_person_who_can_be_erased() throws Exception {
        Corpus.Meeting meeting = Corpus.load().stream().filter(m -> m.id().equals("m-001")).findFirst().orElseThrow();
        String raw = meeting.text();
        assertThat(raw).contains("SYNTH-PII-0101");   // the raw script holds the planted name

        String clean = redacted("i-1", raw);
        JsonNode result = extract("m-001", "i-1", clean);

        // nothing planted reaches the batch, the answer or the base
        List<String> markers = Corpus.allMarkers(Corpus.load());
        assertThat(result.toString()).doesNotContain(markers.toArray(String[]::new));
        KnowledgeBatch batch = KnowledgeBatchJson.fromJson(result.get("batch"));
        var person = batch.entities().stream().filter(e -> e.type().equals("Person")).findFirst().orElseThrow();
        assertThat(person.label()).matches("PERSON_[0-9a-f]{32}");   // the stable pseudonym, 128 bits
        assertThat(person.properties()).containsOnlyKeys("pseudonym");   // and nothing else about the person
        assertThat(person.sensitivity()).isEqualTo(Sensitivity.CONFIDENTIAL);

        CompositeKnowledgeStore store = new CompositeKnowledgeStore(new DeterministicEmbedder(),
                new FilesystemBlobStore(Files.createTempDirectory("stint-3b2-pipeline")), new RetentionClass("restricted",
                        Duration.ofDays(30)), InstantSource.system());
        store.write(SCOPE, batch).toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertThat(store.dump(SCOPE)).doesNotContain(markers.toArray(String[]::new)).contains(person.label());

        // the same person, in another meeting and another instance, is the same node
        String again = redacted("i-9", "Thanks everyone. " + raw);
        assertThat(PSEUDONYM.matcher(again).results().map(r -> r.group()).distinct().toList())
                .containsExactly(person.label());

        // an erasure computes the id from who the person is — the pseudonym, then the entity id — and finds nothing to look up
        String pseudonym = PSEUDONYMS.labelStable(SCOPE.key(), "person", "SYNTH-PII-0101");
        assertThat(pseudonym).isEqualTo(person.label());
        String entityId = EntityIds.of("Person", pseudonym);
        assertThat(entityId).isEqualTo(person.entityId());
        var receipt = store.delete(SCOPE, new DeleteRequest.ByPerson("req-1", "erasure:person", entityId, null))
                .toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertThat(receipt.counts()).containsKeys("ERASE.ENTITY", "ERASE.RELATION");
        assertThat(store.dump(SCOPE)).doesNotContain(person.label());
    }

    @Test
    void without_the_stable_mode_the_pseudonyms_are_those_of_the_instance_and_do_not_match_across_instances() {
        RedactionHandler plain = new RedactionHandler(new LayeredRedactor("reference", "1", List.of(new PatternLayer(),
                new TermListLayer("person", List.of("SYNTH-PII-0101")))), PSEUDONYMS);

        String one = plain.execute(redactionTask("i-1", "SYNTH-PII-0101 said hello")).toCompletableFuture().join()
                .path(RedactionContract.OUTPUT).asText();
        String two = plain.execute(redactionTask("i-2", "SYNTH-PII-0101 said hello")).toCompletableFuture().join()
                .path(RedactionContract.OUTPUT).asText();

        assertThat(one).matches("PERSON_[0-9a-f]{6} said hello");
        assertThat(two).isNotEqualTo(one);
    }
}
