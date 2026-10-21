package io.stintflow.worker.langchain4j;

import static io.stintflow.worker.langchain4j.Fakes.answer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.knowledge.DeterministicEmbedder;
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
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;

/**
 * SDD 3b.3, 4.3 and 4.7 (CA9): the extraction reads its text by pointer and answers with the pointer to the batch; the embedder
 * over a scripted embedding model records its model and dimension, and a base refuses a language it was not meant for.
 */
class KnowledgeIngestionPartsTest {

    static final WorkflowRef DEFINITION = new WorkflowRef("examples", "knowledge-ingest", "1.0.0");
    static final RetentionClass BATCH = new RetentionClass("knowledge-batch", Duration.ofDays(7));
    static final String ANSWER = "{\"entities\":[{\"ref\":\"s1\",\"type\":\"System\",\"mention\":\"Checkout Service\","
            + "\"passages\":[0]}],\"relations\":[]}";
    static final String MINUTES = "# Minutes m-7\n\nThe Checkout Service needs a retry policy.\n\nAction: PERSON_a1b2c3 writes it.";

    FilesystemBlobStore blobs;

    @BeforeEach
    void setUp() throws Exception {
        blobs = new FilesystemBlobStore(Files.createTempDirectory("stint-3b3-parts"));
    }

    private KnowledgeExtractionWorker worker(boolean withBlobs) {
        KnowledgeExtractionWorker.Builder b = KnowledgeExtractionWorker.builder()
                .chatModel(new Fakes.ScriptedModel(r -> answer(ANSWER))).provider("scripted", false)
                .glossaries(ref -> new Glossary("product", "1", List.of()));
        return (withBlobs ? b.blobs(blobs, BATCH) : b).build();
    }

    private ObjectNode input(String textRef) {
        ObjectNode in = Json.obj();
        in.putObject("meeting").put("meetingId", "m-7");
        in.put("textRef", textRef).put("schema", "meetings@1").put("glossary", "product@1").put("revision", 1_700_000_000_000L);
        in.putObject("scope").put("namespace", "examples").put("base", "product-meetings");
        return in;
    }

    private static TaskContext task(JsonNode in) {
        return new TaskContext("/do/1/extract", "i-7", "c-7", 1, in, DEFINITION, "chn-7", Lineage.NONE, null,
                Sensitivity.CONFIDENTIAL, null);
    }

    private String minutesBlob() {
        return blobs.put(MINUTES.getBytes(StandardCharsets.UTF_8), "minutes/m-7.md", new RetentionClass("minutes",
                Duration.ofDays(90))).toCompletableFuture().join().toString();
    }

    @Test
    void the_text_is_read_by_pointer_and_the_batch_comes_back_as_a_pointer_with_counts() {
        JsonNode result = worker(true).execute(task(input(minutesBlob()))).toCompletableFuture().join();

        assertThat(result.has("batch")).isFalse();   // the batch itself does not travel
        assertThat(result.get("counts").get("passages").asInt()).isEqualTo(1);
        assertThat(result.get("counts").get("entities").asInt()).isEqualTo(2);   // the system and the meeting
        assertThat(result.get("schemaVersion").asText()).isEqualTo("meetings@1.0.0");

        String ref = result.get("batchRef").asText();
        assertThat(ref).contains("knowledge-batch");   // the key sits under the class prefix
        KnowledgeBatch batch = KnowledgeBatchJson.fromJson(Json.read(blobs.get(URI.create(ref)).toCompletableFuture().join()));
        assertThat(batch.documentId()).isEqualTo("meeting:m-7");
        assertThat(batch.revision()).isEqualTo(1_700_000_000_000L);
        assertThat(batch.passages()).extracting(p -> p.text()).singleElement().asString()
                .contains("Checkout Service needs a retry policy").contains("PERSON_a1b2c3");
        assertThat(batch.source().instanceId()).isEqualTo("i-7");
        assertThat(batch.batchId()).isEqualTo("extract:i-7:/do/1/extract");
    }

    @Test
    void the_same_task_run_again_gives_the_same_batch() {
        String minutes = minutesBlob();
        JsonNode first = worker(true).execute(task(input(minutes))).toCompletableFuture().join();
        JsonNode second = worker(true).execute(task(input(minutes))).toCompletableFuture().join();
        KnowledgeBatch a = KnowledgeBatchJson.fromJson(Json.read(blobs.get(URI.create(first.get("batchRef").asText()))
                .toCompletableFuture().join()));
        KnowledgeBatch b = KnowledgeBatchJson.fromJson(Json.read(blobs.get(URI.create(second.get("batchRef").asText()))
                .toCompletableFuture().join()));
        assertThat(b).isEqualTo(a);   // same passage ids, same entity ids, same hash: the write is idempotent
    }

    @Test
    void without_a_blob_store_a_pointer_is_refused_and_the_batch_stays_inline_for_local_use() {
        TaskFailure refused = failure(worker(false), task(input(minutesBlob())));
        assertThat(refused.getMessage()).contains("no blob store");

        ObjectNode inline = Json.obj();
        inline.putObject("meeting").put("meetingId", "m-7");
        inline.putArray("passages").add("The Checkout Service needs a retry policy.");
        inline.put("schema", "meetings@1");
        JsonNode result = worker(false).execute(task(inline)).toCompletableFuture().join();
        assertThat(result.has("batch")).isTrue();
        assertThat(result.has("batchRef")).isFalse();
    }

    @Test
    void a_text_that_cannot_be_read_is_a_communication_failure() {
        TaskFailure unreachable = failure(worker(true), task(input("file:///no/such/minutes.md")));
        assertThat(unreachable.toErrorInfo().status()).isEqualTo(503);
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

    // --- the embedder (CA9) -------------------------------------------------------------------------------------

    /** An embedding model with three dimensions that counts its calls. */
    static final class ScriptedEmbeddings implements EmbeddingModel {
        int calls;
        int dimension = 3;

        @Override
        public Response<List<Embedding>> embedAll(List<TextSegment> segments) {
            calls += segments.size();
            return Response.from(segments.stream().map(s -> {
                float[] v = new float[dimension];
                v[0] = s.text().length();
                v[1] = 1;
                return Embedding.from(v);
            }).toList());
        }
    }

    @Test
    void the_embedder_records_the_model_the_dimension_and_the_languages() {
        ScriptedEmbeddings model = new ScriptedEmbeddings();
        LangChain4jEmbedder embedder = new LangChain4jEmbedder(model, "scripted-embed-1", Set.of("en", "pt"));

        assertThat(embedder.model()).isEqualTo("scripted-embed-1");
        assertThat(embedder.languages()).containsExactlyInAnyOrder("en", "pt");
        float[] vector = embedder.embed("hello");
        assertThat(vector).hasSize(3);
        assertThat(vector[0]).isEqualTo(5f);
        assertThat(embedder.dimension()).isEqualTo(3);
    }

    @Test
    void a_model_that_changes_its_dimension_is_refused_instead_of_mixing_vectors() {
        ScriptedEmbeddings model = new ScriptedEmbeddings();
        LangChain4jEmbedder embedder = new LangChain4jEmbedder(model, "scripted-embed-1", Set.of("en"));
        embedder.embed("first");
        model.dimension = 4;
        assertThatThrownBy(() -> embedder.embed("second")).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("reindex");
    }

    @Test
    void a_base_with_that_embedder_refuses_a_language_it_does_not_embed() throws Exception {
        LangChain4jEmbedder embedder = new LangChain4jEmbedder(new ScriptedEmbeddings(), "scripted-embed-1", Set.of("en"));
        CompositeKnowledgeStore store = new CompositeKnowledgeStore(embedder, blobs, new RetentionClass("restricted",
                Duration.ofDays(30)), java.time.InstantSource.system());
        KnowledgeScope scope = KnowledgeScope.named("examples", "product-meetings");
        var passage = new io.stintflow.knowledge.Passage(io.stintflow.knowledge.PassageIds.of(scope, "meeting:m-8", 0, "Olá"), 0,
                "Olá, equipe.", "pt", Sensitivity.CONFIDENTIAL);
        var batch = new KnowledgeBatch("b-1", "meeting:m-8", "h", null, null, "meetings@1.0.0", List.of(passage), List.of(),
                List.of(), List.of());
        assertThatThrownBy(() -> store.write(scope, batch).toCompletableFuture().join()).hasCauseInstanceOf(
                io.stintflow.knowledge.KnowledgeValidationException.class);
    }

    @Test
    void an_embedder_needs_a_name_and_a_language() {
        assertThatThrownBy(() -> new LangChain4jEmbedder(new ScriptedEmbeddings(), " ", Set.of("en")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new LangChain4jEmbedder(new ScriptedEmbeddings(), "m", Set.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(new DeterministicEmbedder().dimension()).isPositive();   // the one for tests stays
    }
}
