package io.stintflow.worker.langchain4j;

import static io.stintflow.worker.langchain4j.Fakes.failure;
import static io.stintflow.worker.langchain4j.Fakes.run;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.jupiter.api.Test;

import io.stintflow.spi.BlobStore;
import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.RetentionClass;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.ClaimCheck;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskHandler;
import io.stintflow.worker.TaskReport;
import io.stintflow.worker.langchain4j.Fakes.Chat;
import io.stintflow.worker.langchain4j.Fakes.ScriptedModel;
import io.stintflow.worker.langchain4j.memory.Exchange;
import io.stintflow.worker.langchain4j.memory.InMemoryConversationStore;
import io.stintflow.worker.langchain4j.memory.MemoryKey;

/**
 * SDD 3a.5, RF6 (E1, memory): a restricted instance's conversation never rests in the store — every exchange goes to
 * the blob store by pointer, under the {@code restricted} class, and expires no later than it; the next step reads it
 * back. Without a blob store, a restricted step with memory fails closed before the model is called. Other levels keep
 * the 3a.3 behavior. Fake model, synthetic data.
 */
class RestrictedMemoryTest {

    static final String MARKER = "SYNTH-PII-0201";

    /** Blobs in a map, by key. */
    static final class MapBlobs implements BlobStore {
        final Map<URI, byte[]> data = new ConcurrentHashMap<>();

        @Override
        public CompletionStage<URI> put(byte[] bytes, String key) {
            URI uri = URI.create("mem://" + key);
            data.put(uri, bytes);
            return CompletableFuture.completedFuture(uri);
        }

        @Override
        public CompletionStage<byte[]> get(URI ref) {
            return CompletableFuture.completedFuture(data.get(ref));
        }

        @Override
        public CompletionStage<Void> delete(URI ref) {
            data.remove(ref);
            return CompletableFuture.completedFuture(null);
        }
    }

    private final InMemoryConversationStore store = new InMemoryConversationStore();
    private final MapBlobs blobs = new MapBlobs();
    private final Fakes.Clock clock = new Fakes.Clock();

    private static TaskContext task(String correlationId, Sensitivity level) {
        return new TaskContext("/do/0/ask", "i-R", correlationId, 1, Json.obj().put("text", MARKER),
                new WorkflowRef("test", "ai", "1.0.0"), "chn-i-R", Lineage.NONE, null, level, new TaskReport());
    }

    private static TaskHandler step(LangChain4jWorker worker) {
        return worker.handler(Chat.class, (ai, t) -> ai.reply(t.input().path("text").asText()));
    }

    private List<Exchange> stored() {
        return store.exchangesOf("i-R").toCompletableFuture().join();
    }

    @Test
    void a_restricted_exchange_rests_by_pointer_under_its_class_and_is_read_back() {
        ScriptedModel model = ScriptedModel.echoing();
        LangChain4jWorker worker = LangChain4jWorker.builder().chatModel(model).conversations(store, blobs)
                .restrictedRetention(new RetentionClass("restricted", Duration.ofDays(2))).clock(clock).build();

        run(step(worker), task("c-1", Sensitivity.RESTRICTED));
        run(step(worker), task("c-2", Sensitivity.RESTRICTED));

        assertThat(stored()).hasSize(2).allSatisfy(exchange -> {
            assertThat(exchange.memory()).isEqualTo(MemoryKey.instance("i-R").restricted());
            assertThat(ClaimCheck.isPointer(exchange.messages())).isTrue();
            assertThat(exchange.toString()).doesNotContain(MARKER);
            assertThat(exchange.expiresAt()).isEqualTo(exchange.committedAt().plus(Duration.ofDays(2)));
        });
        assertThat(blobs.data.keySet()).allSatisfy(uri -> assertThat(uri.toString())
                .startsWith("mem://retention/restricted/conversations/i-R/"));
        assertThat(model.lastMessages().toString()).contains(MARKER);   // the second step read the first back
    }

    @Test
    void a_step_after_a_redaction_lowered_the_instance_never_reads_what_was_said_while_restricted() {
        ScriptedModel model = ScriptedModel.echoing();
        LangChain4jWorker worker = LangChain4jWorker.builder().chatModel(model).conversations(store, blobs)
                .clock(clock).build();

        run(step(worker), task("c-1", Sensitivity.CONFIDENTIAL));   // said before it was restricted
        run(step(worker), task("c-2", Sensitivity.RESTRICTED));     // reads both parts
        assertThat(model.lastMessages()).hasSize(3);   // c-1's question and answer, and its own question
        run(step(worker), new TaskContext("/do/2/ask", "i-R", "c-3", 1, Json.obj().put("text", "redacted"),
                new WorkflowRef("test", "ai", "1.0.0"), "chn-i-R", Lineage.NONE, null, Sensitivity.CONFIDENTIAL,
                new TaskReport()));

        assertThat(model.lastMessages()).hasSize(3);   // c-1's question and answer, and its own question
        assertThat(model.requests.get(2).messages().toString()).contains("1:SYNTH-PII-0201")
                .doesNotContain("3:SYNTH-PII-0201");   // never c-2's answer, said while restricted
    }

    @Test
    void a_confidential_exchange_keeps_the_3a3_behavior() {
        LangChain4jWorker worker = LangChain4jWorker.builder().chatModel(ScriptedModel.echoing())
                .conversations(store, blobs).clock(clock).build();

        run(step(worker), task("c-1", Sensitivity.CONFIDENTIAL));

        assertThat(stored()).singleElement().satisfies(exchange -> {
            assertThat(ClaimCheck.isPointer(exchange.messages())).isFalse();
            assertThat(exchange.expiresAt()).isEqualTo(exchange.committedAt().plus(Duration.ofDays(30)));
        });
        assertThat(blobs.data).isEmpty();
    }

    @Test
    void without_a_blob_store_a_restricted_step_with_memory_fails_closed_before_the_model() {
        ScriptedModel model = ScriptedModel.echoing();
        LangChain4jWorker worker = LangChain4jWorker.builder().chatModel(model).conversations(store).clock(clock)
                .build();

        ErrorInfo error = failure(step(worker), task("c-1", Sensitivity.RESTRICTED)).toErrorInfo();

        assertThat(error.type()).isEqualTo(ErrorInfo.StandardType.CONFIGURATION.uri());
        assertThat(error.status()).isEqualTo(500);
        assertThat(error.detail()).contains("needs a blob store").doesNotContain(MARKER);
        assertThat(model.requests).isEmpty();
        assertThat(stored()).isEmpty();
    }
}
