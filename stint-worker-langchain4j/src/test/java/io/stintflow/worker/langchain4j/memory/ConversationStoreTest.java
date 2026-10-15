package io.stintflow.worker.langchain4j.memory;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import io.stintflow.spi.BlobStore;
import io.stintflow.wire.ClaimCheck;
import io.stintflow.wire.Json;

/**
 * SDD 3a.3, CA10 (RF5): the in-memory adapter and the blob offloading over it — order, one commit per correlationId,
 * retention, every scope of an instance, deletion with the blobs.
 */
class ConversationStoreTest {

    static final Instant T0 = Instant.parse("2026-10-12T12:00:00Z");

    private final AtomicReference<Instant> now = new AtomicReference<>(T0);
    private final InMemoryConversationStore store = new InMemoryConversationStore(now::get);

    static Exchange exchange(MemoryKey memory, String correlationId, Instant at, JsonNode messages) {
        return new Exchange(memory, correlationId, "/do/0/a", 1, at, messages, Json.obj().put("ok", true),
                at.plus(Duration.ofDays(30)));
    }

    static JsonNode messages(String text) {
        ArrayNode array = Json.MAPPER.createArrayNode();
        array.addObject().put("type", "USER").put("text", text);
        return array;
    }

    private <T> T join(CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }

    @Test
    void exchanges_come_in_commit_order_and_a_correlation_id_commits_once() {
        MemoryKey memory = MemoryKey.instance("i-A");
        assertThat(join(store.commit(exchange(memory, "c-2", T0.plusSeconds(2), messages("second")))))
                .isEqualTo(CommitOutcome.COMMITTED);
        join(store.commit(exchange(memory, "c-1", T0.plusSeconds(1), messages("first"))));

        assertThat(join(store.commit(exchange(memory, "c-1", T0.plusSeconds(3), messages("again")))))
                .isEqualTo(CommitOutcome.ALREADY_COMMITTED);
        assertThat(join(store.committed(memory))).extracting(Exchange::correlationId).containsExactly("c-1", "c-2");
    }

    @Test
    void expired_exchanges_are_not_read() {
        MemoryKey memory = MemoryKey.instance("i-A");
        join(store.commit(exchange(memory, "c-1", T0, messages("old"))));
        join(store.commit(exchange(memory, "c-2", T0.plus(Duration.ofDays(20)), messages("newer"))));

        now.set(T0.plus(Duration.ofDays(31)));

        assertThat(join(store.committed(memory))).extracting(Exchange::correlationId).containsExactly("c-2");
    }

    @Test
    void delete_instance_removes_every_scope_of_that_instance_only() {
        join(store.commit(exchange(MemoryKey.instance("i-A"), "c-1", T0, messages("a"))));
        join(store.commit(exchange(MemoryKey.task("i-A", "/do/1/b"), "c-2", T0, messages("b"))));
        join(store.commit(exchange(MemoryKey.instance("i-B"), "c-3", T0, messages("c"))));

        assertThat(join(store.exchangesOf("i-A"))).hasSize(2);
        join(store.deleteInstance("i-A"));
        join(store.deleteInstance("i-A"));   // idempotent

        assertThat(join(store.exchangesOf("i-A"))).isEmpty();
        assertThat(join(store.exchangesOf("i-B"))).hasSize(1);
    }

    @Test
    void a_big_exchange_goes_to_the_blob_store_and_comes_back_whole() {
        MapBlobs blobs = new MapBlobs();
        BlobOffloadingConversationStore offloading = new BlobOffloadingConversationStore(store, blobs, 1024);
        MemoryKey memory = MemoryKey.instance("i-A");
        JsonNode big = messages("x".repeat(4096));

        join(offloading.commit(exchange(memory, "c-1", T0, big)));
        join(offloading.commit(exchange(memory, "c-2", T0.plusSeconds(1), messages("small"))));

        assertThat(blobs.data).hasSize(1);
        assertThat(join(store.committed(memory)).getFirst().messages().has(ClaimCheck.POINTER_FIELD)).isTrue();
        assertThat(join(store.committed(memory)).getFirst().output()).isNull();
        List<Exchange> read = join(offloading.committed(memory));
        assertThat(read.getFirst().messages()).isEqualTo(big);
        assertThat(read.getFirst().output()).isEqualTo(Json.obj().put("ok", true));
        assertThat(read.get(1).messages()).isEqualTo(messages("small"));

        join(offloading.deleteInstance("i-A"));

        assertThat(blobs.data).isEmpty();
        assertThat(join(store.exchangesOf("i-A"))).isEmpty();
    }

    @Test
    void a_losing_commit_deletes_only_its_own_blob() {
        MapBlobs blobs = new MapBlobs();
        BlobOffloadingConversationStore offloading = new BlobOffloadingConversationStore(store, blobs, 1024);
        MemoryKey memory = MemoryKey.instance("i-A");

        assertThat(join(offloading.commit(exchange(memory, "c-1", T0, messages("x".repeat(4096))))))
                .isEqualTo(CommitOutcome.COMMITTED);
        assertThat(join(offloading.commit(exchange(memory, "c-1", T0, messages("y".repeat(4096))))))
                .isEqualTo(CommitOutcome.ALREADY_COMMITTED);

        assertThat(blobs.data).hasSize(1);
        assertThat(join(offloading.committed(memory)).getFirst().messages().toString()).contains("xxxx");
    }

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
}
