package io.stintflow.worker.publishing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.spi.BlobNotFoundException;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskReport;

/**
 * SDD 3a.6, 4.5 (CA3): a page for another space is refused before the document is read or any request is made; the
 * Confluence adapter publishes in storage format with the key and level labels and, published again with the same
 * key, returns the same page; its errors map to standard ones. Fake server, fake credentials, synthetic documents.
 */
class PublishingTest {

    static final String EMAIL = "stint-test@example.com";
    static final String TOKEN = "not-a-real-token";
    static final String MINUTES = """
            # Minutes m-001 (ata)
            ## Summary
            The team reviewed the <checkout> & pricing.
            ## Items
            - I will update the checkout copy by Friday.
            - I will schedule the usability test.
            """;

    static final class MapBlobs implements BlobStore {
        final Map<URI, byte[]> data = new ConcurrentHashMap<>();
        int reads;

        @Override
        public CompletionStage<URI> put(byte[] bytes, String key) {
            URI uri = URI.create("mem://" + key);
            data.put(uri, bytes);
            return CompletableFuture.completedFuture(uri);
        }

        @Override
        public CompletionStage<byte[]> get(URI ref) {
            reads++;
            byte[] bytes = data.get(ref);
            return bytes == null ? CompletableFuture.failedFuture(new BlobNotFoundException(ref, "absent"))
                    : CompletableFuture.completedFuture(bytes);
        }

        @Override
        public CompletionStage<Void> delete(URI ref) {
            data.remove(ref);
            return CompletableFuture.completedFuture(null);
        }
    }

    private static TaskContext task(JsonNode input) {
        return new TaskContext("/do/7/publish", "i-1", "c-1", 1, input, new WorkflowRef("examples", "pm", "1.0.0"),
                "chn-1", Lineage.NONE, null, Sensitivity.CONFIDENTIAL, new TaskReport());
    }

    private static JsonNode request(String space, URI documentRef) {
        return Json.obj().put("space", space).put("key", "m-001").put("title", "Minutes m-001 (ata)")
                .put("documentRef", documentRef.toString())
                .set("labels", Json.MAPPER.createArrayNode().add("doc-ata"));
    }

    @Test
    void ca3_another_space_is_refused_before_any_read_or_request() throws Exception {
        MapBlobs blobs = new MapBlobs();
        URI doc = blobs.put(MINUTES.getBytes(StandardCharsets.UTF_8), "minutes/m-001.md").toCompletableFuture().join();
        try (FakeConfluenceServer confluence = new FakeConfluenceServer(EMAIL, TOKEN, "PRODUCT", "HR")) {
            ConfluencePagePublisher adapter = ConfluencePagePublisher.builder().baseUri(confluence.baseUri())
                    .space("PRODUCT").email(EMAIL).apiToken(TOKEN).build();
            PublishHandler handler = new PublishHandler(adapter, blobs, "PRODUCT");

            assertThatThrownBy(() -> handler.execute(task(request("HR", doc))).toCompletableFuture().join())
                    .isInstanceOf(CompletionException.class)
                    .satisfies(e -> {
                        ErrorInfo error = ((TaskFailure) e.getCause()).toErrorInfo();
                        assertThat(error.type()).isEqualTo(ErrorInfo.StandardType.AUTHORIZATION.uri());
                        assertThat(error.status()).isEqualTo(403);
                        assertThat(error.detail()).isEqualTo("Publishing to space 'HR' is not allowed: only 'PRODUCT' is");
                    });
            assertThat(confluence.requests()).isEmpty();
            assertThat(blobs.reads).isZero();
            // the adapter, alone, is the second barrier
            assertThatThrownBy(() -> adapter.publish(new PagePublisher.Page("HR", "k", "t", "<p/>", List.of(), null)))
                    .isInstanceOf(TaskFailure.class);
            assertThat(confluence.requests()).isEmpty();
        }
    }

    @Test
    void the_adapter_publishes_in_storage_format_with_labels_and_is_idempotent_by_key() throws Exception {
        MapBlobs blobs = new MapBlobs();
        URI doc = blobs.put(MINUTES.getBytes(StandardCharsets.UTF_8), "minutes/m-001.md").toCompletableFuture().join();
        try (FakeConfluenceServer confluence = new FakeConfluenceServer(EMAIL, TOKEN, "PRODUCT")) {
            PublishHandler handler = new PublishHandler(ConfluencePagePublisher.builder().baseUri(confluence.baseUri())
                    .space("PRODUCT").email(EMAIL).apiToken(TOKEN).build(), blobs, "PRODUCT");

            JsonNode first = handler.execute(task(request("PRODUCT", doc))).toCompletableFuture().join();
            JsonNode again = handler.execute(task(request("PRODUCT", doc))).toCompletableFuture().join();

            assertThat(first.path("created").asBoolean()).isTrue();
            assertThat(again.path("created").asBoolean()).isFalse();
            assertThat(again.path("id").asText()).isEqualTo(first.path("id").asText());
            assertThat(first.path("url").asText()).startsWith(confluence.baseUri() + "/wiki/spaces/PRODUCT/pages/");
            assertThat(confluence.pages()).singleElement().satisfies(page -> {
                assertThat(page.title()).isEqualTo("Minutes m-001 (ata)");
                assertThat(page.body()).startsWith("<h1>Minutes m-001 (ata)</h1><h2>Summary</h2>")
                        .contains("&lt;checkout&gt; &amp; pricing", "<ul><li>I will update the checkout copy");
                assertThat(page.labels()).containsExactlyInAnyOrder("doc-ata", "stint-key-m-001",
                        "stint-confidential");
            });
        }
    }

    @Test
    void errors_map_to_standard_ones_and_the_in_memory_publisher_keeps_one_page_per_key() throws Exception {
        try (FakeConfluenceServer confluence = new FakeConfluenceServer(EMAIL, TOKEN, "PRODUCT")) {
            ConfluencePagePublisher wrongToken = ConfluencePagePublisher.builder().baseUri(confluence.baseUri())
                    .space("PRODUCT").email(EMAIL).apiToken("wrong").build();
            assertThatThrownBy(() -> wrongToken.publish(new PagePublisher.Page("PRODUCT", "k", "t", "<p/>", List.of(),
                    Sensitivity.INTERNAL))).satisfies(e ->
                    assertThat(((TaskFailure) e).toErrorInfo().type()).isEqualTo(ErrorInfo.StandardType.AUTHENTICATION.uri()));
            ConfluencePagePublisher noSpace = ConfluencePagePublisher.builder().baseUri(confluence.baseUri())
                    .space("NOPE").email(EMAIL).apiToken(TOKEN).build();
            assertThatThrownBy(() -> noSpace.publish(new PagePublisher.Page("NOPE", "k", "t", "<p/>", List.of(),
                    null))).satisfies(e -> assertThat(((TaskFailure) e).toErrorInfo().status()).isEqualTo(404));
        }
        InMemoryPagePublisher memory = new InMemoryPagePublisher();
        PagePublisher.PublishedPage a = memory.publish(new PagePublisher.Page("P", "m-1", "t", "b", List.of(), null));
        PagePublisher.PublishedPage b = memory.publish(new PagePublisher.Page("P", "m-1", "t", "b", List.of(), null));
        assertThat(b.id()).isEqualTo(a.id());
        assertThat(b.created()).isFalse();
        assertThat(memory.pages()).hasSize(1);
    }
}
