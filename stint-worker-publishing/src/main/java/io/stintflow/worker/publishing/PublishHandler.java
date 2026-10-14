package io.stintflow.worker.publishing;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.spi.BlobNotFoundException;
import io.stintflow.spi.BlobStore;
import io.stintflow.wire.ClaimCheck;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;

/**
 * The task behind {@code publish-page} (SDD 3a.6, 4.5). Its input is {@code { space, key, title, documentRef, labels? }};
 * the document — markdown, already redacted — by pointer. A space other than the one allowed is refused <b>before</b>
 * the document is read or any call is made ({@code authorization} 403). The page carries the instance's level as a
 * label. Its output: {@code { id, url, version, created }}. Logs carry ids only.
 */
public final class PublishHandler implements TaskHandler {

    private static final Logger LOG = LoggerFactory.getLogger(PublishHandler.class);

    private final PagePublisher publisher;
    private final BlobStore blobs;
    private final String allowedSpace;

    /** @param allowedSpace the one space this worker may publish to ({@code stint.publishing.space}) */
    public PublishHandler(PagePublisher publisher, BlobStore blobs, String allowedSpace) {
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.blobs = Objects.requireNonNull(blobs, "blobs");
        if (allowedSpace == null || allowedSpace.isBlank()) {
            throw new IllegalArgumentException("One allowed space is required");
        }
        this.allowedSpace = allowedSpace;
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext ctx) {
        JsonNode input = ctx.input();
        String space = input.path("space").asText("");
        if (!allowedSpace.equals(space)) {
            LOG.warn("Task {} of instance {}: publishing to space '{}' refused, only '{}' is allowed", ctx.taskId(),
                    ctx.instanceId(), space, allowedSpace);
            return CompletableFuture.failedFuture(PublishFailures.spaceNotAllowed(space, allowedSpace));
        }
        String key = input.path("key").asText("");
        String title = input.path("title").asText("");
        URI documentRef;
        try {
            if (key.isBlank() || title.isBlank()) {
                throw PublishFailures.validation("key and title are required");
            }
            documentRef = pointer(input.get("documentRef"));
        } catch (TaskFailure failure) {
            return CompletableFuture.failedFuture(failure);
        }
        List<String> labels = new ArrayList<>();
        input.path("labels").forEach(l -> labels.add(l.asText()));
        return blobs.get(documentRef).handle((bytes, failure) -> {
            if (failure != null) {
                throw new CompletionException(BlobNotFoundException.in(failure)
                        ? PublishFailures.validation("The document is not in the blob store")
                        : PublishFailures.unavailable(503, "The document could not be read from the blob store",
                                null));
            }
            return bytes;
        }).thenApply(bytes -> {
            PagePublisher.PublishedPage page = publisher.publish(new PagePublisher.Page(space, key, title,
                    StorageFormat.fromMarkdown(new String(bytes, StandardCharsets.UTF_8)), labels, ctx.sensitivity()));
            LOG.info("Task {} of instance {}: page {} in space {} ({})", ctx.taskId(), ctx.instanceId(), page.id(),
                    space, page.created() ? "created" : "already published");
            ObjectNode out = Json.obj();
            out.put("id", page.id());
            out.put("url", page.url().toString());
            out.put("version", page.version());
            out.put("created", page.created());
            return out;
        });
    }

    private static URI pointer(JsonNode node) {
        String text = node == null ? null : ClaimCheck.isPointer(node) ? node.get(ClaimCheck.POINTER_FIELD).asText()
                : node.isTextual() ? node.asText() : null;
        try {
            URI uri = text == null ? null : URI.create(text);
            if (uri == null || uri.getScheme() == null) {
                throw PublishFailures.validation("documentRef must be a pointer to the document (a URI)");
            }
            return uri;
        } catch (IllegalArgumentException e) {
            throw PublishFailures.validation("documentRef must be a pointer to the document (a URI)");
        }
    }
}
