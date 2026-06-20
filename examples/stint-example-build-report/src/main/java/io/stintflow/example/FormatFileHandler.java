package io.stintflow.example;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.core.Json;
import io.stintflow.spi.BlobStore;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskHandler;

/**
 * Worker for {@link BuildReport#ROUTE_FORMAT}: reads the staged document via its pointer, "formats" it,
 * stages the final file and returns its pointer. The engine never sees the bytes — only references.
 */
public final class FormatFileHandler implements TaskHandler {

    private final BlobStore blob;

    public FormatFileHandler(BlobStore blob) {
        this.blob = blob;
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext ctx) {
        URI source = URI.create(ctx.input().get("source").asText());
        String format = ctx.input().get("format").asText();

        return blob.get(source).thenCompose(bytes -> {
            byte[] finalFile = ("FORMATTED[" + format + "](" + bytes.length + " bytes)")
                    .getBytes(StandardCharsets.UTF_8);
            return blob.put(finalFile, ctx.instanceId() + "/final." + format);
        }).thenApply(uri -> {
            ObjectNode out = Json.obj();
            out.put("file", uri.toString());
            return out;
        });
    }
}
