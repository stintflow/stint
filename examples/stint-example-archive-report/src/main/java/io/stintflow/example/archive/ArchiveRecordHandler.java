package io.stintflow.example.archive;

import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.spi.BlobStore;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskHandler;

/**
 * Worker for {@link ArchiveReport#ROUTE_ARCHIVE_RECORD} (SDD 2.6, RF2/RF3): stores one archive record per
 * fact in the {@link BlobStore} (S3 on AWS) under {@code archive/<fact id>.json}. The record carries the
 * fact's id and data plus the chain this execution belongs to, which makes the end-to-end chain visible
 * in the archived artifact itself. Writing the same key again is harmless, so a redelivery is idempotent.
 */
public final class ArchiveRecordHandler implements TaskHandler {

    private final BlobStore blob;

    public ArchiveRecordHandler(BlobStore blob) {
        this.blob = blob;
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext ctx) {
        JsonNode fact = ctx.input().get("fact");
        String factId = fact.get("id").asText();
        ObjectNode record = Json.obj();
        record.put("factId", factId);
        record.put("factType", fact.path("type").asText());
        record.put("chainId", ctx.chainId());
        record.set("report", fact.path("data"));
        String key = "archive/" + factId + ".json";
        return blob.put(Json.bytes(record), key).thenApply(uri -> {
            ObjectNode out = Json.obj();
            out.put("archivedAt", uri.toString());
            out.put("factId", factId);
            out.put("chainId", ctx.chainId());
            return out;
        });
    }
}
