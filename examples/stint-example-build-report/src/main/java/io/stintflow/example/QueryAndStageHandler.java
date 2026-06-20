package io.stintflow.example;

import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.core.Json;
import io.stintflow.spi.BlobStore;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskHandler;

/**
 * Worker for {@link BuildReport#ROUTE_QUERY}: simulates a DB query, stages the result document in the
 * blob store (the deliberate domain-level claim-check) and returns a pointer — never the rows inline.
 */
public final class QueryAndStageHandler implements TaskHandler {

    private final BlobStore blob;

    public QueryAndStageHandler(BlobStore blob) {
        this.blob = blob;
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext ctx) {
        // Simulate a query result document.
        ObjectNode doc = Json.obj();
        doc.put("query", ctx.input().get("query").asText());
        ArrayNode rows = doc.putArray("rows");
        for (int i = 0; i < 12_000; i++) {
            rows.add("row-" + i);
        }

        String key = ctx.instanceId() + "/extract.json";
        return blob.put(Json.bytes(doc), key).thenApply(uri -> {
            ObjectNode out = Json.obj();
            out.put("pointer", uri.toString());
            out.put("rows", rows.size());
            return out;
        });
    }
}
