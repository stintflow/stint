package io.stintflow.example.meeting;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskHandler;

/**
 * The task {@code publish-adr} (SDD 3c.4, 4.2): publishes the ADRs {@code write-adrs} wrote, one page each, to the one space the
 * {@code publish-page} handler it wraps is allowed — the architecture space, a handler of its own and not the one of the product
 * minutes. Input {@code {"space", "adrs": [{"decisionId", "title", "status", "documentRef"}]}} (at most {@value #MAX_PAGES}); output
 * {@code {"pages": [{"decisionId", "pageId", "status"}]}}: ids and the status, no text.
 */
final class PublishAdrsHandler implements TaskHandler {

    static final int MAX_PAGES = 20;

    private final TaskHandler page;

    PublishAdrsHandler(TaskHandler page) {
        this.page = Objects.requireNonNull(page, "page");
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext ctx) {
        JsonNode adrs = ctx.input().path("adrs");
        if (!adrs.isArray() || adrs.isEmpty() || adrs.size() > MAX_PAGES) {
            return CompletableFuture.failedFuture(new io.stintflow.worker.TaskFailure(
                    io.stintflow.spi.ErrorInfo.StandardType.VALIDATION.uri(), 400, "adrs is one to " + MAX_PAGES + " ADRs", null));
        }
        List<CompletableFuture<JsonNode>> published = new ArrayList<>();
        for (JsonNode adr : adrs) {
            ObjectNode input = Json.obj();
            input.put("space", ctx.input().path("space").asText(""));
            input.put("key", "adr-" + adr.path("decisionId").asText(""));
            input.put("title", "ADR " + adr.path("decisionId").asText("") + ": " + adr.path("title").asText(""));
            input.set("documentRef", adr.path("documentRef"));
            input.putArray("labels").add("adr").add("adr-" + adr.path("status").asText(""));
            TaskContext one = new TaskContext(ctx.taskId(), ctx.instanceId(), ctx.correlationId(), ctx.attempt(), input,
                    ctx.definition(), ctx.chainId(), ctx.lineage(), ctx.deadline(), ctx.sensitivity(), ctx.report());
            published.add(page.execute(one).toCompletableFuture().thenApply(result -> (JsonNode) Json.obj()
                    .put("decisionId", adr.path("decisionId").asText()).put("pageId", result.path("id").asText())
                    .put("status", adr.path("status").asText())));
        }
        return CompletableFuture.allOf(published.toArray(CompletableFuture[]::new)).thenApply(v -> {
            ObjectNode out = Json.obj();
            ArrayNode pages = out.putArray("pages");
            published.forEach(p -> pages.add(p.join()));
            return out;
        });
    }
}
