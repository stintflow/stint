package io.stintflow.example.meeting;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.spi.BlobStore;
import io.stintflow.spi.RetentionClass;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskHandler;
import io.stintflow.worker.langchain4j.LangChain4jWorker;

/**
 * The task behind {@code structure-minutes} (SDD 3a.6, 4.4): the {@link MinutesWriter} over the redacted transcript,
 * then the minutes — markdown — into a blob of the {@code minutes} class. Its output: the pointer, the action items'
 * ids and the document type; never the minutes themselves.
 */
public final class StructureHandler implements TaskHandler {

    public static final RetentionClass MINUTES = new RetentionClass("minutes", Duration.ofDays(90));

    private final TaskHandler minutes;
    private final BlobStore blobs;

    public StructureHandler(LangChain4jWorker worker, BlobStore blobs) {
        this.minutes = worker.handler(MinutesWriter.class, (ai, task) -> ai.structure(String.format(
                MinutesWriter.PROMPT, task.input().path("docType").asText(), task.input().path("text").asText())));
        this.blobs = Objects.requireNonNull(blobs, "blobs");
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext ctx) {
        String meetingId = ctx.input().path("meetingId").asText();
        String docType = ctx.input().path("docType").asText();
        return minutes.execute(ctx).thenCompose(structured -> {
            MinutesWriter.Minutes parsed = Json.MAPPER.convertValue(structured, MinutesWriter.Minutes.class);
            String markdown = parsed.markdown("Minutes " + meetingId + " (" + docType + ")");
            return blobs.put(markdown.getBytes(StandardCharsets.UTF_8), "minutes/" + meetingId + "-"
                    + ctx.correlationId() + ".md", MINUTES).thenApply(uri -> {
                        ObjectNode out = Json.obj();
                        out.put("minutesRef", uri.toString());
                        ArrayNode ids = out.putArray("itemIds");
                        if (parsed.items() != null) {
                            parsed.items().forEach(i -> ids.add(i.id()));
                        }
                        out.put("docType", docType);
                        return (JsonNode) out;
                    });
        });
    }
}
