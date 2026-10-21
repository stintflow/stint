package io.stintflow.example.meeting;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import dev.langchain4j.rag.content.Content;
import dev.langchain4j.rag.content.retriever.ContentRetriever;
import dev.langchain4j.rag.query.Query;
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
 * <p>
 * SDD 3b.4, 4.6: with a retriever, the model is also given what the base holds from earlier meetings, each item with the document
 * it came from, and the minutes end with a {@code ## Context used} section — the documents that were handed over, written by this
 * code and not by the model, so the citation does not depend on the model remembering to make it. Without a retriever, or when it
 * finds nothing, the prompt and the minutes are what they were before.
 */
public final class StructureHandler implements TaskHandler {

    public static final RetentionClass MINUTES = new RetentionClass("minutes", Duration.ofDays(90));

    /** How much of the transcript is the question put to the base. */
    static final int QUERY_CHARS = 1500;
    private static final String CONTEXT_FIELD = "earlierContext";

    private final TaskHandler minutes;
    private final BlobStore blobs;
    private final Function<TaskContext, ContentRetriever> retrievers;

    public StructureHandler(LangChain4jWorker worker, BlobStore blobs) {
        this(worker, blobs, null);
    }

    /** @param retrievers one retriever for the task (its ceiling is the task's), or {@code null} for none, or one that gives none */
    public StructureHandler(LangChain4jWorker worker, BlobStore blobs, Function<TaskContext, ContentRetriever> retrievers) {
        this.minutes = worker.handler(MinutesWriter.class, (ai, task) -> ai.structure(prompt(task.input())));
        this.blobs = Objects.requireNonNull(blobs, "blobs");
        this.retrievers = retrievers;
    }

    /** The prompt of before, with the earlier context — when there is any — between the document type and the transcript. */
    private static String prompt(JsonNode input) {
        String template = MinutesWriter.PROMPT;
        String context = input.path(CONTEXT_FIELD).asText("");
        if (!context.isEmpty()) {
            template = template.replace("Transcript:\n%s", context + "Transcript:\n%s");
        }
        return String.format(template, input.path("docType").asText(), input.path("text").asText());
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext task) {
        String meetingId = task.input().path("meetingId").asText();
        String docType = task.input().path("docType").asText();
        List<Content> earlier = earlier(task);
        Set<String> used = documentsOf(earlier);
        TaskContext ctx = earlier.isEmpty() ? task : withContext(task, earlier);
        return minutes.execute(ctx).thenCompose(structured -> {
            MinutesWriter.Minutes parsed = Json.MAPPER.convertValue(structured, MinutesWriter.Minutes.class);
            String markdown = parsed.markdown("Minutes " + meetingId + " (" + docType + ")");
            if (!used.isEmpty()) {
                StringBuilder section = new StringBuilder(markdown).append("## Context used\n");
                used.forEach(document -> section.append("- ").append(document).append('\n'));
                markdown = section.toString();
            }
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

    /** What the base holds that bears on this meeting, or nothing: the minutes go out without context rather than not at all. */
    private List<Content> earlier(TaskContext task) {
        if (retrievers == null) {
            return List.of();
        }
        String text = task.input().path("text").asText("");
        if (text.isBlank()) {
            return List.of();
        }
        ContentRetriever retriever = retrievers.apply(task);
        return retriever == null ? List.of()
                : retriever.retrieve(Query.from(text.substring(0, Math.min(text.length(), QUERY_CHARS))));
    }

    /** The documents the items came from, in the order of the ranking, each once. */
    private static Set<String> documentsOf(List<Content> contents) {
        Set<String> documents = new LinkedHashSet<>();
        for (Content content : contents) {
            documents.addAll(sources(content));
        }
        return documents;
    }

    private static List<String> sources(Content content) {
        var metadata = content.textSegment().metadata();
        String document = metadata.getString("documentId");
        if (document != null) {
            return List.of(document);
        }
        String evidence = metadata.getString("evidence");
        return evidence == null || evidence.isEmpty() ? List.of() : List.of(evidence.split(","));
    }

    private static TaskContext withContext(TaskContext task, List<Content> earlier) {
        StringBuilder block = new StringBuilder("Earlier context from the knowledge base (cite as [source: <document>]):\n");
        for (Content content : earlier) {
            block.append("- [source: ").append(String.join(", ", sources(content))).append("] ")
                    .append(content.textSegment().text().strip().replace('\n', ' ')).append('\n');
        }
        ObjectNode input = task.input().deepCopy();
        input.put(CONTEXT_FIELD, block.toString());
        return new TaskContext(task.taskId(), task.instanceId(), task.correlationId(), task.attempt(), input, task.definition(),
                task.chainId(), task.lineage(), task.deadline(), task.sensitivity(), task.report());
    }
}
