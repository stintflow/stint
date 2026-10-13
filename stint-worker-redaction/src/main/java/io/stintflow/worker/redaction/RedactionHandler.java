package io.stintflow.worker.redaction;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import io.stintflow.spi.ErrorInfo.StandardType;
import io.stintflow.wire.Json;
import io.stintflow.wire.redaction.RedactionContract;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;

/**
 * The task handler behind a {@code call: redact} (SDD 3a.5, RF5): redacts every text of the request's {@code input} —
 * keys and structure untouched — with a {@link LayeredRedactor}, and answers the redaction contract of
 * {@code stint-wire}: the redacted input, the counts by type (never a value), the doubt and the provenance.
 * <p>
 * <b>Fail closed:</b> a redactor that throws fails the task ({@code runtime}, 500) — the engine keeps the instance at
 * its level, and the original text never comes back. Logs carry the ids and the counts only.
 */
public final class RedactionHandler implements TaskHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RedactionHandler.class);

    private final LayeredRedactor redactor;
    private final Pseudonyms pseudonyms;

    public RedactionHandler(LayeredRedactor redactor, Pseudonyms pseudonyms) {
        this.redactor = Objects.requireNonNull(redactor, "redactor");
        this.pseudonyms = Objects.requireNonNull(pseudonyms, "pseudonyms");
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext ctx) {
        try {
            JsonNode request = ctx.input();
            Set<String> types = new HashSet<>();
            request.path(RedactionContract.TYPES).forEach(t -> types.add(t.asText()));
            RedactionLayer.Scope scope = new RedactionLayer.Scope(ctx.instanceId(), types, pseudonyms);
            Map<String, Integer> counts = new LinkedHashMap<>();
            boolean[] doubt = {false};
            JsonNode redacted = redact(request.get(RedactionContract.INPUT), scope, counts, doubt);

            ObjectNode result = Json.obj();
            result.set(RedactionContract.OUTPUT, redacted == null ? Json.MAPPER.nullNode() : redacted);
            ObjectNode countsNode = result.putObject(RedactionContract.COUNTS);
            counts.forEach(countsNode::put);
            result.put(RedactionContract.DOUBT, doubt[0]);
            ObjectNode provenance = result.putObject(RedactionContract.PROVENANCE);
            provenance.put("redactor", redactor.name());
            provenance.put("version", redactor.version());
            ArrayNode layers = provenance.putArray("layers");
            redactor.layerNames().forEach(layers::add);
            LOG.info("Redaction of task {} in instance {}: counts {}, doubt {}", ctx.taskId(), ctx.instanceId(), counts,
                    doubt[0]);
            return CompletableFuture.completedFuture(result);
        } catch (RuntimeException e) {
            LOG.warn("Redaction of task {} in instance {} failed ({}): the instance keeps its level", ctx.taskId(),
                    ctx.instanceId(), e.getClass().getName());
            return CompletableFuture.failedFuture(new TaskFailure(StandardType.RUNTIME.uri(), 500,
                    "The redaction failed (" + e.getClass().getSimpleName() + ")", null));
        }
    }

    private JsonNode redact(JsonNode node, RedactionLayer.Scope scope, Map<String, Integer> counts, boolean[] doubt) {
        if (node == null || node.isNull()) {
            return node;
        }
        if (node.isTextual()) {
            RedactionLayer.Result result = redactor.redact(node.textValue(), scope);
            result.counts().forEach((type, n) -> counts.merge(type, n, Integer::sum));
            doubt[0] |= result.doubt();
            return TextNode.valueOf(result.text());
        }
        if (node.isObject()) {
            ObjectNode out = Json.obj();
            node.properties().forEach(e -> out.set(e.getKey(), redact(e.getValue(), scope, counts, doubt)));
            return out;
        }
        if (node.isArray()) {
            ArrayNode out = Json.MAPPER.createArrayNode();
            node.forEach(element -> out.add(redact(element, scope, counts, doubt)));
            return out;
        }
        if (node.isNumber() && node.asText().replaceAll("\\D", "").length() >= 8) {
            doubt[0] = true;   // a long number may be a phone or a document: kept, but never trusted (fail closed)
        }
        return node;   // numbers and booleans: not text, kept as they are
    }
}
