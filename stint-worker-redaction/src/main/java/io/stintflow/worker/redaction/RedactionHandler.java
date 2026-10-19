package io.stintflow.worker.redaction;

import java.net.URI;
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

import io.stintflow.spi.BlobStore;
import io.stintflow.spi.ErrorInfo.StandardType;
import io.stintflow.wire.ClaimCheck;
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
    private final BlobStore blobs;
    private final boolean allowStable;

    public RedactionHandler(LayeredRedactor redactor, Pseudonyms pseudonyms) {
        this(redactor, pseudonyms, null);
    }

    /**
     * SDD 3a.6, 4.6: with a {@code blobs} store, an {@code input} that is a claim-check pointer — a raw transcript that
     * rests in a restricted blob — is read from it. A transcript ({@code transcript.v1}, with {@code segments}) is
     * redacted as its segments' text, one segment per line; any other JSON as it is.
     */
    public RedactionHandler(LayeredRedactor redactor, Pseudonyms pseudonyms, BlobStore blobs) {
        this(redactor, pseudonyms, blobs, false);
    }

    private RedactionHandler(LayeredRedactor redactor, Pseudonyms pseudonyms, BlobStore blobs, boolean allowStable) {
        this.redactor = Objects.requireNonNull(redactor, "redactor");
        this.pseudonyms = Objects.requireNonNull(pseudonyms, "pseudonyms");
        this.blobs = blobs;
        this.allowStable = allowStable;
    }

    /**
     * SDD 3b.2, 4.6: this redactor may give the <em>stable</em> pseudonyms a request asks for ({@code pseudonyms.mode: scope}):
     * the same person gets the same {@code PERSON_<32 hex>} in every meeting of one knowledge base. Off by default, so a request
     * for it is answered with the pseudonyms of the instance (and the provenance says which) — the definition asks, the worker
     * decides, since the secret behind the pseudonyms is the worker's.
     */
    public RedactionHandler allowingStablePseudonyms() {
        return new RedactionHandler(redactor, pseudonyms, blobs, true);
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext ctx) {
        try {
            JsonNode request = ctx.input();
            Set<String> types = new HashSet<>();
            request.path(RedactionContract.TYPES).forEach(t -> types.add(t.asText()));
            JsonNode asked = request.path(RedactionContract.PSEUDONYMS);
            boolean askedScope = RedactionContract.MODE_SCOPE.equals(asked.path(RedactionContract.PSEUDONYMS_MODE).asText())
                    && asked.path(RedactionContract.PSEUDONYMS_SCOPE).isTextual();
            boolean stable = askedScope && allowStable;
            RedactionLayer.Scope scope = stable
                    ? new RedactionLayer.Scope(asked.get(RedactionContract.PSEUDONYMS_SCOPE).asText(), types, pseudonyms, true)
                    : new RedactionLayer.Scope(ctx.instanceId(), types, pseudonyms);
            Map<String, Integer> counts = new LinkedHashMap<>();
            boolean[] doubt = {false};
            JsonNode redacted = redact(resolve(request.get(RedactionContract.INPUT)), scope, counts, doubt);

            ObjectNode result = Json.obj();
            result.set(RedactionContract.OUTPUT, redacted == null ? Json.MAPPER.nullNode() : redacted);
            ObjectNode countsNode = result.putObject(RedactionContract.COUNTS);
            counts.forEach(countsNode::put);
            result.put(RedactionContract.DOUBT, doubt[0]);
            ObjectNode provenance = result.putObject(RedactionContract.PROVENANCE);
            provenance.put("redactor", redactor.name());
            provenance.put("version", redactor.version());
            provenance.put("pseudonyms", stable ? "scope" : "instance");   // which the result holds, whatever was asked
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

    /** The input itself, or what its pointer points at (a transcript as its text). */
    private JsonNode resolve(JsonNode input) {
        if (blobs == null || !ClaimCheck.isPointer(input)) {
            return input;
        }
        URI ref = URI.create(input.get(ClaimCheck.POINTER_FIELD).asText());
        JsonNode pointed = Json.read(blobs.get(ref).toCompletableFuture().join());
        if (pointed.has("segments") && pointed.get("segments").isArray()) {
            StringBuilder text = new StringBuilder();
            for (JsonNode segment : pointed.get("segments")) {
                text.append(text.isEmpty() ? "" : "\n").append(segment.path("text").asText().strip());
            }
            return TextNode.valueOf(text.toString());
        }
        return pointed;
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
