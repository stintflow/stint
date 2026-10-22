package io.stintflow.worker.knowledge;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.knowledge.GraphPointer;
import io.stintflow.knowledge.KnowledgeValidationException;
import io.stintflow.spi.BlobNotFoundException;
import io.stintflow.spi.BlobStore;
import io.stintflow.wire.Json;

/**
 * SDD 3c.4, 4.1: the draft of the architecture decisions of a meeting, a blob {@code {"decisions": [{"id", "title", "summary",
 * "motivatedBy": [pointer], "affects"?}]}}, and the limits every reader of it applies — at most {@value #MAX_DECISIONS} decisions,
 * one to {@value #MAX_POINTERS} pointers each, at most {@value #MAX_PAIRS} pairs (decision, pointer) in all — before any pair is
 * checked. A pointer is well formed and of a domain the reader listens to, or the whole draft is refused.
 */
final class AdrDrafts {

    static final int MAX_DECISIONS = 20;
    static final int MAX_POINTERS = 5;
    static final int MAX_PAIRS = 50;
    private static final Pattern ID = Pattern.compile("[a-z0-9][a-z0-9-]{0,62}");

    private AdrDrafts() {
    }

    record Decision(String id, String title, String summary, List<GraphPointer> motivatedBy, String affects) {
    }

    /** The id of the pair of a decision and its {@code index}-th pointer: a decision id never holds {@code #}. */
    static String pairId(String decisionId, int index) {
        return decisionId + "#" + index;
    }

    static List<Decision> read(BlobStore blobs, String draftRef, OriginAllowList origins) {
        JsonNode draft = blob(blobs, draftRef, "The draft");
        JsonNode decisions = draft.path("decisions");
        if (!decisions.isArray() || decisions.isEmpty() || decisions.size() > MAX_DECISIONS) {
            throw Failures.validation("decisions is one to " + MAX_DECISIONS + " decisions");
        }
        List<Decision> out = new ArrayList<>();
        Set<String> ids = new HashSet<>();
        int pairs = 0;
        for (int i = 0; i < decisions.size(); i++) {
            JsonNode d = decisions.get(i);
            String id = d.path("id").asText("");
            String title = d.path("title").asText("");
            String summary = d.path("summary").asText("");
            if (!ID.matcher(id).matches() || !ids.add(id)) {
                throw Failures.validation("decisions[" + i + "]: id is a unique name of lowercase letters, digits and hyphens");
            }
            if (title.isBlank() || title.length() > 200 || summary.isBlank() || summary.length() > 4000) {
                throw Failures.validation("decisions[" + i + "]: title (200) and summary (4000) are required");
            }
            JsonNode pointers = d.path("motivatedBy");
            if (!pointers.isArray() || pointers.isEmpty() || pointers.size() > MAX_POINTERS) {
                throw Failures.validation("decisions[" + i + "]: motivatedBy is one to " + MAX_POINTERS + " pointers");
            }
            List<GraphPointer> parsed = new ArrayList<>();
            for (int p = 0; p < pointers.size(); p++) {
                GraphPointer pointer;
                try {
                    pointer = GraphPointer.parse(pointers.get(p).asText(null));
                } catch (KnowledgeValidationException e) {
                    throw Failures.validation("decisions[" + i + "].motivatedBy[" + p + "]: " + e.getMessage());
                }
                if (!origins.allows(pointer.scope())) {
                    throw Failures.refused("This worker does not listen to the domain of decisions[" + i + "].motivatedBy[" + p + "]");
                }
                parsed.add(pointer);
            }
            pairs += parsed.size();
            if (pairs > MAX_PAIRS) {
                throw Failures.validation("The draft holds more than " + MAX_PAIRS + " pairs of decision and pointer");
            }
            String affects = d.path("affects").asText("");
            if (affects.length() > 200) {
                throw Failures.validation("decisions[" + i + "]: affects is a system name");
            }
            out.add(new Decision(id, title, summary, parsed, affects.isBlank() ? null : affects));
        }
        return out;
    }

    /** What {@code resolve-pointers} resolved for {@code pointer}: the view of its entry, empty if the blob is gone or has no such entry. */
    static Optional<JsonNode> resolved(BlobStore blobs, String viewRef, GraphPointer pointer) {
        if (viewRef == null || viewRef.isBlank() || "null".equals(viewRef)) {
            return Optional.empty();
        }
        Optional<JsonNode> view = ResolvedViews.read(blobs, viewRef);
        if (view.isEmpty()) {
            return Optional.empty();
        }
        for (JsonNode entry : view.get().path("entries")) {
            if (pointer.format().equals(entry.path("pointer").asText()) && "RESOLVED".equals(entry.path("status").asText())) {
                return Optional.of(entry.path("view"));
            }
        }
        return Optional.empty();
    }

    static JsonNode blob(BlobStore blobs, String ref, String what) {
        try {
            return Json.read(blobs.get(URI.create(ref)).toCompletableFuture().join());
        } catch (IllegalArgumentException e) {
            throw Failures.validation(what + " must be a pointer (a URI)");
        } catch (RuntimeException e) {
            throw BlobNotFoundException.in(e) ? Failures.validation(what + " is not in the blob store")
                    : Failures.unavailable(what + " could not be read from the blob store");
        }
    }
}
