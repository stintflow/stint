package io.stintflow.worker.knowledge;

import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.knowledge.DeleteRequest;

/**
 * What the tasks {@code knowledge-erase} and {@code knowledge-verify} are asked (SDD 3b.5, 4.3): the same request, so the
 * verification looks at exactly what the erasure was told to remove. Ids only — a document id, the id of a person's entity —
 * never a name.
 * <p>
 * {@code {"requestId", "kind": "document" | "person", "documentId"? , "entityId"?, "passages"?: "LINKED" | "NONE",
 * "scope": {"namespace", "base"}}}
 */
record ErasureInput(String requestId, String kind, DeleteRequest request) {

    private static final Pattern REQUEST_ID = Pattern.compile("[A-Za-z0-9._:-]{1,80}");
    private static final Pattern ENTITY_ID = Pattern.compile("e-[0-9a-f]{32}");

    static ErasureInput parse(JsonNode in) {
        String requestId = in.path("requestId").asText("");
        if (!REQUEST_ID.matcher(requestId).matches()) {
            throw Failures.validation("requestId is 1 to 80 letters, digits or . _ : -");
        }
        String kind = in.path("kind").asText("");
        return switch (kind) {
            case "document" -> {
                String documentId = in.path("documentId").asText("");
                if (documentId.isBlank() || documentId.length() > 200) {
                    throw Failures.validation("documentId is required for kind 'document'");
                }
                yield new ErasureInput(requestId, kind, new DeleteRequest.ByDocument(requestId, "erasure:document", documentId));
            }
            case "person" -> {
                String entityId = in.path("entityId").asText("");
                if (!ENTITY_ID.matcher(entityId).matches()) {
                    throw Failures.validation("entityId is required for kind 'person' (e- and 32 hex digits)");
                }
                DeleteRequest.PassagePolicy passages = switch (in.path("passages").asText("LINKED")) {
                    case "LINKED" -> DeleteRequest.PassagePolicy.LINKED;
                    case "NONE" -> DeleteRequest.PassagePolicy.NONE;
                    default -> throw Failures.validation("passages is LINKED or NONE");
                };
                yield new ErasureInput(requestId, kind, new DeleteRequest.ByPerson(requestId, "erasure:person", entityId, passages));
            }
            default -> throw Failures.validation("kind is 'document' or 'person'");
        };
    }
}
