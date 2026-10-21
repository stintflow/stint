package io.stintflow.knowledge;

import java.util.Objects;

/**
 * Asks the store to move a base to another embedding model (SDD 3b.5, 4.1): every vector is recomputed with {@code target} in
 * one atomic step.
 *
 * @param requestId identifies the request in the change log
 * @param target    the embedder of the new model; the store uses it for this scope from then on
 * @param reason    a code, not a text
 */
public record ReindexRequest(String requestId, Embedder target, String reason) {

    public ReindexRequest {
        Objects.requireNonNull(requestId, "requestId");
        Objects.requireNonNull(target, "target");
        reason = reason == null ? "reindex" : reason;
    }
}
