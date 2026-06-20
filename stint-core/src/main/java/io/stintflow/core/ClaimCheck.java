package io.stintflow.core;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.spi.AdapterCapabilities;
import io.stintflow.spi.BlobStore;

/**
 * Transparent claim-check: when a payload exceeds the transport's {@link AdapterCapabilities#maxPayloadBytes()},
 * it is offloaded to a {@link BlobStore} and replaced on the wire by a small pointer. The worker side (and the
 * engine, for results) rehydrates it. This is what keeps a 12&nbsp;000-row query result out of a 256&nbsp;KB queue.
 */
public final class ClaimCheck {

    public static final String POINTER_FIELD = "__stint_claim_check__";

    private ClaimCheck() {
    }

    public static CompletionStage<JsonNode> offload(JsonNode payload, AdapterCapabilities caps, BlobStore blob, String key) {
        byte[] bytes = Json.bytes(payload);
        if (bytes.length <= caps.maxPayloadBytes()) {
            return CompletableFuture.completedFuture(payload);
        }
        return blob.put(bytes, key).thenApply(uri -> {
            ObjectNode pointer = Json.obj();
            pointer.put(POINTER_FIELD, uri.toString());
            return pointer;
        });
    }

    public static CompletionStage<JsonNode> rehydrate(JsonNode payload, BlobStore blob) {
        if (payload != null && payload.has(POINTER_FIELD)) {
            URI ref = URI.create(payload.get(POINTER_FIELD).asText());
            return blob.get(ref).thenApply(Json::read);
        }
        return CompletableFuture.completedFuture(payload);
    }
}
