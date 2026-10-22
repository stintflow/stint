package io.stintflow.worker.knowledge;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.CompletionException;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.spi.BlobNotFoundException;
import io.stintflow.spi.BlobStore;
import io.stintflow.wire.Json;

/**
 * SDD 3c.2, 4.4: reading what {@code resolve-pointers} resolved. The content lives in a blob of a retention class, not in the
 * state of the instance; a blob that has expired or been discarded is simply gone, and the answer is empty — never an error and
 * never something left over. Whoever needs the content again resolves again.
 */
public final class ResolvedViews {

    private ResolvedViews() {
    }

    /** The document {@code {"entries": [{"pointer","status","viewVersion"?,"view"?}]}} at {@code viewRef}, or empty if it is gone. */
    public static Optional<JsonNode> read(BlobStore blobs, String viewRef) {
        try {
            return Optional.of(Json.read(blobs.get(URI.create(viewRef)).toCompletableFuture().join()));
        } catch (CompletionException e) {
            if (e.getCause() instanceof BlobNotFoundException) {
                return Optional.empty();
            }
            throw e;
        } catch (BlobNotFoundException e) {
            return Optional.empty();
        }
    }

    /** Deletes the blob once it was used: the content does not wait for its retention to go. */
    public static void discard(BlobStore blobs, String viewRef) {
        blobs.delete(URI.create(viewRef)).toCompletableFuture().join();
    }
}
