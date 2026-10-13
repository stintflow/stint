package io.stintflow.spi;

import java.net.URI;

/**
 * SDD 3a.5, RF6/RF7: what a {@link BlobStore} throws (as the cause of a failed {@code get}) for a blob that isn't there —
 * deleted, never written, or expired by its retention class. Unlike a transient failure, reading again won't help:
 * whoever needed the content must fail, never read anything else in its place.
 */
public class BlobNotFoundException extends RuntimeException {

    private final URI ref;

    public BlobNotFoundException(URI ref, String reason) {
        super("Blob " + ref + " not found: " + reason);
        this.ref = ref;
    }

    public URI ref() {
        return ref;
    }

    /** Whether {@code failure}, or any cause of it, is a missing blob. */
    public static boolean in(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t instanceof BlobNotFoundException) {
                return true;
            }
        }
        return false;
    }
}
