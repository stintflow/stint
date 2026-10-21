package io.stintflow.spi;

import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;

/**
 * Port (SDD 3b.5b): the key of an <i>area</i> — a set of blobs that are erased together. {@link EnvelopeBlobStore} asks it for a
 * key to encrypt each blob with, and for the same key back to read it. Destroying the area's key makes every copy of the area's
 * blobs unreadable at once, wherever the copy is (a replica, an old version, a backup): that is crypto-shredding, and it is what
 * {@code delete} cannot do for a copy it does not know about.
 * <p>
 * Destroying is irreversible and idempotent. A destroyed area stays destroyed: a key is never created again under its name, so
 * nothing can be written to it afterwards.
 */
public interface AreaKeys {

    enum State { ABSENT, ACTIVE, DESTROYED }

    Pattern AREA = Pattern.compile("[a-z0-9][a-z0-9-]{0,62}");

    /**
     * A fresh data key for {@code area}, in the clear and wrapped by the area's key. Creates the area's key the first time.
     *
     * @throws AreaKeyDestroyedException (as the failure of the stage) if the area was destroyed
     */
    CompletionStage<DataKey> newDataKey(String area);

    /**
     * The data key a blob of {@code area} was encrypted with.
     *
     * @throws AreaKeyDestroyedException (as the failure of the stage) if the area was destroyed
     */
    CompletionStage<byte[]> unwrap(String area, byte[] wrappedKey);

    /** Destroys the key of {@code area}; the area ends {@link State#DESTROYED} even if it never had a key. */
    CompletionStage<State> destroy(String area);

    CompletionStage<State> state(String area);

    /** {@code area}, if it is a valid name: lowercase letters, digits and dashes (it is part of an alias and of a header). */
    static String requireArea(String area) {
        if (area == null || !AREA.matcher(area).matches()) {
            throw new IllegalArgumentException("An area is named with lowercase letters, digits and dashes");
        }
        return area;
    }
}
