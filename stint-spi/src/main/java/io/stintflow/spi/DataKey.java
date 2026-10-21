package io.stintflow.spi;

import java.util.Objects;

/**
 * SDD 3b.5b: a data key for one blob, in the clear (to encrypt now and drop) and wrapped by the area's key (to keep beside the
 * blob). The clear bytes never reach a log, an exception or {@code toString}.
 */
public record DataKey(byte[] plaintext, byte[] wrapped) {

    public DataKey {
        Objects.requireNonNull(plaintext, "plaintext");
        Objects.requireNonNull(wrapped, "wrapped");
    }

    @Override
    public String toString() {
        return "DataKey[wrapped=" + wrapped.length + " bytes]";
    }
}
