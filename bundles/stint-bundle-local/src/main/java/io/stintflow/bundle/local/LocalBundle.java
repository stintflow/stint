package io.stintflow.bundle.local;

/**
 * Marker for the local bundle (core + in-memory connectors). Depend on this artifact to pull in the
 * zero-cloud dev/test stack. The class itself carries no behaviour.
 */
public final class LocalBundle {
    private LocalBundle() {
    }
}
