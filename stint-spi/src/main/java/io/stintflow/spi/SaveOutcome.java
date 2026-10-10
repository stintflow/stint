package io.stintflow.spi;

/** Result of a conditional {@link StateStore#save}: whether the version/wait preconditions held. */
public enum SaveOutcome {
    OK,
    CONFLICT
}
