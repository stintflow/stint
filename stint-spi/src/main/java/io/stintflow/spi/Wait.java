package io.stintflow.spi;

import java.time.Instant;

/**
 * A generic wait registration (SDD 1.2, RF2): an instance suspended at {@code position}, waiting for
 * whatever {@code waitKey} identifies. Typed key conventions: {@code task:<correlationId>},
 * {@code timer:<timerId>} and, in a later SDD, {@code event:<type>:<key>}.
 */
public record Wait(String waitKey, String instanceId, String position, Instant createdAt) {
}
