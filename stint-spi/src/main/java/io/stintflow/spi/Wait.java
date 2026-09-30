package io.stintflow.spi;

import java.time.Instant;

/**
 * A generic wait registration (SDD 1.2, RF2): an instance suspended at {@code position}, waiting for
 * whatever {@code waitKey} identifies. Typed key conventions: {@code task:<correlationId>},
 * {@code timer:<timerId>} and (SDD 2.3) {@code event:<type>:<shape>:<valueHash>}.
 */
public record Wait(String waitKey, String instanceId, String position, Instant createdAt) {
}
