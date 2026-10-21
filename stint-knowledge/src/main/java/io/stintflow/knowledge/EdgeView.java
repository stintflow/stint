package io.stintflow.knowledge;

import io.stintflow.spi.Sensitivity;

/** An edge as the inspection sees it (SDD 3b.6, 4.1): its identity, type, ends and level. */
public record EdgeView(String key, String type, String from, String to, Sensitivity level, long addedSequence) {
}
