package io.stintflow.knowledge;

import io.stintflow.spi.Sensitivity;

/**
 * A node as the inspection sees it (SDD 3b.6, 4.1): the id, the type and the level always; the label unless the node is
 * restricted — then {@code null}, because the label rests in a blob this port never opens. No properties.
 *
 * @param addedSequence the sequence of the log entry that added it, or 0 if the log no longer says
 * @param evidence      how many documents sustain it
 */
public record NodeView(String id, String type, Sensitivity level, String label, long addedSequence, int evidence) {
}
