package io.stintflow.knowledge;

import java.util.Objects;

import io.stintflow.spi.Sensitivity;

/**
 * A piece of text of a document (SDD 3b.1, 4.2), kept in the language it was written in.
 *
 * @param passageId   {@link PassageIds#of}: the same document, position and text always give the same id
 * @param ordinal     the position in the document
 * @param language    BCP 47 language of the text, e.g. {@code en}
 * @param sensitivity its level; absent reads as {@code RESTRICTED}
 */
public record Passage(String passageId, int ordinal, String text, String language, Sensitivity sensitivity) {

    public Passage {
        Objects.requireNonNull(passageId, "passageId");
        Objects.requireNonNull(text, "text");
        Objects.requireNonNull(language, "language");
        sensitivity = sensitivity == null ? Sensitivity.RESTRICTED : sensitivity;
    }
}
