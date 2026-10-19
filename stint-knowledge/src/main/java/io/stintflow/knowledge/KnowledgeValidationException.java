package io.stintflow.knowledge;

/**
 * A batch, query or request the store refuses (SDD 3b.1). The message names fields and rules, never the content.
 */
public class KnowledgeValidationException extends RuntimeException {

    public KnowledgeValidationException(String message) {
        super(message);
    }
}
