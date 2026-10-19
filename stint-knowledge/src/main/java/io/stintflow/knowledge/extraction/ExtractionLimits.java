package io.stintflow.knowledge.extraction;

import io.stintflow.knowledge.KnowledgeValidationException;

/**
 * What an extraction may spend (SDD 3b.2, 4.7). The numbers are estimates of tokens by characters / 4 — a bound that is cheap
 * and the same everywhere, not a count of a model's tokenizer.
 *
 * @param maxGlossaryTerms     terms of the glossary in one prompt
 * @param maxGlossaryTokens    estimated tokens of those terms
 * @param maxWindowTokens      estimated tokens of the passages in one call
 * @param maxTokensPerDocument estimated tokens of all the calls of one document; past it the extraction stops and writes nothing
 */
public record ExtractionLimits(int maxGlossaryTerms, int maxGlossaryTokens, int maxWindowTokens, int maxTokensPerDocument) {

    public static final ExtractionLimits DEFAULT = new ExtractionLimits(40, 1_500, 3_000, 40_000);

    public ExtractionLimits {
        if (maxGlossaryTerms < 0 || maxGlossaryTokens < 0 || maxWindowTokens < 1 || maxTokensPerDocument < 1) {
            throw new KnowledgeValidationException("The limits of an extraction are positive");
        }
    }

    /** Estimated tokens of {@code text}: its characters over four, rounded up. */
    public static int tokens(String text) {
        return (text.length() + 3) / 4;
    }
}
