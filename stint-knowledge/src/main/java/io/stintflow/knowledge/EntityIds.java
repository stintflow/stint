package io.stintflow.knowledge;

import java.text.Normalizer;
import java.util.Locale;

/**
 * SDD 3b.1, 4.6: ids for entities that are new, and the normal form names are compared in. Two spellings that differ more than
 * case, accents, punctuation and spacing are two entities here; telling them apart as a duplicate is SDD 3b.2/3b.6's.
 */
public final class EntityIds {

    private EntityIds() {
    }

    /** {@code e-} and the first 32 hex digits of SHA-256 of the type and the normalized name. */
    public static String of(String type, String canonicalName) {
        return "e-" + PassageIds.sha256Hex(type + '\0' + normalize(canonicalName)).substring(0, 32);
    }

    /** NFKC, lowercase, accents dropped, punctuation turned into spaces, spaces collapsed and trimmed. */
    public static String normalize(String name) {
        String decomposed = Normalizer.normalize(name, Normalizer.Form.NFKD).replaceAll("\\p{M}+", "");
        return Normalizer.normalize(decomposed, Normalizer.Form.NFKC).toLowerCase(Locale.ROOT)
                .replaceAll("[^\\p{L}\\p{N}]+", " ").trim();
    }
}
