package io.stintflow.knowledge;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * SDD 3b.1, 4.6: the id of a passage is a function of where and what it is, so writing a document again gives the same ids
 * and never a duplicate.
 */
public final class PassageIds {

    private PassageIds() {
    }

    /** {@code p-} and the first 32 hex digits of SHA-256 of scope, document, position and text. */
    public static String of(KnowledgeScope scope, String documentId, int ordinal, String text) {
        return "p-" + sha256Hex(scope.key() + '\0' + documentId + '\0' + ordinal + '\0' + sha256Hex(text)).substring(0, 32);
    }

    /** SHA-256 of {@code text} as UTF-8, in lowercase hex. */
    public static String sha256Hex(String text) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }
}
