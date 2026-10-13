package io.stintflow.worker.redaction;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.HexFormat;
import java.util.Locale;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Pseudonyms stable within an instance (SDD 3a.5, 4.4): the same value of the same type gets the same label —
 * {@code PERSON_3f9a2c} — in every step of one instance, a different one in another instance, and there is no map
 * back. The label is an HMAC of the instance, the type and the normalized value under the worker's secret; nothing is
 * stored, so it survives a worker restart and a redelivery.
 */
public final class Pseudonyms {

    private final byte[] secret;

    /** @param secret the worker's own secret (from its secret store); at least 16 bytes */
    public Pseudonyms(byte[] secret) {
        if (secret == null || secret.length < 16) {
            throw new IllegalArgumentException("A pseudonym secret has at least 16 bytes");
        }
        this.secret = secret.clone();
    }

    /** The label of {@code value} of {@code type} within {@code instanceId}, e.g. {@code EMAIL_3f9a2c}. */
    public String label(String instanceId, String type, String value) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret, "HmacSHA256"));
            byte[] digest = mac.doFinal((instanceId + '\0' + type + '\0' + normalize(value))
                    .getBytes(StandardCharsets.UTF_8));
            return type.toUpperCase(Locale.ROOT) + "_" + HexFormat.of().formatHex(digest, 0, 3);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HmacSHA256 is not available", e);
        }
    }

    /** Case and separators don't make another value: {@code A.B@x.com} and {@code a.b@X.com} are one. */
    static String normalize(String value) {
        return value.toLowerCase(Locale.ROOT).replaceAll("[\\s().-]", "");
    }
}
