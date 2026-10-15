package io.stintflow.spi;

import java.time.Duration;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * SDD 3a.5, RF7: how long a class of blobs lives — e.g. {@code restricted} for 30 days. A blob put with a class goes
 * under the key prefix {@code retention/<name>/}, which is what a bucket's lifecycle rule (or a local store's clock)
 * expires.
 *
 * @param name the class: lowercase letters, digits and dashes (it is a key prefix and an S3 tag value)
 * @param ttl  how long a blob of the class lives after it is put; at least a day (S3's lifecycle granularity)
 */
public record RetentionClass(String name, Duration ttl) {

    private static final Pattern NAME = Pattern.compile("[a-z0-9][a-z0-9-]{0,62}");

    public static final String KEY_PREFIX = "retention/";
    /** SDD 3a.5, 8e: the class of a restricted instance's content when the policy names none. */
    public static final RetentionClass RESTRICTED_DEFAULT = new RetentionClass("restricted", Duration.ofDays(30));

    public RetentionClass {
        if (name == null || !NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("A retention class is named with lowercase letters, digits and dashes");
        }
        Objects.requireNonNull(ttl, "ttl");
        if (ttl.compareTo(Duration.ofDays(1)) < 0) {
            throw new IllegalArgumentException("A retention class lives at least a day (S3 lifecycle granularity)");
        }
    }

    /** {@code key} under this class's prefix. */
    public String prefixed(String key) {
        return KEY_PREFIX + name + "/" + key;
    }

    /** The prefix of the class's blobs, {@code retention/<name>/}. */
    public String prefix() {
        return KEY_PREFIX + name + "/";
    }
}
