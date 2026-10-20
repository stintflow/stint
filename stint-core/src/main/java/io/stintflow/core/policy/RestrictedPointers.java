package io.stintflow.core.policy;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.spi.RetentionClass;

/**
 * SDD 3a.5, 4.5: a pointer to restricted content — a claim-check, a content-at-rest pointer or a fact's
 * {@code dataref} — names a blob under the {@code restricted} class prefix. After a redaction lowers an instance, such
 * a pointer left in its data may not go to a route the policy doesn't allow for restricted data: whoever can read the
 * pointer could read the content. The author exports the redacted text instead.
 */
public final class RestrictedPointers {

    private RestrictedPointers() {
    }

    /** Whether any text in {@code data} names a blob of class {@code restricted}. */
    public static boolean in(JsonNode data, RetentionClass restricted) {
        if (data == null) {
            return false;
        }
        if (data.isTextual()) {
            String text = data.textValue();
            // a key prefix as stored (S3: retention/<class>/...; a local store flattens the slashes)
            return text.contains(restricted.prefix()) || text.contains(restricted.prefix().replace('/', '_'));
        }
        for (JsonNode child : data) {
            if (in(child, restricted)) {
                return true;
            }
        }
        return false;
    }
}
