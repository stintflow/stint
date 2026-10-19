package io.stintflow.knowledge.inmemory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * The text of an entity as it rests in a blob when the entity is restricted (SDD 3b.1, 4.4): the label on the first line,
 * then one {@code key<TAB>value} line per property, with backslash, newline and tab escaped. No library.
 */
final class ContentCodec {

    private ContentCodec() {
    }

    record Content(String label, Map<String, String> properties) {
    }

    static String encode(String label, Map<String, String> properties) {
        StringBuilder out = new StringBuilder(escape(label));
        for (Map.Entry<String, String> property : new TreeMap<>(properties).entrySet()) {
            out.append('\n').append(escape(property.getKey())).append('\t').append(escape(property.getValue()));
        }
        return out.toString();
    }

    static Content decode(String encoded) {
        String[] lines = encoded.split("\n", -1);
        Map<String, String> properties = new LinkedHashMap<>();
        for (int i = 1; i < lines.length; i++) {
            int tab = lines[i].indexOf('\t');
            if (tab > 0) {
                properties.put(unescape(lines[i].substring(0, tab)), unescape(lines[i].substring(tab + 1)));
            }
        }
        return new Content(unescape(lines[0]), properties);
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\n", "\\n").replace("\t", "\\t");
    }

    private static String unescape(String value) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\\' && i + 1 < value.length()) {
                char next = value.charAt(++i);
                out.append(next == 'n' ? '\n' : next == 't' ? '\t' : next);
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }
}
