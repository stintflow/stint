package io.stintflow.dsl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;

/**
 * Maps each JSON Pointer in a YAML document to the line it starts on (SDD 1.4, RF5/sec. 8e).
 * Plain {@code ObjectMapper.readTree()} discards {@link JsonLocation} per node, so this walks the
 * token stream a second time, independently of the compiler's {@code JsonNode} tree, tracking the
 * same pointer path the compiler builds.
 */
final class YamlLocationIndex {

    static final YamlLocationIndex EMPTY = new YamlLocationIndex(Map.of());

    private final Map<String, JsonLocation> byPointer;

    private YamlLocationIndex(Map<String, JsonLocation> byPointer) {
        this.byPointer = byPointer;
    }

    static YamlLocationIndex build(byte[] yamlBytes) throws IOException {
        Map<String, JsonLocation> byPointer = new HashMap<>();
        try (YAMLParser parser = (YAMLParser) new YAMLFactory().createParser(yamlBytes)) {
            if (parser.nextToken() != null) {
                indexValue(parser, "", byPointer);
            }
        }
        return new YamlLocationIndex(byPointer);
    }

    Integer lineOf(String pointer) {
        JsonLocation loc = byPointer.get(pointer);
        return loc == null ? null : loc.getLineNr();
    }

    private static void indexValue(YAMLParser p, String pointer, Map<String, JsonLocation> out) throws IOException {
        JsonToken t = p.currentToken();
        out.put(pointer, p.currentTokenLocation());
        if (t == JsonToken.START_OBJECT) {
            while (p.nextToken() != JsonToken.END_OBJECT) {
                String field = p.currentName();
                p.nextToken(); // move to the field's value
                indexValue(p, pointer + "/" + escape(field), out);
            }
        } else if (t == JsonToken.START_ARRAY) {
            int i = 0;
            while (p.nextToken() != JsonToken.END_ARRAY) {
                indexValue(p, pointer + "/" + i, out);
                i++;
            }
        }
        // scalar: already fully consumed, nothing more to do
    }

    private static String escape(String token) {
        return token.replace("~", "~0").replace("/", "~1");
    }
}
