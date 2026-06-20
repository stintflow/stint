package io.stintflow.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Shared Jackson holder. Kept tiny and dependency-free so stint-core stays framework-light. */
public final class Json {

    public static final ObjectMapper MAPPER = new ObjectMapper();

    private Json() {
    }

    public static ObjectNode obj() {
        return MAPPER.createObjectNode();
    }

    public static byte[] bytes(JsonNode node) {
        try {
            return MAPPER.writeValueAsBytes(node);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize JSON", e);
        }
    }

    public static JsonNode read(byte[] data) {
        try {
            return MAPPER.readTree(data);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse JSON", e);
        }
    }
}
