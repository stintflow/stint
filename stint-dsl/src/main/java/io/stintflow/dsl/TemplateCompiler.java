package io.stintflow.dsl;

import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Compiles a YAML/JSON value into jq source (SDD 1.4, sec. 8b/8d): every {@code ${ ... }} leaf
 * becomes the wrapped jq expression, verbatim; everything else is reconstructed as a jq literal, so
 * the compiled expression rebuilds an equivalent structure with just the expression leaves computed
 * against {@code $context}/{@code .}/{@code $workflow} (whatever the evaluator has in scope).
 */
final class TemplateCompiler {

    private static final Pattern WHOLE_EXPRESSION = Pattern.compile("^\\$\\{\\s*(.*?)\\s*}$", Pattern.DOTALL);
    private static final ObjectMapper JSON = new ObjectMapper();

    private TemplateCompiler() {
    }

    /** {@code with}/{@code set}-style: {@code ${...}} marks an expression leaf; everything else is a literal. */
    static String compileTemplate(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return "null";
        }
        if (node.isTextual()) {
            Matcher m = WHOLE_EXPRESSION.matcher(node.textValue());
            return m.matches() ? "(" + m.group(1) + ")" : jsonLiteral(node);
        }
        if (node.isObject()) {
            StringBuilder sb = new StringBuilder("{");
            Iterator<String> fields = node.fieldNames();
            boolean first = true;
            while (fields.hasNext()) {
                String field = fields.next();
                if (!first) {
                    sb.append(", ");
                }
                first = false;
                sb.append(jsonLiteral(field)).append(": ").append(compileTemplate(node.get(field)));
            }
            return sb.append('}').toString();
        }
        if (node.isArray()) {
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < node.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(compileTemplate(node.get(i)));
            }
            return sb.append(']').toString();
        }
        return jsonLiteral(node); // number, boolean
    }

    /**
     * {@code input.from}/{@code output.as}/{@code export.as}/{@code when}-style: the DSL 1.0 schema
     * types these fields as always being a runtime expression, so no {@code ${}} is required (see
     * the spec's own {@code switch} example: {@code when: .orderType == "electronic"}); an optional
     * {@code ${ }} wrapper is tolerated and stripped for writers who prefer to be explicit.
     */
    static String compileBareExpression(String raw) {
        Matcher m = WHOLE_EXPRESSION.matcher(raw);
        return m.matches() ? m.group(1) : raw;
    }

    private static String jsonLiteral(JsonNode node) {
        try {
            return JSON.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unexpected failure serializing a literal value", e);
        }
    }

    private static String jsonLiteral(String raw) {
        try {
            return JSON.writeValueAsString(raw);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unexpected failure serializing a literal key", e);
        }
    }
}
