package io.stintflow.worker.knowledge;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.schema.KnowledgeSchema;

/**
 * SDD 3c.2, 4.2 and RF8: the contract an origin publishes for one base — versioned, by node type. A property the schema marks
 * {@code sensitive} is shown only if the view says {@code allowSensitive}, which the audit records; without it, listing one is an
 * error at configuration time (the view is not built).
 */
public record PublishedView(String version, KnowledgeScope base, Map<String, PublishedType> types, boolean allowSensitive) {

    public PublishedView {
        Objects.requireNonNull(version, "version");
        Objects.requireNonNull(base, "base");
        if (version.isBlank()) {
            throw new IllegalArgumentException("A published view has a version");
        }
        types = Map.copyOf(types);
    }

    /** Builds the view and checks it against the schema of the base: no sensitive property unless allowed, no type or property unknown. */
    public static PublishedView of(String version, KnowledgeScope base, KnowledgeSchema schema, List<PublishedType> types,
            boolean allowSensitive) {
        Map<String, PublishedType> byType = new LinkedHashMap<>();
        for (PublishedType published : types) {
            KnowledgeSchema.NodeType node = schema.nodes().get(published.type());
            if (node == null) {
                throw new IllegalArgumentException("The schema " + schema.fullVersion() + " has no node type " + published.type());
            }
            for (String property : published.properties()) {
                KnowledgeSchema.Property declared = node.properties().get(property);
                if (declared == null) {
                    throw new IllegalArgumentException("The type " + published.type() + " has no property " + property);
                }
                if (declared.sensitive() && !allowSensitive) {
                    throw new IllegalArgumentException("The property " + published.type() + "." + property
                            + " is sensitive: a view shows it only with allowSensitive");
                }
            }
            for (String relation : published.relations()) {
                if (!schema.relations().containsKey(relation)) {
                    throw new IllegalArgumentException("The schema has no relation " + relation);
                }
            }
            if (byType.put(published.type(), published) != null) {
                throw new IllegalArgumentException("The type " + published.type() + " is published twice");
            }
        }
        return new PublishedView(version, base, byType, allowSensitive);
    }
}
