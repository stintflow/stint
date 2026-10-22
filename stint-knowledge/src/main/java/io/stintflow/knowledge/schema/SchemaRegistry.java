package io.stintflow.knowledge.schema;

import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.stintflow.knowledge.KnowledgeValidationException;

/**
 * The schemas a worker knows (SDD 3b.2, 4.1). A definition names one as {@code <id>@<major>} ({@code x-stint-knowledge.schema});
 * the registry resolves it to the highest version of that major it holds, since a minor only adds. {@code (id, version)} is
 * immutable: registering other content under a version already held is refused.
 */
public final class SchemaRegistry {

    private static final Pattern REFERENCE = Pattern.compile("([a-z0-9][a-z0-9-]*)@(\\d+)");

    private final ConcurrentMap<String, KnowledgeSchema> byVersion = new ConcurrentHashMap<>();

    /** A registry holding the reference schemas of the module ({@code meetings@1.0.0}, {@code architecture@1.0.0}, {@code catalog@1.0.0}, {@code catalog-links@1.0.0}). */
    public static SchemaRegistry withReferenceSchemas() {
        SchemaRegistry registry = new SchemaRegistry();
        registry.register(KnowledgeSchema.meetings());
        registry.register(KnowledgeSchema.architecture());
        registry.register(KnowledgeSchema.architectureDecisions());
        registry.register(KnowledgeSchema.catalog());
        registry.register(KnowledgeSchema.catalogLinks());
        return registry;
    }

    public SchemaRegistry register(KnowledgeSchema schema) {
        KnowledgeSchema held = byVersion.putIfAbsent(schema.fullVersion(), schema);
        if (held != null && !held.contentHash().equals(schema.contentHash())) {
            throw new KnowledgeValidationException("Schema " + schema.fullVersion() + " is already registered with other "
                    + "content: a version is immutable (publish a new version)");
        }
        return this;
    }

    public Optional<KnowledgeSchema> find(String id, String version) {
        return Optional.ofNullable(byVersion.get(id + "@" + version));
    }

    /** {@code meetings@1} → the highest {@code meetings@1.x.y} held. */
    public Optional<KnowledgeSchema> resolve(String reference) {
        Matcher matcher = REFERENCE.matcher(reference);
        if (!matcher.matches()) {
            throw new KnowledgeValidationException("A schema is named <id>@<major>, e.g. meetings@1");
        }
        String id = matcher.group(1);
        int major = Integer.parseInt(matcher.group(2));
        return byVersion.values().stream().filter(s -> s.id().equals(id) && s.major() == major)
                .max(Comparator.comparingInt((KnowledgeSchema s) -> minor(s.version())).thenComparingInt(s -> patch(s.version())));
    }

    private static int minor(String version) {
        return Integer.parseInt(version.split("\\.")[1]);
    }

    private static int patch(String version) {
        return Integer.parseInt(version.split("\\.")[2]);
    }
}
