package io.stintflow.knowledge;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import io.stintflow.knowledge.extraction.Glossary;
import io.stintflow.spi.Sensitivity;

/**
 * The catalog of entities as a batch (SDD 3c.3, 4.1): a glossary, written into the base of the catalog domain, one batch per
 * revision of the file. The catalog holds {@code System}, {@code Product} and {@code Team}, all {@code internal}; a person is
 * personal data and has its own path ({@code resolve-person}), so a term of another type refuses the whole load — with the
 * count by type, never the name. The {@link Glossary} already refuses a repeated id and a name or alias two terms of one type
 * share.
 * <p>
 * The document is always {@value #DOCUMENT}, and the revision is the one of the file: a later revision replaces the earlier one,
 * an entry taken out of the file is removed (its pointer then resolves as {@code REMOVED}, and the id is never reused), the same
 * revision again changes nothing.
 */
public final class CatalogSeed {

    public static final String DOCUMENT = "catalog";
    public static final String SCHEMA_VERSION = "catalog@1.0.0";

    private CatalogSeed() {
    }

    /**
     * @param revision the revision of the catalog file, at least 1
     * @throws KnowledgeValidationException if a term is of a type the catalog does not hold, or the revision is not positive
     */
    public static KnowledgeBatch of(Glossary catalog, long revision) {
        if (revision < 1) {
            throw new KnowledgeValidationException("The revision of the catalog is at least 1");
        }
        Map<String, Integer> outside = new TreeMap<>();
        for (Glossary.Term term : catalog.terms()) {
            if (!CatalogTypes.NAMES.contains(term.type())) {
                outside.merge(term.type(), 1, Integer::sum);
            }
        }
        if (!outside.isEmpty()) {
            throw new KnowledgeValidationException("The catalog holds " + CatalogTypes.NAMES.stream().sorted().toList()
                    + " only; terms of other types (count by type): " + outside + "; nothing was written");
        }
        KnowledgeScope any = KnowledgeScope.named("catalog", "catalog");
        for (Glossary.Term term : catalog.terms()) {
            GraphPointer.of(any, term.id(), 1);   // an id a pointer can carry, or the load is refused
        }
        List<Entity> entities = new ArrayList<>();
        for (Glossary.Term term : catalog.terms()) {
            Map<String, String> properties = new LinkedHashMap<>();
            properties.put("name", term.canonicalName());
            if (!term.aliases().isEmpty()) {
                properties.put("aliases", String.join("|", term.aliases()));
            }
            if (term.definition() != null) {
                properties.put("definition", term.definition());
            }
            entities.add(new Entity(term.id(), term.type(), term.canonicalName(), properties, Sensitivity.INTERNAL));
        }
        String hash = PassageIds.sha256Hex(catalog.id() + "@" + catalog.version() + entities);
        return new KnowledgeBatch("catalog:" + revision, DOCUMENT, hash, revision, Source.NONE, SCHEMA_VERSION, List.of(),
                entities, List.of(), List.of());
    }
}
