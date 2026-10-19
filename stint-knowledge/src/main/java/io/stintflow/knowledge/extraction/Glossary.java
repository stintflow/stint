package io.stintflow.knowledge.extraction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.stintflow.knowledge.EntityIds;
import io.stintflow.knowledge.KnowledgeValidationException;

/**
 * The canonical terms of a domain (SDD 3b.2, 4.4): the systems and terms a base names the same way every time. A mention is
 * resolved to a term by its normalized name or alias, never by similarity; the prompt carries only the terms relevant to the
 * text ({@link GlossarySelector}), never the whole glossary.
 */
public final class Glossary {

    /**
     * @param id            the id the entity gets in the base
     * @param type          the node type (a glossary type: {@code System}, {@code Term})
     * @param canonicalName the name, in English
     * @param aliases       other spellings, at most {@value #MAX_ALIASES}
     * @param definition    a short line, at most {@value #MAX_DEFINITION} characters, or {@code null}
     */
    public record Term(String id, String type, String canonicalName, List<String> aliases, String definition) {
        public Term {
            aliases = aliases == null ? List.of() : List.copyOf(aliases);
            if (aliases.size() > MAX_ALIASES) {
                throw new KnowledgeValidationException("A glossary term has at most " + MAX_ALIASES + " aliases");
            }
            if (definition != null && definition.length() > MAX_DEFINITION) {
                throw new KnowledgeValidationException("A glossary definition is at most " + MAX_DEFINITION + " characters");
            }
        }

        public Term(String id, String type, String canonicalName) {
            this(id, type, canonicalName, List.of(), null);
        }
    }

    public static final int MAX_ALIASES = 8;
    public static final int MAX_DEFINITION = 80;

    private final String id;
    private final String version;
    private final List<Term> terms;
    private final Map<String, Term> byId = new HashMap<>();
    /** type + '\0' + normalized name or alias → the term */
    private final Map<String, Term> byName = new HashMap<>();

    public Glossary(String id, String version, List<Term> terms) {
        this.id = id;
        this.version = version;
        this.terms = List.copyOf(terms);
        for (Term term : this.terms) {
            if (byId.putIfAbsent(term.id(), term) != null) {
                throw new KnowledgeValidationException("A glossary id is repeated");
            }
            for (String name : names(term)) {
                Term other = byName.putIfAbsent(term.type() + '\0' + name, term);
                if (other != null && other != term) {
                    throw new KnowledgeValidationException("Two glossary terms of one type share a name or alias");
                }
            }
        }
    }

    private static List<String> names(Term term) {
        List<String> all = new ArrayList<>();
        all.add(EntityIds.normalize(term.canonicalName()));
        term.aliases().forEach(alias -> all.add(EntityIds.normalize(alias)));
        return all;
    }

    public String id() {
        return id;
    }

    public String version() {
        return version;
    }

    public List<Term> terms() {
        return terms;
    }

    public Optional<Term> byId(String termId) {
        return Optional.ofNullable(byId.get(termId));
    }

    /** The term of {@code type} named {@code mention} (or one of whose aliases it is), after normalizing. */
    public Optional<Term> byName(String type, String mention) {
        return Optional.ofNullable(byName.get(type + '\0' + EntityIds.normalize(mention)));
    }
}
