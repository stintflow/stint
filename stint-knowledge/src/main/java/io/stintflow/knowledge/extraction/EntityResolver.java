package io.stintflow.knowledge.extraction;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import io.stintflow.knowledge.EntityIds;

/**
 * Turns a mention into the id of an entity (SDD 3b.2, 4.4), before any node is created, in this order:
 * <ol>
 *   <li>the glossary id the model cited, if it exists and is of the type;</li>
 *   <li>the glossary term the normalized mention names (its name, or one of its aliases);</li>
 *   <li>an entity already in the base of that type with the same normalized name or alias;</li>
 *   <li>otherwise a <em>new</em> entity — and every entity of the type that {@link DuplicateCriterion} calls a candidate is
 *       reported beside it.</li>
 * </ol>
 * It <b>never merges by similarity</b>: only an exact normalized match resolves to something that exists. A wrong merge
 * destroys knowledge; a duplicate costs a review. Deterministic, no model.
 */
public final class EntityResolver {

    /** An entity the base already holds, as the resolver needs to see it. */
    public record Known(String id, String type, String label, List<String> aliases, String url) {
        public Known {
            aliases = aliases == null ? List.of() : List.copyOf(aliases);
        }

        public Known(String id, String type, String label) {
            this(id, type, label, List.of(), null);
        }
    }

    /** The base, as the resolver reads it. */
    public interface Existing {
        /** The entities of {@code type} whose normalized label or alias is {@code normalizedName}. */
        List<Known> exact(String type, String normalizedName);

        /** The entities of {@code type} (a bounded view is enough: it only feeds the duplicate candidates). */
        List<Known> ofType(String type);

        Existing NONE = new Existing() {
            @Override
            public List<Known> exact(String type, String normalizedName) {
                return List.of();
            }

            @Override
            public List<Known> ofType(String type) {
                return List.of();
            }
        };
    }

    public enum Kind { GLOSSARY, ALIAS, EXISTING, NEW }

    /** @param other the id of the entity it may be the same as */
    public record Candidate(String other, DuplicateCriterion.Candidate how) {
    }

    public record Resolution(Kind kind, String entityId, String label, List<Candidate> candidates) {
        public Resolution {
            candidates = List.copyOf(candidates);
        }
    }

    private final Glossary glossary;
    private final Existing existing;

    public EntityResolver(Glossary glossary, Existing existing) {
        this.glossary = Objects.requireNonNull(glossary, "glossary");
        this.existing = existing == null ? Existing.NONE : existing;
    }

    public Resolution resolve(String type, String mention, String citedGlossaryId) {
        if (citedGlossaryId != null) {
            var cited = glossary.byId(citedGlossaryId).filter(t -> t.type().equals(type));
            if (cited.isPresent()) {
                return new Resolution(Kind.GLOSSARY, cited.get().id(), cited.get().canonicalName(), List.of());
            }
        }
        var term = glossary.byName(type, mention);
        if (term.isPresent()) {
            boolean canonical = EntityIds.normalize(term.get().canonicalName()).equals(EntityIds.normalize(mention));
            return new Resolution(canonical ? Kind.GLOSSARY : Kind.ALIAS, term.get().id(), term.get().canonicalName(),
                    List.of());
        }
        List<Known> found = new ArrayList<>(existing.exact(type, EntityIds.normalize(mention)));
        found.sort(Comparator.comparing(Known::id));
        if (!found.isEmpty()) {
            return new Resolution(Kind.EXISTING, found.get(0).id(), found.get(0).label(), List.of());
        }
        String id = EntityIds.of(type, mention);
        DuplicateCriterion.Named self = new DuplicateCriterion.Named(type, mention);
        List<Candidate> candidates = new ArrayList<>();
        for (Glossary.Term other : glossary.terms()) {
            if (other.type().equals(type) && !other.id().equals(id)) {
                DuplicateCriterion.compare(self, new DuplicateCriterion.Named(type, other.canonicalName(), other.aliases(), null))
                        .ifPresent(how -> candidates.add(new Candidate(other.id(), how)));
            }
        }
        for (Known other : existing.ofType(type)) {
            if (!other.id().equals(id)) {
                DuplicateCriterion.compare(self, new DuplicateCriterion.Named(type, other.label(), other.aliases(), other.url()))
                        .ifPresent(how -> candidates.add(new Candidate(other.id(), how)));
            }
        }
        candidates.sort(Comparator.comparingDouble((Candidate c) -> c.how().score()).reversed()
                .thenComparing(Candidate::other));
        return new Resolution(Kind.NEW, id, mention, candidates);
    }
}
