package io.stintflow.knowledge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.knowledge.extraction.Glossary;
import io.stintflow.knowledge.extraction.Glossary.Term;
import io.stintflow.knowledge.inmemory.CompositeKnowledgeStore;
import io.stintflow.knowledge.schema.KnowledgeSchema;
import io.stintflow.knowledge.schema.SchemaRegistry;
import io.stintflow.spi.Sensitivity;

/** SDD 3c.3, CA7 and CA8: what loads into the catalog, and what a reload moves. Synthetic names only. */
class CatalogSeedTest {

    static final KnowledgeScope CATALOG = KnowledgeScope.named("examples", "catalog");

    @TempDir
    Path blobs;

    private CompositeKnowledgeStore store() {
        return new CompositeKnowledgeStore(new DeterministicEmbedder(), new FilesystemBlobStore(blobs, () -> Instant.parse("2026-10-22T12:00:00Z")),
                null, () -> Instant.parse("2026-10-22T12:00:00Z"));
    }

    private static Glossary glossary(Term... terms) {
        return new Glossary("catalog", "1", List.of(terms));
    }

    private static Term checkout(String... aliases) {
        return new Term("sys-checkout", "System", "Checkout Service", List.of(aliases), "takes the orders");
    }

    private static final Term SEARCH = new Term("sys-search", "System", "Search Cluster", List.of("search service"), null);

    private OptionalLong version(CompositeKnowledgeStore store, String id) {
        return GraphPointers.versionOf(store.changes(CATALOG, 0, 1000).toCompletableFuture().join(), id);
    }

    @Test
    void the_catalog_is_internal_systems_products_and_teams_found_by_name_or_alias() {
        CompositeKnowledgeStore store = store();
        store.write(CATALOG, CatalogSeed.of(glossary(checkout("checkout", "checkout svc"), SEARCH,
                new Term("prod-shop", "Product", "Shop"), new Term("team-pay", "Team", "Payments Team")), 1)).toCompletableFuture().join();

        var byAlias = store.entities(CATALOG, new EntityQuery("System", EntityIds.normalize("Checkout SVC"), Sensitivity.INTERNAL, 5))
                .toCompletableFuture().join();

        assertThat(byAlias).singleElement().satisfies(m -> {
            assertThat(m.entityId()).isEqualTo("sys-checkout");
            assertThat(m.sensitivity()).isEqualTo(Sensitivity.INTERNAL);
        });
        assertThat(store.entities(CATALOG, new EntityQuery("Team", EntityIds.normalize("payments team"), Sensitivity.INTERNAL, 5))
                .toCompletableFuture().join()).hasSize(1);
    }

    @Test
    void ca7_a_person_in_the_file_refuses_the_whole_load_and_the_message_has_the_count_not_the_name() {
        Glossary withPerson = glossary(checkout(), new Term("p-1", "Person", "SYNTH-PII-Ana Souza"));

        assertThatThrownBy(() -> CatalogSeed.of(withPerson, 1)).isInstanceOf(KnowledgeValidationException.class)
                .hasMessageContaining("Person=1").hasMessageNotContaining("SYNTH-PII").hasMessageNotContaining("Souza");
    }

    @Test
    void ca7_the_glossary_refuses_a_repeated_id_and_a_name_two_terms_of_one_type_share() {
        assertThatThrownBy(() -> glossary(checkout(), new Term("sys-checkout", "System", "Other")))
                .isInstanceOf(KnowledgeValidationException.class);
        assertThatThrownBy(() -> glossary(checkout("search service"), SEARCH)).isInstanceOf(KnowledgeValidationException.class);
    }

    @Test
    void ca7_an_id_a_pointer_cannot_carry_refuses_the_load() {
        assertThatThrownBy(() -> CatalogSeed.of(glossary(new Term("Sys Checkout!", "System", "Checkout")), 1))
                .isInstanceOf(KnowledgeValidationException.class);
        assertThatThrownBy(() -> CatalogSeed.of(glossary(checkout()), 0)).isInstanceOf(KnowledgeValidationException.class);
    }

    @Test
    void ca8_the_same_revision_writes_nothing_and_a_changed_entry_moves_only_its_own_version() {
        CompositeKnowledgeStore store = store();
        store.write(CATALOG, CatalogSeed.of(glossary(checkout("checkout"), SEARCH), 1)).toCompletableFuture().join();
        long checkoutV1 = version(store, "sys-checkout").orElseThrow();
        long searchV1 = version(store, "sys-search").orElseThrow();
        long last = store.changes(CATALOG, 0, 1000).toCompletableFuture().join().getLast().sequence();

        store.write(CATALOG, CatalogSeed.of(glossary(checkout("checkout"), SEARCH), 1)).toCompletableFuture().join();
        assertThat(store.changes(CATALOG, 0, 1000).toCompletableFuture().join().getLast().sequence()).isEqualTo(last);

        store.write(CATALOG, CatalogSeed.of(glossary(checkout("checkout", "checkout-service"), SEARCH), 2)).toCompletableFuture().join();

        assertThat(version(store, "sys-checkout").orElseThrow()).isGreaterThan(checkoutV1);
        assertThat(version(store, "sys-search").orElseThrow()).isEqualTo(searchV1);
    }

    @Test
    void ca9_an_entry_taken_out_of_the_file_is_removed_and_its_id_is_gone_from_the_log_as_removed() {
        CompositeKnowledgeStore store = store();
        store.write(CATALOG, CatalogSeed.of(glossary(checkout(), SEARCH), 1)).toCompletableFuture().join();

        store.write(CATALOG, CatalogSeed.of(glossary(checkout()), 2)).toCompletableFuture().join();

        assertThat(version(store, "sys-search")).isEmpty();
        assertThat(version(store, "sys-checkout")).isPresent();
    }

    @Test
    void the_two_new_schemas_are_in_the_registry_and_the_older_ones_keep_their_versions() {
        SchemaRegistry registry = SchemaRegistry.withReferenceSchemas();

        assertThat(registry.resolve("catalog@1")).get().extracting(KnowledgeSchema::fullVersion).isEqualTo("catalog@1.0.0");
        assertThat(registry.resolve("catalog-links@1")).get().extracting(KnowledgeSchema::fullVersion).isEqualTo("catalog-links@1.0.0");
        assertThat(registry.resolve("meetings@1")).get().extracting(KnowledgeSchema::fullVersion).isEqualTo("meetings@1.0.0");
        assertThat(KnowledgeSchema.catalog().nodes()).containsOnlyKeys("System", "Product", "Team");
        assertThat(KnowledgeSchema.catalogLinks().relations()).containsOnlyKeys("denotes");
        assertThat(Files.exists(blobs)).isTrue();
    }
}
