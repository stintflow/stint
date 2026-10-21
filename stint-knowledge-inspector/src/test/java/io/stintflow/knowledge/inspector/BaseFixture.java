package io.stintflow.knowledge.inspector;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.knowledge.DeterministicEmbedder;
import io.stintflow.knowledge.Entity;
import io.stintflow.knowledge.KnowledgeBatch;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.Link;
import io.stintflow.knowledge.Passage;
import io.stintflow.knowledge.PassageIds;
import io.stintflow.knowledge.Relation;
import io.stintflow.knowledge.Source;
import io.stintflow.knowledge.inmemory.CompositeKnowledgeStore;
import io.stintflow.spi.RetentionClass;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.WorkflowRef;

/**
 * A small base with a history, for the tests of SDD 3b.6: three days, two chains, one restricted person and one erasure. Synthetic
 * data only. The clock is the test's.
 */
final class BaseFixture {

    static final KnowledgeScope SCOPE = KnowledgeScope.of(new WorkflowRef("meetings", "notes", "1.0.0"));
    static final String PERSON_NAME = "SYNTH-PII-0201";
    static final String PERSON_ID = "e-person";

    final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-10-01T10:00:00Z"));
    final CompositeKnowledgeStore store;

    BaseFixture(Path dir) throws Exception {
        store = new CompositeKnowledgeStore(new DeterministicEmbedder(), new FilesystemBlobStore(Files.createTempDirectory(dir, "b"),
                now::get), new RetentionClass("restricted", Duration.ofDays(30)), now::get);
    }

    /** Day 1: chain-A writes meeting m-1. Day 2: chain-B writes m-2. Day 3: the person is erased. */
    static BaseFixture withHistory(Path dir) throws Exception {
        BaseFixture f = new BaseFixture(dir);
        f.write("meeting:m-1", "chain-A", "inst-1", 1L, f.entities(
                new Entity("e-checkout", "System", "Checkout Service", Sensitivity.INTERNAL),
                new Entity("e-payments", "System", "Payments API", Sensitivity.INTERNAL),
                new Entity(PERSON_ID, "Person", PERSON_NAME, Sensitivity.RESTRICTED)),
                List.of(new Relation("affects", "e-checkout", "e-payments", Sensitivity.INTERNAL),
                        new Relation("owns", PERSON_ID, "e-checkout", Sensitivity.RESTRICTED)));
        f.now.set(Instant.parse("2026-10-02T10:00:00Z"));
        f.write("meeting:m-2", "chain-B", "inst-2", 1L, f.entities(
                new Entity("e-checkout", "System", "Checkout Service", Sensitivity.INTERNAL),
                new Entity("e-billing", "System", "Billing Service", Sensitivity.INTERNAL)),
                List.of(new Relation("affects", "e-billing", "e-checkout", Sensitivity.INTERNAL)));
        f.now.set(Instant.parse("2026-10-03T10:00:00Z"));
        f.store.delete(SCOPE, new io.stintflow.knowledge.DeleteRequest.ByPerson("er-1", "erasure:person", PERSON_ID, null))
                .toCompletableFuture().get(10, TimeUnit.SECONDS);
        return f;
    }

    List<Entity> entities(Entity... entities) {
        return List.of(entities);
    }

    void write(String document, String chain, String instance, Long revision, List<Entity> entities, List<Relation> relations)
            throws Exception {
        String text = "Text of " + document + " revision " + revision + ".";
        Passage passage = new Passage(PassageIds.of(SCOPE, document, 0, text), 0, text, "en", Sensitivity.INTERNAL);
        List<Link> links = new ArrayList<>();
        entities.forEach(e -> links.add(new Link(passage.passageId(), e.entityId())));
        String hash = PassageIds.sha256Hex(text + entities + relations);
        store.write(SCOPE, new KnowledgeBatch("batch-" + document + "-" + revision, document, hash, revision,
                new Source(instance, chain, new WorkflowRef("meetings", "notes", "1.0.0")), "meetings@1.0.0", List.of(passage),
                entities, relations, links)).toCompletableFuture().get(10, TimeUnit.SECONDS);
    }
}
