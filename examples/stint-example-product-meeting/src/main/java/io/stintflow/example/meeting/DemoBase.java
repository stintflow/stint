package io.stintflow.example.meeting;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.knowledge.DeleteRequest;
import io.stintflow.knowledge.DeterministicEmbedder;
import io.stintflow.knowledge.Entity;
import io.stintflow.knowledge.EntityIds;
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
 * Mock data for looking at the inspector (SDD 3b.6): a base with two weeks of history written straight to the store, so the page has
 * something to show — several chains, five node types, restricted people (masked), confidential teams, names that are almost the same
 * (for the duplicates tab), the erasure of a person and of a document. Deterministic (the same base every time) and synthetic: no
 * name here stands for anyone.
 */
final class DemoBase {

    static final KnowledgeScope SCOPE = KnowledgeScope.named("examples", "demo-history");

    private record Item(String type, String name, Sensitivity level) {
        String id() {
            return EntityIds.of(type, name);
        }
    }

    private static final List<String> CHAINS = List.of("chain-planning", "chain-incident", "chain-roadmap", "chain-security",
            "chain-support", "chain-finance");
    private static final String[] RELATIONS = {"affects", "mentions", "owns", "decided"};

    /** In the order they first show up in the meetings: the near-duplicates come late, as they do in real bases. */
    private static final List<Item> UNIVERSE = List.of(
            new Item("System", "Checkout Service", Sensitivity.INTERNAL), new Item("System", "Payments API", Sensitivity.INTERNAL),
            new Item("Team", "Platform Team", Sensitivity.CONFIDENTIAL), new Item("Person", "SYNTH-PII-0501", Sensitivity.RESTRICTED),
            new Item("Decision", "Move checkout to the new gateway", Sensitivity.INTERNAL),
            new Item("System", "Billing Service", Sensitivity.INTERNAL), new Item("Risk", "Gateway latency under load", Sensitivity.INTERNAL),
            new Item("Person", "SYNTH-PII-0502", Sensitivity.RESTRICTED), new Item("Term", "Idempotency key", Sensitivity.INTERNAL),
            new Item("System", "Notification Hub", Sensitivity.INTERNAL), new Item("Team", "Payments Team", Sensitivity.CONFIDENTIAL),
            new Item("Decision", "Freeze releases on Fridays", Sensitivity.INTERNAL), new Item("Person", "SYNTH-PII-0503", Sensitivity.RESTRICTED),
            new Item("Risk", "Single point of failure in the queue", Sensitivity.INTERNAL), new Item("System", "Identity Provider", Sensitivity.INTERNAL),
            new Item("Term", "Dead letter queue", Sensitivity.INTERNAL), new Item("System", "Reporting Pipeline", Sensitivity.INTERNAL),
            new Item("Team", "Security Team", Sensitivity.CONFIDENTIAL), new Item("Decision", "Rotate keys every 90 days", Sensitivity.INTERNAL),
            new Item("Person", "SYNTH-PII-0504", Sensitivity.RESTRICTED), new Item("Risk", "Expired certificate on the gateway", Sensitivity.INTERNAL),
            new Item("System", "Payment API", Sensitivity.INTERNAL),              // almost "Payments API"
            new Item("System", "Checkout Services", Sensitivity.INTERNAL),        // almost "Checkout Service"
            new Item("System", "Billing", Sensitivity.INTERNAL),                  // the start of "Billing Service"
            new Item("Team", "Platform Teams", Sensitivity.CONFIDENTIAL), new Item("Term", "Idempotency keys", Sensitivity.INTERNAL),
            new Item("System", "Search Index", Sensitivity.INTERNAL), new Item("Decision", "Adopt the shared design system", Sensitivity.INTERNAL),
            new Item("Person", "SYNTH-PII-0505", Sensitivity.RESTRICTED), new Item("Risk", "Vendor lock-in on the queue", Sensitivity.INTERNAL),
            new Item("System", "Release 12", Sensitivity.INTERNAL), new Item("System", "Release 13", Sensitivity.INTERNAL));

    private DemoBase() {
    }

    static CompositeKnowledgeStore build(Path dir) throws Exception {
        AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-10-01T09:00:00Z"));
        CompositeKnowledgeStore store = new CompositeKnowledgeStore(new DeterministicEmbedder(),
                new FilesystemBlobStore(Files.createDirectories(dir.resolve("demo-blobs")), now::get),
                new RetentionClass("restricted", Duration.ofDays(30)), now::get);
        Random random = new Random(42);
        List<String> documents = new ArrayList<>();
        int meeting = 0;
        for (int day = 0; day < 14; day++) {
            int available = Math.min(UNIVERSE.size(), 6 + day * 3);
            int meetings = 1 + random.nextInt(3);
            for (int m = 0; m < meetings; m++) {
                now.set(Instant.parse("2026-10-01T09:00:00Z").plus(Duration.ofDays(day)).plus(Duration.ofHours(m * 2L)));
                String chain = CHAINS.get(random.nextInt(CHAINS.size()));
                String document = "meeting:demo-" + String.format("%03d", ++meeting);
                documents.add(document);
                write(store, document, chain, pick(random, available));
            }
            if (day == 9) {   // someone asks to be forgotten
                now.set(now.get().plus(Duration.ofHours(6)));
                store.delete(SCOPE, new DeleteRequest.ByPerson("demo-erase-person", "erasure:person",
                        EntityIds.of("Person", "SYNTH-PII-0501"), null)).toCompletableFuture().get(10, TimeUnit.SECONDS);
            }
            if (day == 12) {   // a meeting is withdrawn
                now.set(now.get().plus(Duration.ofHours(6)));
                store.delete(SCOPE, new DeleteRequest.ByDocument("demo-erase-document", "erasure:document", documents.get(4)))
                        .toCompletableFuture().get(10, TimeUnit.SECONDS);
            }
        }
        return store;
    }

    private static List<Item> pick(Random random, int available) {
        Set<Item> chosen = new LinkedHashSet<>();
        int wanted = 3 + random.nextInt(4);
        while (chosen.size() < Math.min(wanted, available)) {
            // lean towards what came in recently, so the base keeps growing and still reuses what it knows
            int index = random.nextInt(4) == 0 ? random.nextInt(available) : Math.max(0, available - 1 - random.nextInt(Math.min(available, 9)));
            chosen.add(UNIVERSE.get(index));
        }
        return new ArrayList<>(chosen);
    }

    private static void write(CompositeKnowledgeStore store, String document, String chain, List<Item> items) throws Exception {
        String text = "Notes of " + document + ": " + items.size() + " topics were discussed and agreed.";
        Passage passage = new Passage(PassageIds.of(SCOPE, document, 0, text), 0, text, "en", Sensitivity.INTERNAL);
        List<Entity> entities = new ArrayList<>();
        List<Link> links = new ArrayList<>();
        for (Item item : items) {
            entities.add(new Entity(item.id(), item.type(), item.name(), item.level()));
            links.add(new Link(passage.passageId(), item.id()));
        }
        List<Relation> relations = new ArrayList<>();
        for (int i = 0; i + 1 < items.size(); i++) {
            Item from = items.get(i);
            Item to = items.get(i + 1);
            Sensitivity level = from.level().compareTo(to.level()) >= 0 ? from.level() : to.level();
            relations.add(new Relation(RELATIONS[(from.name().length() + i) % RELATIONS.length], from.id(), to.id(), level));
        }
        String hash = PassageIds.sha256Hex(text + entities + relations);
        store.write(SCOPE, new KnowledgeBatch("batch-" + document, document, hash, 1L,
                new Source("inst-" + document.substring(document.length() - 3), chain, new WorkflowRef("meetings", "notes", "1.0.0")),
                "meetings@1.0.0", List.of(passage), entities, relations, links)).toCompletableFuture().get(10, TimeUnit.SECONDS);
    }
}
