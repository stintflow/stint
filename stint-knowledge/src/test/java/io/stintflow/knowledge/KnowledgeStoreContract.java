package io.stintflow.knowledge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.stintflow.knowledge.ChangeEntry.Kind;
import io.stintflow.knowledge.ChangeEntry.Op;
import io.stintflow.knowledge.WriteReceipt.Status;
import io.stintflow.spi.ModelProvider;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.WorkflowRef;

/**
 * SDD 3b.1, CA15: what any {@link KnowledgeStore} must do, through the port alone. A backend extends this class and says how to
 * build itself, how to move its clock and what it holds at rest; the in-memory composite is the first to pass it. Data is
 * synthetic: {@code SYNTH-PII-*} markers stand for whatever must not rest in the clear.
 */
public abstract class KnowledgeStoreContract {

    protected static final KnowledgeScope SCOPE = KnowledgeScope.of(new WorkflowRef("meetings", "notes", "1.0.0"));
    protected static final String SCHEMA = "meetings@1.0.0";

    /** A store with no model behind its embedder; {@code withRestrictedClass} gives it a retention class for restricted content. */
    protected abstract KnowledgeStore newStore(boolean withRestrictedClass);

    /** Moves the store's clock (what a blob's retention is measured against). */
    protected abstract void advanceClock(Duration by);

    /** Everything the store holds for {@code scope} as text outside the vault of restricted content — its structures — for the scanner. */
    protected abstract String heldOutsideBlobs(KnowledgeScope scope);

    /** Everything in the blobs of restricted content, as text. */
    protected abstract String heldInBlobs();

    /** Removes what has expired from the index, if the backend keeps it apart from the blobs; returns how many things. */
    protected abstract int reapExpired(KnowledgeScope scope);

    /** Puts a blob in the store's blob store (an ata, a batch) and returns its pointer. */
    protected abstract URI putBlob(String content);

    /** Whether the blob is still there. */
    protected abstract boolean blobExists(URI uri);

    // --- a small way to build batches ----------------------------------------------------------------------------

    protected static final class B {
        final KnowledgeScope scope;
        final String doc;
        final List<Passage> passages = new ArrayList<>();
        final List<Entity> entities = new ArrayList<>();
        final List<Relation> relations = new ArrayList<>();
        final List<Link> links = new ArrayList<>();
        Long revision;
        final List<URI> derivedFrom = new ArrayList<>();
        String batchId;
        Source source = new Source("inst-1", "chain-1", new WorkflowRef("meetings", "notes", "1.0.0"));

        B(KnowledgeScope scope, String doc) {
            this.scope = scope;
            this.doc = doc;
            this.batchId = "batch-" + doc;
        }

        B passage(String text, Sensitivity level) {
            int ordinal = passages.size();
            passages.add(new Passage(PassageIds.of(scope, doc, ordinal, text), ordinal, text, "en", level));
            return this;
        }

        String idOf(int ordinal) {
            return passages.get(ordinal).passageId();
        }

        B entity(String id, String type, String label, Sensitivity level) {
            entities.add(new Entity(id, type, label, level));
            return this;
        }

        B relation(String type, String from, String to, Sensitivity level) {
            relations.add(new Relation(type, from, to, level));
            return this;
        }

        B link(int ordinal, String entityId) {
            links.add(new Link(idOf(ordinal), entityId));
            return this;
        }

        B revision(long revision) {
            this.revision = revision;
            return this;
        }

        B derivedFrom(URI... uris) {
            derivedFrom.addAll(List.of(uris));
            return this;
        }

        B batchId(String id) {
            this.batchId = id;
            return this;
        }

        KnowledgeBatch build() {
            String hash = PassageIds.sha256Hex(passages + "|" + entities + "|" + relations + "|" + links);
            return new KnowledgeBatch(batchId, doc, hash, revision, source, SCHEMA, passages, entities, relations, links,
                    derivedFrom);
        }
    }

    protected static WriteReceipt write(KnowledgeStore store, KnowledgeScope scope, KnowledgeBatch batch) throws Exception {
        return store.write(scope, batch).toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    protected static List<ContextItem> retrieve(KnowledgeStore store, KnowledgeScope scope, String text, Sensitivity ceiling)
            throws Exception {
        return store.retrieve(scope, new RetrievalQuery(text, 10, ceiling)).toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    protected static List<ChangeEntry> log(KnowledgeStore store, KnowledgeScope scope) throws Exception {
        return store.changes(scope, 0, 1000).toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    protected static DeleteReceipt erase(KnowledgeStore store, KnowledgeScope scope, DeleteRequest request) throws Exception {
        return store.delete(scope, request).toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    private B checkout() {
        return new B(SCOPE, "meeting:m-001")
                .passage("The checkout service retries a failed payment three times.", Sensitivity.INTERNAL)
                .passage("We agreed to move the checkout copy review to Friday.", Sensitivity.INTERNAL)
                .entity("e-checkout", "System", "Checkout Service", Sensitivity.INTERNAL)
                .entity("e-payments", "System", "Payments API", Sensitivity.INTERNAL)
                .relation("affects", "e-checkout", "e-payments", Sensitivity.INTERNAL)
                .link(0, "e-checkout").link(0, "e-payments").link(1, "e-checkout");
    }

    // --- CA1: write and read ----------------------------------------------------------------------------------------

    @Test
    protected void ca1_a_written_document_is_retrieved_with_its_origin_score_entities_and_level() throws Exception {
        KnowledgeStore store = newStore(false);
        WriteReceipt receipt = write(store, SCOPE, checkout().build());
        assertThat(receipt.status()).isEqualTo(Status.APPLIED);

        List<ContextItem> items = retrieve(store, SCOPE, "checkout payment retry", Sensitivity.INTERNAL);

        assertThat(items).isNotEmpty();
        ContextItem best = items.get(0);
        assertThat(best.content()).contains("retries a failed payment");
        assertThat(best.origin().kind()).isEqualTo(ContextItem.Kind.PASSAGE);
        assertThat(best.origin().documentId()).isEqualTo("meeting:m-001");
        assertThat(best.origin().instanceId()).isEqualTo("inst-1");
        assertThat(best.origin().chainId()).isEqualTo("chain-1");
        assertThat(best.score()).isBetween(0.0, 1.0);
        assertThat(best.sensitivity()).isEqualTo(Sensitivity.INTERNAL);
        assertThat(best.entities()).extracting(ContextItem.EntityRef::label)
                .containsExactlyInAnyOrder("Checkout Service", "Payments API");
        assertThat(items).extracting(ContextItem::score).isSortedAccordingTo(java.util.Comparator.reverseOrder());
        assertThat(retrieve(store, SCOPE, "checkout payment retry", Sensitivity.INTERNAL))
                .isEqualTo(items);   // deterministic
    }

    @Test
    protected void ca1_the_graph_adds_the_entities_and_relations_one_hop_away() throws Exception {
        KnowledgeStore store = newStore(false);
        write(store, SCOPE, checkout().build());

        List<ContextItem> items = store.retrieve(SCOPE, new RetrievalQuery("checkout payment retry", 5, 0,
                Sensitivity.INTERNAL, null, Set.of(), null, 1)).toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertThat(items).extracting(i -> i.origin().kind()).contains(ContextItem.Kind.ENTITY, ContextItem.Kind.RELATION);
        assertThat(items).filteredOn(i -> i.origin().kind() == ContextItem.Kind.RELATION).singleElement()
                .satisfies(i -> assertThat(i.content()).isEqualTo("Checkout Service -[affects]-> Payments API"));
    }

    // --- CA2: scopes ------------------------------------------------------------------------------------------------

    @Test
    protected void ca2_the_scope_of_a_definition_ignores_its_version_and_scopes_do_not_leak() throws Exception {
        assertThat(KnowledgeScope.of(new WorkflowRef("meetings", "notes", "1.0.0")))
                .isEqualTo(KnowledgeScope.of(new WorkflowRef("meetings", "notes", "2.0.0")));
        assertThat(KnowledgeScope.of(new WorkflowRef("meetings", "notes", "1.0.0")))
                .isNotEqualTo(KnowledgeScope.named("meetings", "shared"));

        KnowledgeStore store = newStore(false);
        write(store, SCOPE, checkout().build());
        KnowledgeScope other = KnowledgeScope.named("meetings", "shared");

        assertThat(retrieve(store, other, "checkout payment retry", Sensitivity.RESTRICTED)).isEmpty();
        assertThat(log(store, other)).isEmpty();
        assertThat(retrieve(store, KnowledgeScope.of(new WorkflowRef("meetings", "notes", "2.0.0")), "checkout payment",
                Sensitivity.INTERNAL)).isNotEmpty();   // a new version of the definition sees the same base
    }

    // --- CA3: the ceiling -------------------------------------------------------------------------------------------

    @Test
    protected void ca3_nothing_above_the_ceiling_comes_back_and_nothing_tells_it_is_there() throws Exception {
        KnowledgeStore withSecret = newStore(true);
        KnowledgeStore without = newStore(true);
        B open = new B(SCOPE, "meeting:open").passage("The roadmap review moved to Thursday.", Sensitivity.INTERNAL)
                .entity("e-roadmap", "Term", "Roadmap", Sensitivity.INTERNAL).link(0, "e-roadmap");
        B secret = new B(SCOPE, "meeting:secret").passage("The roadmap review has a SYNTH-PII-0001 budget cut.",
                        Sensitivity.CONFIDENTIAL)
                .passage("The roadmap review names SYNTH-PII-0002 as lead.", Sensitivity.RESTRICTED)
                .entity("e-budget", "Term", "Budget", Sensitivity.CONFIDENTIAL)
                .entity("e-lead", "Person", "PERSON_a1", Sensitivity.RESTRICTED)
                .relation("mentions", "e-budget", "e-roadmap", Sensitivity.CONFIDENTIAL)
                .link(0, "e-budget").link(0, "e-roadmap").link(1, "e-lead");
        write(withSecret, SCOPE, open.build());
        write(withSecret, SCOPE, secret.build());
        write(without, SCOPE, open.build());

        for (Sensitivity ceiling : new Sensitivity[] {Sensitivity.INTERNAL, Sensitivity.CONFIDENTIAL}) {
            RetrievalQuery query = new RetrievalQuery("roadmap review", 10, 0, ceiling, null, Set.of(), null, 1);
            List<ContextItem> a = withSecret.retrieve(SCOPE, query).toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertThat(a).allSatisfy(i -> assertThat(i.sensitivity().atLeast(ceiling) && i.sensitivity() != ceiling)
                    .as("%s above %s", i.content(), ceiling).isFalse());
            assertThat(a).noneMatch(i -> i.content().contains("SYNTH-PII-0002"));
            if (ceiling == Sensitivity.INTERNAL) {
                // the same answer, to the score and the order, as a base that never had the other documents
                List<ContextItem> b = without.retrieve(SCOPE, query).toCompletableFuture().get(10, TimeUnit.SECONDS);
                assertThat(a).extracting(i -> i.origin().id()).containsExactlyElementsOf(
                        b.stream().map(i -> i.origin().id()).toList());
                assertThat(a).extracting(ContextItem::score).containsExactlyElementsOf(b.stream().map(ContextItem::score).toList());
            }
        }
        assertThat(retrieve(withSecret, SCOPE, "roadmap review", Sensitivity.RESTRICTED))
                .anyMatch(i -> i.content().contains("SYNTH-PII-0002"));
        assertThat(withSecret.entities(SCOPE, new EntityQuery(null, EntityIds.normalize("PERSON_a1"), Sensitivity.CONFIDENTIAL, 5))
                .toCompletableFuture().get(10, TimeUnit.SECONDS)).isEmpty();
        assertThat(withSecret.entities(SCOPE, new EntityQuery(null, EntityIds.normalize("PERSON_a1"), Sensitivity.RESTRICTED, 5))
                .toCompletableFuture().get(10, TimeUnit.SECONDS)).hasSize(1);
    }

    @Test
    protected void ca3_the_ceiling_is_the_lower_of_the_instance_and_the_provider() {
        ModelProvider local = new ModelProvider("litjev", "q", false);
        ModelProvider approved = new ModelProvider("enterprise", "m", true);
        ModelProvider other = new ModelProvider("public", "m", true);

        assertThat(Audience.ceiling(Sensitivity.RESTRICTED, local, false)).isEqualTo(Sensitivity.RESTRICTED);
        assertThat(Audience.ceiling(Sensitivity.INTERNAL, local, false)).isEqualTo(Sensitivity.INTERNAL);
        assertThat(Audience.ceiling(Sensitivity.RESTRICTED, approved, true)).isEqualTo(Sensitivity.CONFIDENTIAL);
        assertThat(Audience.ceiling(Sensitivity.CONFIDENTIAL, other, false)).isEqualTo(Sensitivity.INTERNAL);
        assertThat(Audience.ceiling(null, null, false)).isEqualTo(Sensitivity.RESTRICTED);
        assertThatThrownBy(() -> new RetrievalQuery("x", 5, 0, null, null, Set.of(), null, 0))
                .isInstanceOf(NullPointerException.class);
    }

    // --- CA4 and CA5: restricted content ---------------------------------------------------------------------------

    @Test
    protected void ca4_restricted_content_rests_only_in_blobs_and_a_store_with_no_class_refuses_it() throws Exception {
        KnowledgeStore noClass = newStore(false);
        B restricted = new B(SCOPE, "meeting:r1").passage("Cut the vendor, says SYNTH-PII-0101 Rivera.", Sensitivity.RESTRICTED)
                .entity("e-vendor", "Decision", "Drop SYNTH-PII-0102 vendor", Sensitivity.RESTRICTED).link(0, "e-vendor");
        assertThatThrownBy(() -> write(noClass, SCOPE, restricted.build())).isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(KnowledgeValidationException.class).hasMessageContaining("retention class")
                .hasMessageNotContaining("SYNTH-PII");

        KnowledgeStore store = newStore(true);
        assertThat(write(store, SCOPE, restricted.build()).status()).isEqualTo(Status.APPLIED);

        assertThat(heldOutsideBlobs(SCOPE)).as("restricted text in the index, the graph or the log")
                .doesNotContain("SYNTH-PII-0101").doesNotContain("SYNTH-PII-0102");
        assertThat(heldInBlobs()).contains("SYNTH-PII-0101").contains("SYNTH-PII-0102");   // it rests in the vault
        assertThat(retrieve(store, SCOPE, "vendor cut Rivera", Sensitivity.INTERNAL)).isEmpty();
        assertThat(retrieve(store, SCOPE, "vendor cut Rivera", Sensitivity.RESTRICTED)).singleElement()
                .satisfies(i -> assertThat(i.content()).contains("SYNTH-PII-0101"));
        assertThat(log(store, SCOPE)).noneMatch(e -> e.contentHash() != null && e.sensitivity() == Sensitivity.RESTRICTED);
    }

    @Test
    protected void ca5_an_expired_blob_is_left_out_of_a_retrieval_and_reaping_logs_the_expiry() throws Exception {
        KnowledgeStore store = newStore(true);
        write(store, SCOPE, new B(SCOPE, "meeting:r2").passage("Plan to relocate, says SYNTH-PII-0201.", Sensitivity.RESTRICTED)
                .passage("The relocation is internal news.", Sensitivity.INTERNAL).build());
        assertThat(retrieve(store, SCOPE, "relocation plan", Sensitivity.RESTRICTED)).hasSize(2);

        advanceClock(Duration.ofDays(31));

        List<ContextItem> after = retrieve(store, SCOPE, "relocation plan", Sensitivity.RESTRICTED);
        assertThat(after).singleElement().satisfies(i -> assertThat(i.content()).contains("internal news"));
        assertThat(reapExpired(SCOPE)).isEqualTo(1);
        assertThat(log(store, SCOPE)).anySatisfy(e -> {
            assertThat(e.op()).isEqualTo(Op.EXPIRE);
            assertThat(e.kind()).isEqualTo(Kind.PASSAGE);
            assertThat(e.reason()).isEqualTo("retention");
        });
    }

    // --- CA7: the log -----------------------------------------------------------------------------------------------

    @Test
    protected void ca7_the_log_has_no_gap_no_text_and_says_when_who_and_under_which_schema() throws Exception {
        KnowledgeStore store = newStore(false);
        write(store, SCOPE, checkout().build());

        List<ChangeEntry> entries = log(store, SCOPE);

        assertThat(entries).isNotEmpty();
        for (int i = 0; i < entries.size(); i++) {
            ChangeEntry e = entries.get(i);
            assertThat(e.sequence()).isEqualTo(i + 1);
            assertThat(e.at()).isNotNull();
            assertThat(e.instanceId()).isEqualTo("inst-1");
            assertThat(e.chainId()).isEqualTo("chain-1");
            assertThat(e.definition()).isEqualTo("meetings:notes:1.0.0");
            assertThat(e.schemaVersion()).isEqualTo(SCHEMA);
            assertThat(e.batchId()).isEqualTo("batch-meeting:m-001");
        }
        assertThat(entries.toString()).doesNotContain("retries a failed payment").doesNotContain("Checkout Service");
        assertThat(entries).extracting(ChangeEntry::kind).contains(Kind.PASSAGE, Kind.ENTITY, Kind.RELATION, Kind.LINK);
        // SDD 3b.6 counts nodes by type over time: the log says what each thing is, without its content
        assertThat(entries).filteredOn(e -> e.kind() == Kind.ENTITY).extracting(ChangeEntry::elementType).containsOnly("System");
        assertThat(entries).filteredOn(e -> e.kind() == Kind.RELATION).extracting(ChangeEntry::elementType)
                .containsOnly("affects");
        assertThat(entries).filteredOn(e -> e.kind() == Kind.PASSAGE).extracting(ChangeEntry::elementType)
                .containsOnly("Passage");
        assertThat(log(store, SCOPE)).isEqualTo(entries);   // the same, read again
        assertThat(store.changes(SCOPE, 3, 2).toCompletableFuture().get(10, TimeUnit.SECONDS))
                .extracting(ChangeEntry::sequence).containsExactly(4L, 5L);
    }

    // --- CA8, CA9, CA10: idempotence --------------------------------------------------------------------------------

    @Test
    protected void ca8_writing_the_same_document_again_changes_and_logs_nothing() throws Exception {
        KnowledgeStore store = newStore(false);
        assertThat(write(store, SCOPE, checkout().build()).status()).isEqualTo(Status.APPLIED);
        long last = log(store, SCOPE).size();
        List<ContextItem> before = retrieve(store, SCOPE, "checkout", Sensitivity.INTERNAL);

        WriteReceipt again = write(store, SCOPE, checkout().build());
        WriteReceipt redelivery = write(store, SCOPE, checkout().batchId("another-batch-id").build());

        assertThat(again.status()).isEqualTo(Status.UNCHANGED);
        assertThat(redelivery.status()).isEqualTo(Status.UNCHANGED);
        assertThat(log(store, SCOPE)).hasSize((int) last);
        assertThat(retrieve(store, SCOPE, "checkout", Sensitivity.INTERNAL)).isEqualTo(before);
        assertThat(retrieve(store, SCOPE, "checkout", Sensitivity.INTERNAL)).extracting(i -> i.origin().id()).doesNotHaveDuplicates();
    }

    @Test
    protected void ca8_concurrent_redeliveries_leave_one_copy() throws Exception {
        KnowledgeStore store = newStore(false);
        int writers = 8;
        CyclicBarrier go = new CyclicBarrier(writers);
        var pool = Executors.newFixedThreadPool(writers);
        List<Future<WriteReceipt>> receipts = new ArrayList<>();
        for (int i = 0; i < writers; i++) {
            receipts.add(pool.submit(() -> {
                go.await(10, TimeUnit.SECONDS);
                return write(store, SCOPE, checkout().build());
            }));
        }
        List<Status> statuses = new ArrayList<>();
        for (Future<WriteReceipt> receipt : receipts) {
            statuses.add(receipt.get(30, TimeUnit.SECONDS).status());
        }
        pool.shutdown();

        assertThat(statuses).containsOnly(Status.APPLIED, Status.UNCHANGED);
        assertThat(statuses.stream().filter(s -> s == Status.APPLIED)).hasSize(1);
        assertThat(retrieve(store, SCOPE, "checkout", Sensitivity.INTERNAL)).hasSize(2);
        assertThat(log(store, SCOPE).stream().filter(e -> e.kind() == Kind.PASSAGE)).hasSize(2);
    }

    @Test
    protected void ca9_a_changed_document_applies_only_the_difference_and_shared_facts_outlive_one_document() throws Exception {
        KnowledgeStore store = newStore(false);
        write(store, SCOPE, checkout().build());
        // a second document asserts the same system
        write(store, SCOPE, new B(SCOPE, "meeting:m-002").passage("Checkout was down for ten minutes.", Sensitivity.INTERNAL)
                .entity("e-checkout", "System", "Checkout Service", Sensitivity.INTERNAL).link(0, "e-checkout").build());
        long before = log(store, SCOPE).size();

        // the first document again, its second passage reworded and the Payments API gone
        B edited = new B(SCOPE, "meeting:m-001")
                .passage("The checkout service retries a failed payment three times.", Sensitivity.INTERNAL)
                .passage("We agreed to review the checkout copy on Monday.", Sensitivity.INTERNAL)
                .entity("e-checkout", "System", "Checkout Service", Sensitivity.INTERNAL).link(0, "e-checkout")
                .link(1, "e-checkout");
        WriteReceipt receipt = write(store, SCOPE, edited.build());

        assertThat(receipt.status()).isEqualTo(Status.APPLIED);
        List<ChangeEntry> added = log(store, SCOPE).subList((int) before, log(store, SCOPE).size());
        assertThat(added).noneMatch(e -> e.targetId().equals(edited.idOf(0)));   // the first passage kept its id: untouched
        assertThat(added).anyMatch(e -> e.op() == Op.REMOVE && e.kind() == Kind.PASSAGE);
        assertThat(added).anyMatch(e -> e.op() == Op.ADD && e.kind() == Kind.PASSAGE && e.targetId().equals(edited.idOf(1)));
        assertThat(added).anyMatch(e -> e.op() == Op.REMOVE && e.kind() == Kind.ENTITY && e.targetId().equals("e-payments"));
        assertThat(added).noneMatch(e -> e.kind() == Kind.ENTITY && e.targetId().equals("e-checkout") && e.op() == Op.REMOVE);
        assertThat(store.entities(SCOPE, new EntityQuery("System", EntityIds.normalize("checkout service"),
                Sensitivity.INTERNAL, 5)).toCompletableFuture().get(10, TimeUnit.SECONDS)).hasSize(1);
        assertThat(store.entities(SCOPE, new EntityQuery("System", EntityIds.normalize("payments api"),
                Sensitivity.INTERNAL, 5)).toCompletableFuture().get(10, TimeUnit.SECONDS)).isEmpty();

        // erasing the second document leaves the system, which the first still asserts
        erase(store, SCOPE, new DeleteRequest.ByDocument("req-1", "erasure:document", "meeting:m-002"));
        assertThat(store.entities(SCOPE, new EntityQuery("System", EntityIds.normalize("checkout service"),
                Sensitivity.INTERNAL, 5)).toCompletableFuture().get(10, TimeUnit.SECONDS)).hasSize(1);
    }

    @Test
    protected void ca10_an_older_revision_after_a_newer_one_is_stale_and_changes_nothing() throws Exception {
        KnowledgeStore store = newStore(false);
        B v2 = new B(SCOPE, "meeting:m-010").revision(2).passage("Version two of the minutes.", Sensitivity.INTERNAL);
        B v1 = new B(SCOPE, "meeting:m-010").revision(1).passage("Version one of the minutes.", Sensitivity.INTERNAL);
        write(store, SCOPE, v2.build());
        long size = log(store, SCOPE).size();

        assertThat(write(store, SCOPE, v1.build()).status()).isEqualTo(Status.STALE);

        assertThat(log(store, SCOPE)).hasSize((int) size);
        assertThat(retrieve(store, SCOPE, "minutes", Sensitivity.INTERNAL)).singleElement()
                .satisfies(i -> assertThat(i.content()).contains("Version two"));
        B conflicting = new B(SCOPE, "meeting:m-010").revision(2).passage("Another text, same revision.", Sensitivity.INTERNAL);
        assertThatThrownBy(() -> write(store, SCOPE, conflicting.build())).hasCauseInstanceOf(KnowledgeValidationException.class);
    }

    // --- CA11 and CA12: erasure -------------------------------------------------------------------------------------

    @Test
    protected void ca11_erasing_a_document_and_a_person_leaves_them_nowhere_and_the_log_keeps_only_ids() throws Exception {
        KnowledgeStore store = newStore(true);
        write(store, SCOPE, new B(SCOPE, "meeting:p1")
                .passage("PERSON_9f spoke about the SYNTH-PII-0301 contract.", Sensitivity.RESTRICTED)
                .passage("PERSON_9f joined the standup.", Sensitivity.CONFIDENTIAL)
                .passage("The build is green.", Sensitivity.INTERNAL)
                .entity("e-person", "Person", "PERSON_9f", Sensitivity.CONFIDENTIAL)
                .entity("e-build", "Term", "Build", Sensitivity.INTERNAL)
                .relation("mentions", "e-person", "e-build", Sensitivity.CONFIDENTIAL)
                .link(0, "e-person").link(1, "e-person").link(2, "e-build").build());
        write(store, SCOPE, new B(SCOPE, "meeting:p2").passage("SYNTH-PII-0302 reviewed the roadmap.", Sensitivity.RESTRICTED)
                .build());
        long before = log(store, SCOPE).size();

        // by person: the node, its edges and links, and (the default) the passages that mention it
        DeleteReceipt person = erase(store, SCOPE, new DeleteRequest.ByPerson("req-p", "erasure:person", "e-person", null));
        assertThat(person.counts()).containsKeys("ERASE.ENTITY", "ERASE.RELATION", "ERASE.LINK", "ERASE.PASSAGE");
        assertThat(retrieve(store, SCOPE, "PERSON_9f spoke standup", Sensitivity.RESTRICTED))
                .noneMatch(i -> i.content().contains("PERSON_9f"));
        assertThat(heldOutsideBlobs(SCOPE) + heldInBlobs()).doesNotContain("SYNTH-PII-0301").doesNotContain("PERSON_9f");
        assertThat(retrieve(store, SCOPE, "build green", Sensitivity.INTERNAL)).hasSize(1);   // what was not about the person stays

        // by document
        DeleteReceipt doc = erase(store, SCOPE, new DeleteRequest.ByDocument("req-d", "erasure:document", "meeting:p2"));
        assertThat(doc.counts()).containsKey("ERASE.PASSAGE");
        assertThat(heldOutsideBlobs(SCOPE) + heldInBlobs()).doesNotContain("SYNTH-PII-0302");
        assertThat(retrieve(store, SCOPE, "roadmap", Sensitivity.RESTRICTED)).isEmpty();

        List<ChangeEntry> entries = log(store, SCOPE);
        assertThat(entries.subList((int) before, entries.size())).isNotEmpty().allSatisfy(e -> {
            assertThat(e.op()).isEqualTo(Op.ERASE);
            assertThat(e.reason()).startsWith("erasure:");
        });
        assertThat(entries.toString()).doesNotContain("PERSON_9f").doesNotContain("SYNTH-PII");
        // a repeat of the request erases nothing more
        assertThat(erase(store, SCOPE, new DeleteRequest.ByPerson("req-p", "erasure:person", "e-person", null)).counts()).isEmpty();
    }

    @Test
    protected void ca11_erasing_a_person_can_keep_the_passages() throws Exception {
        KnowledgeStore store = newStore(false);
        write(store, SCOPE, new B(SCOPE, "meeting:p3").passage("PERSON_77 owns the migration.", Sensitivity.INTERNAL)
                .entity("e-owner", "Person", "PERSON_77", Sensitivity.INTERNAL).link(0, "e-owner").build());

        DeleteReceipt receipt = erase(store, SCOPE, new DeleteRequest.ByPerson("req-k", "erasure:person", "e-owner",
                DeleteRequest.PassagePolicy.NONE));

        assertThat(receipt.counts()).containsKeys("ERASE.ENTITY", "ERASE.LINK").doesNotContainKey("ERASE.PASSAGE");
        assertThat(retrieve(store, SCOPE, "migration", Sensitivity.INTERNAL)).singleElement()
                .satisfies(i -> assertThat(i.entities()).isEmpty());
    }

    @Test
    protected void ca12_a_redelivered_write_after_an_erasure_does_not_bring_the_data_back() throws Exception {
        KnowledgeStore store = newStore(false);
        B doc = new B(SCOPE, "meeting:p4").passage("Quarterly numbers for the board.", Sensitivity.INTERNAL)
                .entity("e-board", "Term", "Board", Sensitivity.INTERNAL).entity("e-who", "Person", "PERSON_5", Sensitivity.INTERNAL)
                .relation("mentions", "e-who", "e-board", Sensitivity.INTERNAL).link(0, "e-board").link(0, "e-who");
        write(store, SCOPE, doc.build());
        erase(store, SCOPE, new DeleteRequest.ByDocument("req-1", "erasure:document", "meeting:p4"));

        WriteReceipt again = write(store, SCOPE, doc.build());

        assertThat(again.status()).isEqualTo(Status.REJECTED_ERASED);
        assertThat(retrieve(store, SCOPE, "numbers board", Sensitivity.RESTRICTED)).isEmpty();

        // a person erased: another document that names her is written without her
        write(store, SCOPE, new B(SCOPE, "meeting:p5").passage("Kickoff.", Sensitivity.INTERNAL)
                .entity("e-who", "Person", "PERSON_5", Sensitivity.INTERNAL).link(0, "e-who").build());
        erase(store, SCOPE, new DeleteRequest.ByPerson("req-2", "erasure:person", "e-who", DeleteRequest.PassagePolicy.NONE));
        WriteReceipt later = write(store, SCOPE, new B(SCOPE, "meeting:p6").passage("Retro.", Sensitivity.INTERNAL)
                .entity("e-who", "Person", "PERSON_5", Sensitivity.INTERNAL).entity("e-x", "Term", "Retro", Sensitivity.INTERNAL)
                .link(0, "e-who").link(0, "e-x").build());
        assertThat(later.status()).isEqualTo(Status.APPLIED);
        assertThat(later.dropped()).isEqualTo(2);   // the entity and the link
        assertThat(store.entities(SCOPE, new EntityQuery("Person", EntityIds.normalize("PERSON_5"), Sensitivity.RESTRICTED, 5))
                .toCompletableFuture().get(10, TimeUnit.SECONDS)).isEmpty();
    }

    // --- SDD 3b.5: describe, reindex, verification and the blobs a document came from ------------------------------

    /** Another model: 16 buckets, English and Portuguese. */
    protected static final class OtherEmbedder implements Embedder {
        private final Set<String> languages;

        protected OtherEmbedder(Set<String> languages) {
            this.languages = languages;
        }

        @Override
        public float[] embed(String text) {
            float[] vector = new float[16];
            for (String word : EntityIds.normalize(text).split(" ")) {
                if (!word.isEmpty()) {
                    vector[Math.floorMod(word.hashCode(), 16)] += 1f;
                }
            }
            return vector;
        }

        @Override
        public int dimension() {
            return 16;
        }

        @Override
        public String model() {
            return "other-bag-16";
        }

        @Override
        public Set<String> languages() {
            return languages;
        }
    }

    protected static ReindexReceipt reindex(KnowledgeStore store, KnowledgeScope scope, String id, Embedder target)
            throws Exception {
        return store.reindex(scope, new ReindexRequest(id, target, "reindex:model")).toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    @Test
    protected void ca3_describe_says_the_model_the_counts_and_the_schema_versions() throws Exception {
        KnowledgeStore store = newStore(false);
        assertThat(store.describe(SCOPE).toCompletableFuture().get(10, TimeUnit.SECONDS).model()).isNull();

        write(store, SCOPE, checkout().build());
        BaseInfo info = store.describe(SCOPE).toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertThat(info.model()).isEqualTo(new DeterministicEmbedder().model());
        assertThat(info.dimension()).isEqualTo(DeterministicEmbedder.DIMENSION);
        assertThat(info).extracting(BaseInfo::documents, BaseInfo::passages, BaseInfo::entities, BaseInfo::relations)
                .containsExactly(1, 2, 2, 1);
        assertThat(info.schemaVersions()).containsExactly(SCHEMA);
    }

    @Test
    protected void ca3_a_reindexation_moves_every_vector_to_the_new_model_and_logs_it_once() throws Exception {
        KnowledgeStore store = newStore(true);
        write(store, SCOPE, checkout().build());
        write(store, SCOPE, new B(SCOPE, "meeting:r1").passage("SYNTH-PII-0501 asked about the payment gateway.",
                Sensitivity.RESTRICTED).build());
        long before = log(store, SCOPE).size();
        List<String> found = retrieve(store, SCOPE, "checkout retries payment", Sensitivity.RESTRICTED).stream()
                .map(i -> i.origin().id()).toList();
        assertThat(found).isNotEmpty();

        ReindexReceipt receipt = reindex(store, SCOPE, "req-ri", new OtherEmbedder(Set.of("en")));

        assertThat(receipt.status()).isEqualTo(ReindexReceipt.Status.APPLIED);
        assertThat(receipt.fromModel()).isEqualTo(new DeterministicEmbedder().model());
        assertThat(receipt.toModel()).isEqualTo("other-bag-16");
        assertThat(receipt).extracting(ReindexReceipt::passages, ReindexReceipt::skipped).containsExactly(3, 0);
        BaseInfo info = store.describe(SCOPE).toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertThat(info.model()).isEqualTo("other-bag-16");
        assertThat(info.dimension()).isEqualTo(16);
        // the same passages come back, from the new vectors alone, restricted ones included
        assertThat(retrieve(store, SCOPE, "checkout retries payment", Sensitivity.RESTRICTED).stream()
                .map(i -> i.origin().id()).toList()).containsAll(found);
        assertThat(retrieve(store, SCOPE, "payment gateway", Sensitivity.RESTRICTED))
                .anyMatch(i -> i.content().contains("payment gateway"));
        // one entry for the whole base, with models and no text
        List<ChangeEntry> entries = log(store, SCOPE);
        assertThat(entries.subList((int) before, entries.size())).singleElement().satisfies(e -> {
            assertThat(e.op()).isEqualTo(Op.REINDEX);
            assertThat(e.targetId()).isEqualTo(new DeterministicEmbedder().model() + "->other-bag-16");
            assertThat(e.reason()).isEqualTo("reindex:model");
            assertThat(e.sequence()).isEqualTo(receipt.lastSequence());
        });
        assertThat(entries.toString()).doesNotContain("SYNTH-PII");
        // and again: the model is already the target
        assertThat(reindex(store, SCOPE, "req-ri2", new OtherEmbedder(Set.of("en"))).status())
                .isEqualTo(ReindexReceipt.Status.UNCHANGED);
        assertThat(log(store, SCOPE)).hasSize(entries.size());
    }

    @Test
    protected void ca3_a_reindexation_to_a_model_that_misses_a_language_of_the_base_changes_nothing() throws Exception {
        KnowledgeStore store = newStore(false);
        write(store, SCOPE, checkout().build());
        long before = log(store, SCOPE).size();

        assertThatThrownBy(() -> reindex(store, SCOPE, "req-pt", new OtherEmbedder(Set.of("pt"))))
                .hasCauseInstanceOf(KnowledgeValidationException.class);

        assertThat(store.describe(SCOPE).toCompletableFuture().get(10, TimeUnit.SECONDS).model())
                .isEqualTo(new DeterministicEmbedder().model());
        assertThat(log(store, SCOPE)).hasSize((int) before);
        assertThat(retrieve(store, SCOPE, "checkout payment", Sensitivity.INTERNAL)).isNotEmpty();
    }

    @Test
    protected void ca3_a_restricted_passage_whose_blob_expired_leaves_the_index_in_a_reindexation() throws Exception {
        KnowledgeStore store = newStore(true);
        write(store, SCOPE, new B(SCOPE, "meeting:r2").passage("SYNTH-PII-0502 owns the launch plan.", Sensitivity.RESTRICTED)
                .passage("The launch is on Tuesday.", Sensitivity.INTERNAL).build());
        advanceClock(Duration.ofDays(31));

        ReindexReceipt receipt = reindex(store, SCOPE, "req-ex", new OtherEmbedder(Set.of("en")));

        assertThat(receipt).extracting(ReindexReceipt::passages, ReindexReceipt::skipped).containsExactly(1, 1);
        assertThat(log(store, SCOPE)).filteredOn(e -> e.op() == Op.EXPIRE).hasSize(1);
        assertThat(retrieve(store, SCOPE, "launch", Sensitivity.RESTRICTED)).singleElement()
                .satisfies(i -> assertThat(i.content()).isEqualTo("The launch is on Tuesday."));
    }

    @Test
    protected void ca7_the_verification_counts_what_an_erasure_should_have_removed_and_finds_the_tombstone() throws Exception {
        KnowledgeStore store = newStore(true);
        write(store, SCOPE, checkout().build());
        DeleteRequest byDocument = new DeleteRequest.ByDocument("req-v", "erasure:document", "meeting:m-001");

        ErasureCheck before = store.verifyErased(SCOPE, byDocument).toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertThat(before.verified()).isFalse();
        assertThat(before.tombstone()).isFalse();
        assertThat(before.remaining()).containsKeys("DOCUMENT", "PASSAGE", "ENTITY", "RELATION", "LINK");

        erase(store, SCOPE, byDocument);
        ErasureCheck after = store.verifyErased(SCOPE, byDocument).toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertThat(after.remaining()).isEmpty();
        assertThat(after.tombstone()).isTrue();
        assertThat(after.verified()).isTrue();

        // by person, absent from the base: nothing to remove and the tombstone is in place all the same
        DeleteRequest absent = new DeleteRequest.ByPerson("req-vp", "erasure:person", "e-nobody", null);
        erase(store, SCOPE, absent);
        assertThat(store.verifyErased(SCOPE, absent).toCompletableFuture().get(10, TimeUnit.SECONDS).verified()).isTrue();
    }

    @Test
    protected void ca1_erasing_a_document_deletes_the_blobs_it_came_from_and_the_verification_sees_them_gone() throws Exception {
        KnowledgeStore store = newStore(false);
        URI batch = putBlob("batch of m-b1 with SYNTH-PII-0601");
        URI minutes = putBlob("minutes of m-b1 with SYNTH-PII-0601");
        URI other = putBlob("minutes of m-b2");
        write(store, SCOPE, new B(SCOPE, "meeting:b1").passage("Plan the migration.", Sensitivity.INTERNAL)
                .derivedFrom(batch, minutes).build());
        write(store, SCOPE, new B(SCOPE, "meeting:b2").passage("Plan the retro.", Sensitivity.INTERNAL).derivedFrom(other).build());

        DeleteRequest request = new DeleteRequest.ByDocument("req-b", "erasure:document", "meeting:b1");
        erase(store, SCOPE, request);

        assertThat(blobExists(batch)).isFalse();
        assertThat(blobExists(minutes)).isFalse();
        assertThat(blobExists(other)).isTrue();   // another document's origin stays
        assertThat(store.verifyErased(SCOPE, request).toCompletableFuture().get(10, TimeUnit.SECONDS).verified()).isTrue();
        assertThat(heldOutsideBlobs(SCOPE)).doesNotContain(batch.toString());   // the log never names a pointer
    }

    @Test
    protected void ca1_the_blobs_of_every_publication_of_a_document_go_with_it() throws Exception {
        KnowledgeStore store = newStore(false);
        URI first = putBlob("first batch");
        URI second = putBlob("second batch");
        B doc = new B(SCOPE, "meeting:b3").passage("Same text twice.", Sensitivity.INTERNAL).derivedFrom(first);
        write(store, SCOPE, doc.build());
        // the same state republished from another batch: nothing changes in the base, the new origin is remembered
        doc.derivedFrom(second);
        assertThat(write(store, SCOPE, doc.build()).status()).isEqualTo(Status.UNCHANGED);

        erase(store, SCOPE, new DeleteRequest.ByDocument("req-b3", "erasure:document", "meeting:b3"));

        assertThat(blobExists(first)).isFalse();
        assertThat(blobExists(second)).isFalse();
    }

    @Test
    protected void ca1_erasing_a_person_deletes_the_blobs_of_the_documents_that_named_her_and_no_others() throws Exception {
        KnowledgeStore store = newStore(false);
        URI named = putBlob("minutes naming PERSON_aa");
        URI unrelated = putBlob("minutes of the retro");
        write(store, SCOPE, new B(SCOPE, "meeting:b4").passage("PERSON_aa owns the rollout.", Sensitivity.INTERNAL)
                .entity("e-aa", "Person", "PERSON_aa", Sensitivity.INTERNAL).link(0, "e-aa").derivedFrom(named).build());
        write(store, SCOPE, new B(SCOPE, "meeting:b5").passage("The retro was short.", Sensitivity.INTERNAL)
                .derivedFrom(unrelated).build());

        DeleteRequest request = new DeleteRequest.ByPerson("req-b4", "erasure:person", "e-aa", null);
        erase(store, SCOPE, request);

        assertThat(blobExists(named)).isFalse();
        assertThat(blobExists(unrelated)).isTrue();
        assertThat(store.verifyErased(SCOPE, request).toCompletableFuture().get(10, TimeUnit.SECONDS).verified()).isTrue();
    }

    // --- CA13: language ---------------------------------------------------------------------------------------------

    @Test
    protected void ca13_a_language_the_embedder_does_not_support_is_refused_and_english_is_kept_as_written() throws Exception {
        KnowledgeStore store = newStore(false);
        String text = "Façade review: the café menu is final.";
        KnowledgeBatch portuguese = new KnowledgeBatch("b-pt", "meeting:pt", "h", null, null, SCHEMA,
                List.of(new Passage(PassageIds.of(SCOPE, "meeting:pt", 0, "A reunião foi adiada."), 0,
                        "A reunião foi adiada.", "pt-BR", Sensitivity.INTERNAL)), List.of(), List.of(), List.of());
        assertThatThrownBy(() -> write(store, SCOPE, portuguese)).hasCauseInstanceOf(KnowledgeValidationException.class)
                .hasMessageContaining("language");

        write(store, SCOPE, new B(SCOPE, "meeting:en").passage(text, Sensitivity.INTERNAL).build());

        assertThat(retrieve(store, SCOPE, "review menu final", Sensitivity.INTERNAL)).singleElement()
                .satisfies(i -> assertThat(i.content()).isEqualTo(text));
    }

    // --- the batch itself -------------------------------------------------------------------------------------------

    @Test
    protected void a_malformed_batch_is_refused_whole_and_changes_nothing() throws Exception {
        KnowledgeStore store = newStore(false);
        B wrongId = new B(SCOPE, "meeting:bad");
        wrongId.passages.add(new Passage("p-not-the-derived-id", 0, "Text.", "en", Sensitivity.INTERNAL));
        B danglingLink = new B(SCOPE, "meeting:bad").passage("Text.", Sensitivity.INTERNAL).link(0, "e-nowhere");
        B belowEnds = new B(SCOPE, "meeting:bad").entity("e-a", "Term", "A", Sensitivity.CONFIDENTIAL)
                .entity("e-b", "Term", "B", Sensitivity.INTERNAL).relation("mentions", "e-a", "e-b", Sensitivity.INTERNAL);
        B repeated = new B(SCOPE, "meeting:bad").entity("e-a", "Term", "A", Sensitivity.INTERNAL)
                .entity("e-a", "Term", "A again", Sensitivity.INTERNAL);

        for (B bad : List.of(wrongId, danglingLink, belowEnds, repeated)) {
            assertThatThrownBy(() -> write(store, SCOPE, bad.build())).hasCauseInstanceOf(KnowledgeValidationException.class);
        }
        assertThat(log(store, SCOPE)).isEmpty();
        assertThat(retrieve(store, SCOPE, "text", Sensitivity.RESTRICTED)).isEmpty();
    }

    @Test
    protected void a_level_is_never_lowered_by_a_later_write() throws Exception {
        KnowledgeStore store = newStore(true);
        write(store, SCOPE, new B(SCOPE, "meeting:l1").entity("e-x", "Term", "Thing", Sensitivity.CONFIDENTIAL).build());
        write(store, SCOPE, new B(SCOPE, "meeting:l2").entity("e-x", "Term", "Thing", Sensitivity.INTERNAL).build());

        assertThat(store.entities(SCOPE, new EntityQuery(null, EntityIds.normalize("Thing"), Sensitivity.INTERNAL, 5))
                .toCompletableFuture().get(10, TimeUnit.SECONDS)).isEmpty();
        assertThat(store.entities(SCOPE, new EntityQuery(null, EntityIds.normalize("Thing"), Sensitivity.CONFIDENTIAL, 5))
                .toCompletableFuture().get(10, TimeUnit.SECONDS)).hasSize(1);
    }

    @Test
    protected void entities_are_found_by_label_or_alias_after_normalizing() throws Exception {
        KnowledgeStore store = newStore(false);
        write(store, SCOPE, new B(SCOPE, "meeting:e1").entity("e-sla", "Term", "SLA", Sensitivity.INTERNAL).build());
        KnowledgeBatch withAlias = new KnowledgeBatch("b-alias", "meeting:e2", "h2", null, null, SCHEMA, List.of(),
                List.of(new Entity("e-svc", "System", "Checkout Service", Map.of("aliases", "checkout|the checkout svc"),
                        Sensitivity.INTERNAL)), List.of(), List.of());
        write(store, SCOPE, withAlias);

        assertThat(store.entities(SCOPE, new EntityQuery("Term", EntityIds.normalize(" SLA. "), Sensitivity.INTERNAL, 5))
                .toCompletableFuture().get(10, TimeUnit.SECONDS)).extracting(EntityMatch::entityId).containsExactly("e-sla");
        assertThat(store.entities(SCOPE, new EntityQuery(null, EntityIds.normalize("The Checkout SVC"), Sensitivity.INTERNAL, 5))
                .toCompletableFuture().get(10, TimeUnit.SECONDS)).singleElement()
                .satisfies(m -> assertThat(m.aliases()).containsExactly("checkout", "the checkout svc"));
        assertThat(store.entities(SCOPE, new EntityQuery("Term", EntityIds.normalize("checkout service"), Sensitivity.INTERNAL, 5))
                .toCompletableFuture().get(10, TimeUnit.SECONDS)).isEmpty();   // wrong type
    }

    @Test
    protected void identities_are_a_function_of_where_and_what() {
        assertThat(PassageIds.of(SCOPE, "d", 0, "text")).isEqualTo(PassageIds.of(SCOPE, "d", 0, "text"))
                .isNotEqualTo(PassageIds.of(SCOPE, "d", 1, "text")).isNotEqualTo(PassageIds.of(SCOPE, "d", 0, "other"))
                .isNotEqualTo(PassageIds.of(SCOPE, "e", 0, "text"));
        assertThat(EntityIds.of("System", "Checkout  Service!")).isEqualTo(EntityIds.of("System", "checkout service"))
                .isNotEqualTo(EntityIds.of("Term", "checkout service"));
        assertThat(EntityIds.normalize("  Café-Menu, 2nd  ")).isEqualTo("cafe menu 2nd");
    }

    // --- SDD 3b.4: the graph by depth, the evidence, the seeds --------------------------------------------------------

    /** m-001: the old policy affects checkout; m-002: the new one supersedes the old; m-003: the new one affects the gateway. */
    private void policies(KnowledgeStore store, Sensitivity gatewayLevel) throws Exception {
        write(store, SCOPE, new B(SCOPE, "meeting:m-001").passage("We keep the old retry policy for checkout.", Sensitivity.INTERNAL)
                .entity("e-old", "Decision", "Old retry policy", Sensitivity.INTERNAL)
                .entity("e-checkout", "System", "Checkout Service", Sensitivity.INTERNAL)
                .relation("affects", "e-old", "e-checkout", Sensitivity.INTERNAL).link(0, "e-old").build());
        write(store, SCOPE, new B(SCOPE, "meeting:m-002").passage("The plan for payments is a longer story.", Sensitivity.INTERNAL)
                .entity("e-new", "Decision", "New retry policy", Sensitivity.INTERNAL)
                .entity("e-old", "Decision", "Old retry policy", Sensitivity.INTERNAL)
                .relation("supersedes", "e-new", "e-old", Sensitivity.INTERNAL).link(0, "e-new").build());
        write(store, SCOPE, new B(SCOPE, "meeting:m-003").passage("The gateway needs the new policy.", Sensitivity.INTERNAL)
                .entity("e-new", "Decision", "New retry policy", Sensitivity.INTERNAL)
                .entity("e-gateway", "System", "Gateway", gatewayLevel)
                .entity("e-checkout", "System", "Checkout Service", Sensitivity.INTERNAL)
                .relation("affects", "e-new", "e-gateway", gatewayLevel)
                .relation("affects", "e-checkout", "e-new", Sensitivity.INTERNAL).link(0, "e-new").build());
    }

    private static List<ContextItem> hops(KnowledgeStore store, Sensitivity ceiling, int hops, String... entityIds)
            throws Exception {
        return store.retrieve(SCOPE, new RetrievalQuery("old retry policy", 50, 0, ceiling, null, Set.of(entityIds), null, hops))
                .toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    private static List<String> contents(List<ContextItem> items, ContextItem.Kind kind) {
        return items.stream().filter(i -> i.origin().kind() == kind).map(ContextItem::content).toList();
    }

    @Test
    protected void ca4_two_hops_reach_what_no_passage_names_and_one_hop_is_what_it_was() throws Exception {
        KnowledgeStore store = newStore(false);
        policies(store, Sensitivity.INTERNAL);

        List<ContextItem> one = hops(store, Sensitivity.INTERNAL, 1, "e-old");
        List<ContextItem> two = hops(store, Sensitivity.INTERNAL, 2, "e-old");

        assertThat(contents(one, ContextItem.Kind.ENTITY)).containsExactly("Decision: Old retry policy");
        assertThat(contents(one, ContextItem.Kind.RELATION)).isEmpty();
        assertThat(contents(two, ContextItem.Kind.ENTITY)).contains("Decision: Old retry policy", "System: Checkout Service",
                "Decision: New retry policy");
        assertThat(contents(two, ContextItem.Kind.RELATION)).contains("New retry policy -[supersedes]-> Old retry policy",
                "Old retry policy -[affects]-> Checkout Service");
        assertThat(contents(two, ContextItem.Kind.ENTITY)).doesNotContain("System: Gateway");
    }

    @Test
    protected void ca4_three_hops_go_one_step_further_and_a_cycle_adds_nothing_twice() throws Exception {
        KnowledgeStore store = newStore(false);
        policies(store, Sensitivity.INTERNAL);   // checkout -> new -> old -> checkout is a cycle

        List<ContextItem> three = hops(store, Sensitivity.INTERNAL, 3, "e-old");

        assertThat(contents(three, ContextItem.Kind.ENTITY)).contains("System: Gateway").doesNotHaveDuplicates();
        assertThat(contents(three, ContextItem.Kind.RELATION)).contains("New retry policy -[affects]-> Gateway")
                .doesNotHaveDuplicates();
        assertThat(three).extracting(i -> i.origin().id()).doesNotHaveDuplicates();
    }

    @Test
    protected void ca4_what_the_graph_adds_says_which_documents_hold_it_and_scores_less_the_further_it_is() throws Exception {
        KnowledgeStore store = newStore(false);
        policies(store, Sensitivity.INTERNAL);

        List<ContextItem> three = hops(store, Sensitivity.INTERNAL, 3, "e-old");

        ContextItem supersedes = three.stream().filter(i -> i.content().equals("New retry policy -[supersedes]-> Old retry policy"))
                .findFirst().orElseThrow();
        assertThat(supersedes.origin().evidence()).containsExactly("meeting:m-002");
        ContextItem gateway = three.stream().filter(i -> i.content().equals("System: Gateway")).findFirst().orElseThrow();
        assertThat(gateway.origin().evidence()).containsExactly("meeting:m-003");
        ContextItem old = three.stream().filter(i -> i.content().equals("Decision: Old retry policy")).findFirst().orElseThrow();
        assertThat(old.origin().evidence()).containsExactly("meeting:m-001", "meeting:m-002");
        assertThat(gateway.score()).isLessThan(old.score());
        ContextItem passage = three.stream().filter(i -> i.origin().kind() == ContextItem.Kind.PASSAGE).findFirst().orElseThrow();
        assertThat(passage.origin().evidence()).isEmpty();
        assertThat(passage.origin().documentId()).isEqualTo("meeting:m-001");
    }

    @Test
    protected void ca4_the_graph_never_shows_past_the_ceiling_even_two_hops_away() throws Exception {
        KnowledgeStore store = newStore(false);
        policies(store, Sensitivity.CONFIDENTIAL);

        List<ContextItem> internal = hops(store, Sensitivity.INTERNAL, 3, "e-old");
        List<ContextItem> confidential = hops(store, Sensitivity.CONFIDENTIAL, 3, "e-old");

        assertThat(internal.toString()).doesNotContain("Gateway");
        assertThat(internal).extracting(ContextItem::sensitivity).containsOnly(Sensitivity.INTERNAL);
        assertThat(contents(confidential, ContextItem.Kind.ENTITY)).contains("System: Gateway");
    }

    @Test
    protected void ca4_hops_go_to_three_and_no_further() {
        assertThat(new RetrievalQuery("q", 5, 0, Sensitivity.INTERNAL, null, Set.of(), null, 3).hops()).isEqualTo(3);
        assertThatThrownBy(() -> new RetrievalQuery("q", 5, 0, Sensitivity.INTERNAL, null, Set.of(), null, 4))
                .isInstanceOf(KnowledgeValidationException.class);
    }

    @Test
    protected void ca8_erasing_the_document_that_held_a_relation_takes_the_relation_and_its_evidence_out_of_the_reach()
            throws Exception {
        KnowledgeStore store = newStore(false);
        policies(store, Sensitivity.INTERNAL);

        erase(store, SCOPE, new DeleteRequest.ByDocument("req-3b4", "erasure:document", "meeting:m-002"));
        List<ContextItem> items = hops(store, Sensitivity.INTERNAL, 3, "e-old");

        assertThat(contents(items, ContextItem.Kind.RELATION)).doesNotContain("New retry policy -[supersedes]-> Old retry policy");
        assertThat(items.stream().flatMap(i -> i.origin().evidence().stream())).doesNotContain("meeting:m-002");
        ContextItem old = items.stream().filter(i -> i.content().equals("Decision: Old retry policy")).findFirst().orElseThrow();
        assertThat(old.origin().evidence()).containsExactly("meeting:m-001");
    }

    @Test
    protected void ca3_the_seeds_of_a_question_are_the_entities_of_the_base_it_names_at_or_below_the_ceiling() throws Exception {
        KnowledgeStore store = newStore(false);
        policies(store, Sensitivity.CONFIDENTIAL);

        assertThat(KnowledgeSeeds.find(store, SCOPE, "Which decision superseded the Old Retry Policy?", null,
                Sensitivity.INTERNAL)).containsExactly("e-old");
        assertThat(KnowledgeSeeds.find(store, SCOPE, "what do we know about the gateway and checkout service", null,
                Sensitivity.INTERNAL)).containsExactly("e-checkout");   // the gateway is confidential
        assertThat(KnowledgeSeeds.find(store, SCOPE, "what do we know about the gateway", null, Sensitivity.CONFIDENTIAL))
                .containsExactly("e-gateway");
        assertThat(KnowledgeSeeds.find(store, SCOPE, "nothing of the kind is here", null, Sensitivity.INTERNAL)).isEmpty();
        assertThat(KnowledgeSeeds.find(store, SCOPE, "unrelated words", List.of("new retry policy"), Sensitivity.INTERNAL))
                .containsExactly("e-new");
    }

    @Test
    protected void ca3_a_long_question_costs_at_most_the_lookups_the_limit_allows() throws Exception {
        KnowledgeStore store = newStore(false);
        policies(store, Sensitivity.INTERNAL);
        int[] calls = {0};
        KnowledgeStore counting = (KnowledgeStore) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {KnowledgeStore.class}, (proxy, method, args) -> {
                    if (method.getName().equals("entities")) {
                        calls[0]++;
                    }
                    try {
                        return method.invoke(store, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });

        KnowledgeSeeds.find(counting, SCOPE, "word ".repeat(200), null, Sensitivity.INTERNAL);

        assertThat(calls[0]).isLessThanOrEqualTo(KnowledgeSeeds.MAX_LOOKUPS);
    }
}
