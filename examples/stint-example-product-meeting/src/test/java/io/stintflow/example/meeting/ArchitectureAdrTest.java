package io.stintflow.example.meeting;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.cloudevents.CloudEvent;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.knowledge.Entity;
import io.stintflow.knowledge.EntityIds;
import io.stintflow.knowledge.GraphPointer;
import io.stintflow.knowledge.KnowledgeBatch;
import io.stintflow.knowledge.PassageIds;
import io.stintflow.knowledge.Source;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.CeWire;
import io.stintflow.wire.Json;
import io.stintflow.wire.decision.DecisionKind;
import io.stintflow.wire.decision.DecisionOption;
import io.stintflow.wire.decision.DecisionRequest;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.knowledge.ResolvedViews;

/**
 * SDD 3c.4 — the architecture decisions of a meeting become ADRs: each pair (decision, product decision) is judged by a decision
 * task, a person reviews the unsure and every conflict, and the page says which product decisions motivated it without carrying
 * their text. Fake models, a controllable clock, no sleep; synthetic data only.
 */
class ArchitectureAdrTest {

    /** Planted in the product decision: it may reach the decision and the reviewer, never the page, the fact, the state or the graph. */
    static final String PRODUCT_SECRET = "SYNTH-PRODUCT-SUMMARY the customer sees the confirmation, so the checkout stays synchronous";
    static final String SYNC_LABEL = "Keep the checkout synchronous";

    @TempDir
    Path work;

    private final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-10-22T09:00:00Z"));
    private final InMemoryTimerService timer = new InMemoryTimerService(now::get);

    private ArchitectureAdrPipeline pipeline() throws Exception {
        return new ArchitectureAdrPipeline(work, now::get, timer);
    }

    /** A decision of the product domain, with its status and a summary the view only gives to the one definition that checks conflicts. */
    static String productDecision(ArchitectureAdrPipeline p, String key, String label, String summary) {
        String id = EntityIds.of("Decision", key);
        Entity entity = new Entity(id, "Decision", label, Map.of("key", key, "summary", summary, "status", "accepted"),
                Sensitivity.CONFIDENTIAL);
        p.product.write(ArchitectureAdrPipeline.PRODUCT, new KnowledgeBatch("t:" + key, "product-doc:" + key,
                PassageIds.sha256Hex(entity.toString()), 1L, Source.NONE, "meetings@1.0.0", List.of(), List.of(entity), List.of(),
                List.of())).toCompletableFuture().join();
        return GraphPointer.of(ArchitectureAdrPipeline.PRODUCT, id, 1).format();
    }

    static ObjectNode decision(String id, String title, String summary, String affects, String... pointers) {
        ObjectNode d = Json.obj().put("id", id).put("title", title).put("summary", summary);
        ArrayNode motivated = d.putArray("motivatedBy");
        for (String pointer : pointers) {
            motivated.add(pointer);
        }
        if (affects != null) {
            d.put("affects", affects);
        }
        return d;
    }

    static ArrayNode decisions(ObjectNode... all) {
        ArrayNode array = Json.MAPPER.createArrayNode();
        for (ObjectNode d : all) {
            array.add(d);
        }
        return array;
    }

    private static JsonNode published(ArchitectureAdrPipeline p) {
        CloudEvent fact = p.emitted().stream().filter(e -> e.getType().equals(ArchitectureAdrPipeline.PUBLISHED)).reduce((a, b) -> b)
                .orElseThrow();
        return Json.read(fact.getData().toBytes());
    }

    private static String everywhere(ArchitectureAdrPipeline p) {
        return "fact: " + p.emitted().stream().map(e -> new String(CeWire.toJson(e), StandardCharsets.UTF_8)).toList()
                + "\nstate: " + p.state.load(p.current).toCompletableFuture().join()
                + "\naudit: " + p.audit.of(p.current).toCompletableFuture().join()
                + "\nconfluence: " + p.confluence.pages()
                + "\narchitecture: " + p.architecture.dump(ArchitectureAdrPipeline.ARCHITECTURE);
    }

    /** Runs a single decision against a product decision; the reviewer's answers and the requests it saw come back. */
    private record Outcome(InstanceSnapshot snapshot, List<JsonNode> requests) {
    }

    private Outcome run(ArchitectureAdrPipeline p, CloudEvent drafted, String answer) throws Exception {
        List<JsonNode> seen = new CopyOnWriteArrayList<>();
        String id = p.start(drafted);
        InstanceSnapshot snapshot = p.await(id, Duration.ofSeconds(60), request -> {
            if (!seen.contains(request)) {
                seen.add(request);
            }
            return answer;
        });
        return new Outcome(snapshot, seen);
    }

    @Test
    void ca1_ca6_ca7_a_conflict_is_reviewed_and_the_adr_says_conflicting_without_the_text_of_the_product() throws Exception {
        try (ArchitectureAdrPipeline p = pipeline()) {
            String pointer = productDecision(p, "sync", SYNC_LABEL, PRODUCT_SECRET);
            URI draft = p.draft("arch-1", decisions(decision("async-checkout", "Make the checkout asynchronous",
                    "Make the checkout asynchronous through a queue", "Checkout Service", pointer)));

            Outcome outcome = run(p, ArchitectureAdrPipeline.drafted("arch-1", draft, null), "conflicts");

            assertThat(outcome.snapshot().status()).as("at %s", outcome.snapshot().position()).isEqualTo(InstanceStatus.COMPLETED);
            assertThat(outcome.requests()).as("a confident conflict is still reviewed").hasSize(1);
            assertThat(outcome.requests().getFirst().path("role").asText()).isEqualTo("tech-rep");
            JsonNode fact = published(p);
            assertThat(fact.path("counts").path("decisions").asInt()).isEqualTo(1);
            assertThat(fact.path("counts").path("conflicting").asInt()).isEqualTo(1);
            assertThat(fact.path("pages").get(0).path("status").asText()).isEqualTo("conflicting");

            // CA6: one line per pointer, with the label the origin publishes; neither the summary nor the material
            var page = p.confluence.pages().getFirst();
            assertThat(page.spaceKey()).isEqualTo("ARCHITECTURE");
            assertThat(page.body()).contains(pointer).contains(SYNC_LABEL).contains("Make the checkout asynchronous through a queue")
                    .contains("conflicts");
            // CA7: the decision and the relation, to the same reference the linking wrote, and it is confidential
            String graph = p.architecture.dump(ArchitectureAdrPipeline.ARCHITECTURE);
            assertThat(graph).contains("async-checkout").contains("motivatedBy").contains("affects").contains("Checkout Service");
            assertThat(everywhere(p)).doesNotContain("SYNTH-PRODUCT-SUMMARY");
        }
    }

    @Test
    void ca2_the_answer_of_the_person_is_the_verdict_when_it_differs_from_the_machine() throws Exception {
        try (ArchitectureAdrPipeline p = pipeline()) {
            String pointer = productDecision(p, "sync", SYNC_LABEL, PRODUCT_SECRET);
            URI draft = p.draft("arch-2", decisions(decision("async-checkout", "Make the checkout asynchronous",
                    "Make the checkout asynchronous through a queue", null, pointer)));

            Outcome outcome = run(p, ArchitectureAdrPipeline.drafted("arch-2", draft, null), "compatible");

            assertThat(outcome.snapshot().status()).isEqualTo(InstanceStatus.COMPLETED);
            assertThat(outcome.requests()).hasSize(1);
            assertThat(published(p).path("pages").get(0).path("status").asText()).isEqualTo("proposed");
            assertThat(p.confluence.pages().getFirst().body()).contains("compatible (reviewed)");
        }
    }

    @Test
    void ca3_a_compatible_pair_is_not_reviewed() throws Exception {
        try (ArchitectureAdrPipeline p = pipeline()) {
            String pointer = productDecision(p, "sync", SYNC_LABEL, PRODUCT_SECRET);
            URI draft = p.draft("arch-3", decisions(decision("cache-catalog", "Cache the catalog pages",
                    "Cache the catalog pages at the edge", null, pointer)));

            Outcome outcome = run(p, ArchitectureAdrPipeline.drafted("arch-3", draft, null), "conflicts");

            assertThat(outcome.snapshot().status()).isEqualTo(InstanceStatus.COMPLETED);
            assertThat(outcome.requests()).isEmpty();
            assertThat(published(p).path("pages").get(0).path("status").asText()).isEqualTo("proposed");
            assertThat(p.confluence.pages().getFirst().body()).contains("compatible").doesNotContain("(reviewed)");
        }
    }

    @Test
    void ca4_an_unsure_pair_goes_to_the_person_and_the_adr_says_it_was_reviewed() throws Exception {
        try (ArchitectureAdrPipeline p = pipeline()) {
            String pointer = productDecision(p, "sync", SYNC_LABEL, PRODUCT_SECRET);
            URI draft = p.draft("arch-4", decisions(decision("unclear-queue", "Queue the notifications",
                    "It is unclear whether the queue touches the checkout", null, pointer)));

            Outcome outcome = run(p, ArchitectureAdrPipeline.drafted("arch-4", draft, null), "compatible");

            assertThat(outcome.snapshot().status()).isEqualTo(InstanceStatus.COMPLETED);
            assertThat(outcome.requests()).hasSize(1);
            assertThat(published(p).path("pages").get(0).path("status").asText()).isEqualTo("proposed");
            assertThat(p.confluence.pages().getFirst().body()).contains("compatible (reviewed)");
        }
    }

    @Test
    void ca5_no_answer_in_the_deadline_is_a_conflict() throws Exception {
        try (ArchitectureAdrPipeline p = pipeline()) {
            String pointer = productDecision(p, "sync", SYNC_LABEL, PRODUCT_SECRET);
            URI draft = p.draft("arch-5", decisions(decision("async-checkout", "Make the checkout asynchronous",
                    "Make the checkout asynchronous through a queue", null, pointer)));
            String id = p.start(ArchitectureAdrPipeline.drafted("arch-5", draft, null));
            long until = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (p.channel.allOpen().isEmpty() && System.nanoTime() < until) {
                java.util.concurrent.locks.LockSupport.parkNanos(20_000_000L);
            }
            assertThat(p.channel.allOpen()).hasSize(1);

            InstanceSnapshot snapshot = p.snapshot(id);
            while (snapshot.status() != InstanceStatus.COMPLETED && snapshot.status() != InstanceStatus.FAILED
                    && System.nanoTime() < until) {
                // the reminder fires first and arms the rest of the wait from the new instant: the clock moves a step at a time
                now.set(now.get().plus(Duration.ofHours(5)));
                timer.tick();
                java.util.concurrent.locks.LockSupport.parkNanos(20_000_000L);
                snapshot = p.snapshot(id);
            }

            assertThat(snapshot.status()).as("at %s", snapshot.position()).isEqualTo(InstanceStatus.COMPLETED);
            assertThat(published(p).path("pages").get(0).path("status").asText()).isEqualTo("conflicting");
        }
    }

    @Test
    void ca8_an_origin_that_does_not_answer_sends_every_pair_to_the_person_when_the_author_chose_to_follow() throws Exception {
        try (ArchitectureAdrPipeline p = pipeline()) {
            String sync = productDecision(p, "sync", SYNC_LABEL, PRODUCT_SECRET);
            String cache = productDecision(p, "cache", "Cache the catalog", "Catalog pages are cached");
            URI draft = p.draft("arch-8", decisions(decision("cache-catalog", "Cache the catalog pages",
                    "Cache the catalog pages at the edge", null, sync, cache)));
            p.originDown(true);

            Outcome outcome = run(p, ArchitectureAdrPipeline.drafted("arch-8", draft, "follow"), "compatible");

            assertThat(outcome.snapshot().status()).as("at %s", outcome.snapshot().position()).isEqualTo(InstanceStatus.COMPLETED);
            assertThat(outcome.requests()).as("nothing is decided without content").hasSize(2);
            assertThat(published(p).path("resolution").asText()).isEqualTo("unavailable");
            assertThat(p.confluence.pages().getFirst().body()).contains("(not available)").contains("compatible (reviewed)");
        }
    }

    @Test
    void ca8_an_origin_that_does_not_answer_ends_the_instance_when_the_author_chose_to_suspend() throws Exception {
        try (ArchitectureAdrPipeline p = pipeline()) {
            String sync = productDecision(p, "sync", SYNC_LABEL, PRODUCT_SECRET);
            URI draft = p.draft("arch-8s", decisions(decision("cache-catalog", "Cache the catalog pages",
                    "Cache the catalog pages at the edge", null, sync)));
            p.originDown(true);

            Outcome outcome = run(p, ArchitectureAdrPipeline.drafted("arch-8s", draft, "suspend"), "compatible");

            assertThat(outcome.snapshot().status()).isNotEqualTo(InstanceStatus.COMPLETED);
            assertThat(p.emitted().stream().filter(e -> e.getType().equals(ArchitectureAdrPipeline.PUBLISHED))).isEmpty();
            assertThat(p.confluence.pages()).isEmpty();
        }
    }

    @Test
    void ca9_only_the_definition_that_checks_conflicts_receives_the_summary() throws Exception {
        try (ArchitectureAdrPipeline p = pipeline()) {
            String pointer = productDecision(p, "sync", SYNC_LABEL, PRODUCT_SECRET);

            JsonNode forAdr = resolvedAs(p, "architecture-adr", pointer);
            JsonNode forOthers = resolvedAs(p, "architecture-resolve", pointer);

            assertThat(forAdr.path("viewVersion").asText()).isEqualTo(ArchitectureAdrPipeline.ADR_VIEW);
            assertThat(forAdr.path("view").path("properties").path("summary").asText()).isEqualTo(PRODUCT_SECRET);
            assertThat(forOthers.path("viewVersion").asText()).isEqualTo("product-v1");
            assertThat(forOthers.path("view").path("properties").has("summary")).isFalse();
            assertThat(p.audit.of("adr-view").toCompletableFuture().join()).anySatisfy(r -> {
                assertThat(r.detail()).containsEntry("requester", "examples:architecture-adr").containsEntry("allowSensitive", "true");
            });
        }
    }

    private static JsonNode resolvedAs(ArchitectureAdrPipeline p, String definition, String pointer) {
        ObjectNode input = Json.obj().put("depth", 0);
        input.putArray("pointers").add(pointer);
        JsonNode out = p.resolveHandler.execute(new TaskContext("t", "adr-view", "c-" + definition, 1, input,
                new WorkflowRef("examples", definition, "1.0.0"))).toCompletableFuture().join();
        return ResolvedViews.read(p.blobs, out.path("viewRef").asText()).orElseThrow().path("entries").get(0);
    }

    @Test
    void ca10_the_decision_on_the_type_of_the_document_knows_the_product_requirements() throws Exception {
        var request = new DecisionRequest(DecisionKind.CHOICE, List.of(new DecisionOption("ata", "Minutes"),
                new DecisionOption("prd", "Product requirements")), Json.obj().put("text", "These are the product requirements for the new checkout"));

        var decision = FakeDecisions.docType().decide(request, null).toCompletableFuture().join();

        assertThat(decision.value().asText()).isEqualTo("prd");
        try (var yaml = getClass().getClassLoader().getResourceAsStream("stint/workflows/product-meeting.yaml")) {
            assertThat(new String(yaml.readAllBytes(), StandardCharsets.UTF_8)).contains("- prd:");
        }
    }

    @Test
    void ca12_a_draft_over_the_limits_is_refused_before_any_pair_is_checked() throws Exception {
        try (ArchitectureAdrPipeline p = pipeline()) {
            String pointer = productDecision(p, "sync", SYNC_LABEL, PRODUCT_SECRET);
            List<ObjectNode> many = new ArrayList<>();
            for (int i = 0; i < 21; i++) {
                many.add(decision("d" + i, "Decision " + i, "Cache " + i, null, pointer));
            }
            ObjectNode tooManyPointers = decision("many-pointers", "Many pointers", "Cache", null, pointer, pointer, pointer, pointer,
                    pointer, pointer);

            for (ArrayNode draft : List.of(decisions(many.toArray(ObjectNode[]::new)), decisions(tooManyPointers))) {
                Outcome outcome = run(p, ArchitectureAdrPipeline.drafted("arch-12-" + draft.size(), p.draft("arch-12-" + draft.size(), draft),
                        null), "compatible");
                assertThat(outcome.snapshot().status()).isEqualTo(InstanceStatus.FAILED);
            }
            assertThat(p.channel.allOpen()).isEmpty();
            assertThat(p.confluence.pages()).isEmpty();
        }
    }
}
