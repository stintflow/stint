package io.stintflow.knowledge.inspector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.stintflow.knowledge.ChangeEntry;
import io.stintflow.knowledge.Entity;
import io.stintflow.knowledge.NodeView;
import io.stintflow.knowledge.Relation;
import io.stintflow.spi.Sensitivity;

/**
 * SDD 3b.6, CA1 to CA8 and CA11: the timeline rebuilt from the log, the highlight by chain, the mask, the access, the aggregation
 * above the limit, the metrics, the duplicates and the export — on a base with a history. Synthetic data only.
 */
class InspectorServiceTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String OPERATOR = "op-1";

    @TempDir
    Path dir;

    private BaseFixture base;
    private InspectorService service;

    @BeforeEach
    void setUp() throws Exception {
        base = BaseFixture.withHistory(dir);
        service = serviceWith(InspectorService.Settings.DEFAULT);
    }

    private InspectorService serviceWith(InspectorService.Settings settings) {
        return new InspectorService(base.store, InspectorAccess.allowing(Map.of(OPERATOR, Set.of(BaseFixture.SCOPE))), settings);
    }

    private List<ChangeEntry> log() throws Exception {
        return base.store.changes(BaseFixture.SCOPE, 0, 10_000).toCompletableFuture().get();
    }

    // --- CA1 ---------------------------------------------------------------------------------------------------------

    @Test
    void ca1_the_structure_at_three_points_matches_what_the_log_says_was_written_until_then() throws Exception {
        List<ChangeEntry> log = log();
        long day1 = Timeline.sequenceAt(log, Instant.parse("2026-10-01T23:00:00Z"));
        long day2 = Timeline.sequenceAt(log, Instant.parse("2026-10-02T23:00:00Z"));

        Snapshot first = service.snapshot(OPERATOR, BaseFixture.SCOPE, null, Instant.parse("2026-10-01T23:00:00Z"));
        Snapshot second = service.snapshot(OPERATOR, BaseFixture.SCOPE, day2, null);
        Snapshot last = service.snapshot(OPERATOR, BaseFixture.SCOPE, null, null);

        assertThat(first.sequence()).isEqualTo(day1);
        assertThat(first.nodes()).extracting(Snapshot.Node::id).containsExactly("e-checkout", "e-payments", "e-person");
        assertThat(first.nodesByType()).containsEntry("System", 2).containsEntry("Person", 1);
        assertThat(second.nodes()).extracting(Snapshot.Node::id).containsExactly("e-billing", "e-checkout", "e-payments", "e-person");
        assertThat(second.edges()).hasSize(3);
        assertThat(last.nodes()).extracting(Snapshot.Node::id).containsExactly("e-billing", "e-checkout", "e-payments");
        assertThat(last.edges()).extracting(Snapshot.Edge::type).containsExactlyInAnyOrder("affects", "affects");
        // the counts agree with the log: what was added and not taken out since
        long addedNodes = log.stream().filter(e -> e.sequence() <= day2 && e.kind() == ChangeEntry.Kind.ENTITY
                && e.op() == ChangeEntry.Op.ADD).count();
        assertThat(second.nodes()).hasSize((int) addedNodes);
        assertThat(service.snapshot(OPERATOR, BaseFixture.SCOPE, 0L, null).nodes()).isEmpty();
    }

    // --- CA2 ---------------------------------------------------------------------------------------------------------

    @Test
    void ca2_highlighting_a_chain_shows_exactly_what_it_created_and_nothing_else() throws Exception {
        Snapshot last = service.snapshot(OPERATOR, BaseFixture.SCOPE, null, null);

        Set<String> chainB = Timeline.createdBy(last, "chain-B");

        Set<String> fromLog = log().stream().filter(e -> "chain-B".equals(e.chainId()) && e.op() == ChangeEntry.Op.ADD
                && (e.kind() == ChangeEntry.Kind.ENTITY || e.kind() == ChangeEntry.Kind.RELATION))
                .map(ChangeEntry::targetId).collect(Collectors.toCollection(java.util.TreeSet::new));
        assertThat(chainB).isEqualTo(fromLog).contains("e-billing");
        assertThat(chainB).doesNotContain("e-payments");   // chain-A's
        assertThat(service.changes(OPERATOR, BaseFixture.SCOPE, 0, 1000,
                new InspectorService.ChangeFilter("chain-B", null, null, null)))
                .isNotEmpty().allSatisfy(c -> assertThat(c.chainId()).isEqualTo("chain-B"));
    }

    // --- CA3, CA6 ----------------------------------------------------------------------------------------------------

    @Test
    void ca3_a_restricted_node_is_only_an_id_and_a_type_in_the_view_the_snapshot_and_the_export() throws Exception {
        base.now.set(Instant.parse("2026-10-04T10:00:00Z"));
        base.write("meeting:m-3", "chain-C", "inst-3", 1L, base.entities(new Entity("e-lead", "Person", "SYNTH-PII-0202",
                Sensitivity.RESTRICTED), new Entity("e-checkout", "System", "Checkout Service", Sensitivity.INTERNAL)), List.of());

        InspectorService.Current current = service.current(OPERATOR, BaseFixture.SCOPE);
        Snapshot snapshot = service.snapshot(OPERATOR, BaseFixture.SCOPE, null, null);
        String export = service.graphMl(OPERATOR, BaseFixture.SCOPE);

        NodeView lead = current.nodes().stream().filter(n -> n.id().equals("e-lead")).findFirst().orElseThrow();
        assertThat(lead.label()).isNull();
        assertThat(lead.type()).isEqualTo("Person");
        Snapshot.Node inSnapshot = snapshot.nodes().stream().filter(n -> n.id().equals("e-lead")).findFirst().orElseThrow();
        assertThat(inSnapshot.label()).isNull();
        assertThat(export).contains("e-lead").contains("masked").doesNotContain("SYNTH-PII-0202");
        assertThat(JSON.writeValueAsString(current) + JSON.writeValueAsString(snapshot))
                .doesNotContain("SYNTH-PII-0202").doesNotContain("Text of meeting");
        // a name shows for what is alive and open
        assertThat(snapshot.nodes().stream().filter(n -> n.id().equals("e-checkout")).findFirst().orElseThrow().label())
                .isEqualTo("Checkout Service");
    }

    @Test
    void ca6_after_a_person_is_erased_nothing_reveals_the_name_and_the_timeline_keeps_the_opaque_erase() throws Exception {
        String everything = JSON.writeValueAsString(service.current(OPERATOR, BaseFixture.SCOPE))
                + JSON.writeValueAsString(service.snapshot(OPERATOR, BaseFixture.SCOPE, null, null))
                + JSON.writeValueAsString(service.metrics(OPERATOR, BaseFixture.SCOPE))
                + JSON.writeValueAsString(service.duplicates(OPERATOR, BaseFixture.SCOPE))
                + JSON.writeValueAsString(service.changes(OPERATOR, BaseFixture.SCOPE, 0, 5000, InspectorService.ChangeFilter.ALL))
                + service.graphMl(OPERATOR, BaseFixture.SCOPE);

        assertThat(everything).doesNotContain(BaseFixture.PERSON_NAME);
        List<InspectorService.Change> changes = service.changes(OPERATOR, BaseFixture.SCOPE, 0, 5000, InspectorService.ChangeFilter.ALL);
        assertThat(changes).anySatisfy(c -> {
            assertThat(c.op()).isEqualTo("ERASE");
            assertThat(c.targetId()).isEqualTo(BaseFixture.PERSON_ID);   // opaque: the id means nothing without the pseudonym map
        });
        assertThat(service.snapshot(OPERATOR, BaseFixture.SCOPE, null, null).erased()).contains(BaseFixture.PERSON_ID);
        // and before the erasure, the same node was there as an id and a type, with no name
        Snapshot before = service.snapshot(OPERATOR, BaseFixture.SCOPE, null, Instant.parse("2026-10-02T23:00:00Z"));
        Snapshot.Node person = before.nodes().stream().filter(n -> n.id().equals(BaseFixture.PERSON_ID)).findFirst().orElseThrow();
        assertThat(person.label()).isNull();
        assertThat(person.type()).isEqualTo("Person");
    }

    // --- CA4 ---------------------------------------------------------------------------------------------------------

    @Test
    void ca4_what_left_after_a_point_is_there_by_id_and_type_and_what_stayed_has_its_name() throws Exception {
        base.now.set(Instant.parse("2026-10-05T10:00:00Z"));
        base.store.delete(BaseFixture.SCOPE, new io.stintflow.knowledge.DeleteRequest.ByDocument("rm-2", "erasure:document",
                "meeting:m-2")).toCompletableFuture().get();

        Snapshot before = service.snapshot(OPERATOR, BaseFixture.SCOPE, null, Instant.parse("2026-10-04T00:00:00Z"));
        Snapshot now = service.snapshot(OPERATOR, BaseFixture.SCOPE, null, null);

        Snapshot.Node billing = before.nodes().stream().filter(n -> n.id().equals("e-billing")).findFirst().orElseThrow();
        assertThat(billing.type()).isEqualTo("System");
        assertThat(billing.label()).isNull();   // gone now; the log never held a name
        assertThat(before.nodes().stream().filter(n -> n.id().equals("e-payments")).findFirst().orElseThrow().label())
                .isEqualTo("Payments API");
        assertThat(now.nodes()).extracting(Snapshot.Node::id).doesNotContain("e-billing");
    }

    // --- CA5 ---------------------------------------------------------------------------------------------------------

    @Test
    void ca5_two_spellings_of_a_name_are_a_candidate_until_a_reingestion_brings_them_together() throws Exception {
        base.now.set(Instant.parse("2026-10-06T10:00:00Z"));
        base.write("meeting:m-4", "chain-D", "inst-4", 1L, base.entities(new Entity("e-payment-api", "System", "Payment API",
                Sensitivity.INTERNAL)), List.of(new Relation("affects", "e-payment-api", "e-checkout", Sensitivity.INTERNAL)));

        List<Duplicates.Candidate> found = service.duplicates(OPERATOR, BaseFixture.SCOPE);

        assertThat(found).hasSize(1);
        assertThat(found.get(0).criterion()).isEqualTo("edit-distance");
        assertThat(List.of(found.get(0).aId(), found.get(0).bId())).containsExactlyInAnyOrder("e-payments", "e-payment-api");
        Metrics.Report report = service.metrics(OPERATOR, BaseFixture.SCOPE);
        assertThat(report.duplicateCandidates()).isEqualTo(1);
        assertThat(report.alert()).isEqualTo(Metrics.Alert.ALERT);   // two of four open nodes are involved

        // the document is re-ingested resolving to the one name: the duplicate node is no longer sustained by any document
        base.now.set(Instant.parse("2026-10-07T10:00:00Z"));
        base.write("meeting:m-4", "chain-D", "inst-5", 2L, base.entities(new Entity("e-payments", "System", "Payments API",
                Sensitivity.INTERNAL)), List.of());
        assertThat(service.duplicates(OPERATOR, BaseFixture.SCOPE)).isEmpty();
        assertThat(service.current(OPERATOR, BaseFixture.SCOPE).nodes()).extracting(NodeView::id).doesNotContain("e-payment-api");
        assertThat(service.metrics(OPERATOR, BaseFixture.SCOPE).alert()).isEqualTo(Metrics.Alert.OK);
    }

    // --- CA7 ---------------------------------------------------------------------------------------------------------

    @Test
    void ca7_without_permission_everything_is_refused_the_same_way_and_an_empty_list_refuses_all() {
        InspectorService other = new InspectorService(base.store, InspectorAccess.allowing(Map.of()), InspectorService.Settings.DEFAULT);
        InspectorService nobody = new InspectorService(base.store, InspectorAccess.none(), InspectorService.Settings.DEFAULT);
        io.stintflow.knowledge.KnowledgeScope unknown = io.stintflow.knowledge.KnowledgeScope.named("nowhere", "none");

        for (InspectorService denied : List.of(other, nobody)) {
            assertThatThrownBy(() -> denied.current(OPERATOR, BaseFixture.SCOPE)).isInstanceOf(InspectorAccessDeniedException.class);
            assertThatThrownBy(() -> denied.snapshot(OPERATOR, BaseFixture.SCOPE, null, null))
                    .isInstanceOf(InspectorAccessDeniedException.class);
            assertThatThrownBy(() -> denied.changes(OPERATOR, BaseFixture.SCOPE, 0, 10, InspectorService.ChangeFilter.ALL))
                    .isInstanceOf(InspectorAccessDeniedException.class);
            assertThatThrownBy(() -> denied.metrics(OPERATOR, BaseFixture.SCOPE)).isInstanceOf(InspectorAccessDeniedException.class);
            assertThatThrownBy(() -> denied.duplicates(OPERATOR, BaseFixture.SCOPE)).isInstanceOf(InspectorAccessDeniedException.class);
            assertThatThrownBy(() -> denied.graphMl(OPERATOR, BaseFixture.SCOPE)).isInstanceOf(InspectorAccessDeniedException.class);
        }
        assertThatThrownBy(() -> service.current(OPERATOR, unknown)).isInstanceOf(InspectorAccessDeniedException.class)
                .hasMessage("Not allowed");   // a base that does not exist is refused the same way
        assertThatThrownBy(() -> service.current(null, BaseFixture.SCOPE)).isInstanceOf(InspectorAccessDeniedException.class);
    }

    // --- CA8 ---------------------------------------------------------------------------------------------------------

    @Test
    void ca8_above_the_limit_the_structure_is_counted_and_not_listed_and_it_says_so() throws Exception {
        InspectorService small = serviceWith(new InspectorService.Settings(2, Sensitivity.RESTRICTED, Metrics.Thresholds.DEFAULT));

        InspectorService.Current current = small.current(OPERATOR, BaseFixture.SCOPE);
        Snapshot snapshot = small.snapshot(OPERATOR, BaseFixture.SCOPE, null, null);

        assertThat(current.aggregated()).isTrue();
        assertThat(current.nodes()).isEmpty();
        assertThat(current.nodeCount()).isEqualTo(3);
        assertThat(current.nodesByType()).containsEntry("System", 3);
        assertThat(snapshot.aggregated()).isTrue();
        assertThat(snapshot.nodes()).isEmpty();
        assertThat(snapshot.nodesByType()).containsEntry("System", 3);
        assertThat(service.current(OPERATOR, BaseFixture.SCOPE).aggregated()).isFalse();
    }

    // --- metrics -----------------------------------------------------------------------------------------------------

    @Test
    void the_metrics_follow_the_log_by_day_by_chain_and_by_kind_of_change() throws Exception {
        Metrics.Report report = service.metrics(OPERATOR, BaseFixture.SCOPE);

        assertThat(report.nodes()).isEqualTo(3);
        assertThat(report.nodesByType()).containsEntry("System", 3);
        assertThat(report.restrictedNodes()).isZero();
        assertThat(report.erasures()).isPositive();
        assertThat(report.addedByChain()).containsKeys("chain-A", "chain-B");
        assertThat(report.series()).extracting(Metrics.Point::day).containsExactly("2026-10-01", "2026-10-02", "2026-10-03");
        assertThat(report.series().get(0).nodes()).isEqualTo(3);
        assertThat(report.series().get(1).nodes()).isEqualTo(4);
        assertThat(report.series().get(2).nodes()).isEqualTo(3);
        assertThat(report.lastSequence()).isEqualTo(log().getLast().sequence());
        // asked again after more was written, it only takes in what is new
        base.now.set(Instant.parse("2026-10-08T10:00:00Z"));
        base.write("meeting:m-5", "chain-E", "inst-6", 1L, base.entities(new Entity("e-x", "Team", "Platform Team",
                Sensitivity.INTERNAL)), List.of());
        Metrics.Report later = service.metrics(OPERATOR, BaseFixture.SCOPE);
        assertThat(later.nodes()).isEqualTo(4);
        assertThat(later.series()).hasSize(4);
        InspectorService growing = serviceWith(new InspectorService.Settings(500, Sensitivity.RESTRICTED,
                new Metrics.Thresholds(0.05, 0.15, 1)));
        assertThat(growing.metrics(OPERATOR, BaseFixture.SCOPE).alert()).isEqualTo(Metrics.Alert.ALERT);
        assertThat(growing.metrics(OPERATOR, BaseFixture.SCOPE).reasons()).contains("growth-per-chain");
    }

    @Test
    void the_ceiling_of_the_service_hides_what_is_above_it_from_the_view_the_timeline_and_the_export() throws Exception {
        base.now.set(Instant.parse("2026-10-09T10:00:00Z"));
        base.write("meeting:m-6", "chain-F", "inst-7", 1L, base.entities(new Entity("e-conf", "Team", "Team Confidential",
                Sensitivity.CONFIDENTIAL)), List.of());
        InspectorService internalOnly = serviceWith(new InspectorService.Settings(500, Sensitivity.INTERNAL, Metrics.Thresholds.DEFAULT));

        assertThat(internalOnly.current(OPERATOR, BaseFixture.SCOPE).nodes()).extracting(NodeView::id).doesNotContain("e-conf");
        assertThat(internalOnly.snapshot(OPERATOR, BaseFixture.SCOPE, null, null).nodes()).extracting(Snapshot.Node::id)
                .doesNotContain("e-conf");
        assertThat(internalOnly.changes(OPERATOR, BaseFixture.SCOPE, 0, 5000, InspectorService.ChangeFilter.ALL))
                .extracting(InspectorService.Change::targetId).doesNotContain("e-conf");
        assertThat(internalOnly.graphMl(OPERATOR, BaseFixture.SCOPE)).doesNotContain("e-conf").doesNotContain("Team Confidential");
        assertThat(service.current(OPERATOR, BaseFixture.SCOPE).nodes()).extracting(NodeView::id).contains("e-conf");
    }
}
