package io.stintflow.knowledge.inspector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import io.stintflow.knowledge.ChangeEntry;
import io.stintflow.knowledge.ChangeEntry.Kind;
import io.stintflow.knowledge.NodeView;
import io.stintflow.spi.Sensitivity;

/**
 * SDD 3b.6, 4.3: what the log says about how a base grows — and whether it degrades. Fed entry by entry; it remembers the last
 * sequence it saw (the watermark), so a service gives it only what is new. Counts only: no label, no text.
 */
public final class Metrics {

    /** Where the alerts start; every limit is configurable. */
    public record Thresholds(double duplicateRatioWarn, double duplicateRatioAlert, int growthPerChainAlert) {
        public static final Thresholds DEFAULT = new Thresholds(0.05, 0.15, 500);
    }

    public enum Alert { OK, WARN, ALERT }

    /** The base at the end of a day (UTC) of the log: alive things. */
    public record Point(String day, int nodes, int edges, int passages, int links, Map<String, Integer> nodesByType) {
    }

    public record Report(long lastSequence, int nodes, int edges, int passages, int links, int restrictedNodes,
            double restrictedRatio, Map<String, Integer> nodesByType, Map<String, Integer> edgesByType,
            Map<String, Integer> addedByChain, Map<String, Integer> addedByDefinition, int erasures, int expirations,
            int removals, int reindexes, List<Point> series, int duplicateCandidates, int duplicateNodes,
            double duplicateRatio, Alert alert, List<String> reasons) {
    }

    private final Thresholds thresholds;
    private final LogState state = new LogState();
    private final Map<String, Integer> addedByChain = new TreeMap<>();
    private final Map<String, Integer> addedByDefinition = new TreeMap<>();
    private final Map<String, Point> days = new TreeMap<>();

    public Metrics(Thresholds thresholds) {
        this.thresholds = thresholds;
    }

    /** The last sequence seen, to ask the log for what comes after. */
    public long watermark() {
        return state.sequence();
    }

    public void accept(ChangeEntry e) {
        if (e.sequence() <= state.sequence()) {
            return;   // seen already
        }
        state.apply(e);
        if (e.op() == ChangeEntry.Op.ADD && (e.kind() == Kind.ENTITY || e.kind() == Kind.RELATION)) {
            addedByChain.merge(e.chainId() == null ? "(none)" : e.chainId(), 1, Integer::sum);
            addedByDefinition.merge(e.definition() == null ? "(none)" : e.definition(), 1, Integer::sum);
        }
        String day = e.at().toString().substring(0, 10);
        days.put(day, new Point(day, state.alive(Kind.ENTITY).size(), state.alive(Kind.RELATION).size(),
                state.alive(Kind.PASSAGE).size(), state.alive(Kind.LINK).size(), countByType(Kind.ENTITY)));
    }

    private Map<String, Integer> countByType(Kind kind) {
        Map<String, Integer> out = new TreeMap<>();
        state.alive(kind).values().forEach(el -> out.merge(el.type() == null ? "(none)" : el.type(), 1, Integer::sum));
        return out;
    }

    /** @param current the nodes now (from the view), to look for candidates for duplicates among those with a label */
    public Report report(List<NodeView> current) {
        int nodes = state.alive(Kind.ENTITY).size();
        long restricted = state.alive(Kind.ENTITY).values().stream().filter(n -> n.level() == Sensitivity.RESTRICTED).count();
        List<Duplicates.Candidate> candidates = Duplicates.find(current);
        int involved = Duplicates.involved(candidates);
        long comparable = current.stream().filter(n -> n.level() != Sensitivity.RESTRICTED && n.label() != null).count();
        double ratio = comparable == 0 ? 0 : (double) involved / comparable;
        Alert alert = ratio >= thresholds.duplicateRatioAlert() ? Alert.ALERT
                : ratio >= thresholds.duplicateRatioWarn() ? Alert.WARN : Alert.OK;
        List<String> reasons = new ArrayList<>();
        if (alert != Alert.OK) {
            reasons.add("duplicate-ratio");
        }
        int biggest = addedByChain.values().stream().mapToInt(Integer::intValue).max().orElse(0);
        if (biggest >= thresholds.growthPerChainAlert()) {
            reasons.add("growth-per-chain");
            alert = Alert.ALERT;
        }
        return new Report(state.sequence(), nodes, state.alive(Kind.RELATION).size(), state.alive(Kind.PASSAGE).size(),
                state.alive(Kind.LINK).size(), (int) restricted, nodes == 0 ? 0 : (double) restricted / nodes,
                countByType(Kind.ENTITY), countByType(Kind.RELATION), Map.copyOf(addedByChain), Map.copyOf(addedByDefinition),
                state.erasures(), state.expirations(), state.removals(), state.reindexes(), List.copyOf(days.values()),
                candidates.size(), involved, ratio, alert, reasons);
    }

    /** The replayed state, for the export. */
    LogState state() {
        return state;
    }
}
