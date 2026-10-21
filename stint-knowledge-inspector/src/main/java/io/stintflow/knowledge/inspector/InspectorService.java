package io.stintflow.knowledge.inspector;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import io.stintflow.knowledge.ChangeEntry;
import io.stintflow.knowledge.EdgeView;
import io.stintflow.knowledge.GraphView;
import io.stintflow.knowledge.KnowledgeInspector;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.NodeView;
import io.stintflow.knowledge.ViewQuery;
import io.stintflow.spi.Sensitivity;

/**
 * SDD 3b.6: what the page and the API show, built from the inspection port alone. Every call asks {@link InspectorAccess} first
 * and refuses, without saying whether the base exists, when the caller may not look. Nothing here writes, erases or opens a blob.
 */
public final class InspectorService {

    private static final int PAGE = 1000;

    /** @param maxNodes more nodes than this are counted, not listed (RF9) */
    public record Settings(int maxNodes, Sensitivity ceiling, Metrics.Thresholds thresholds) {
        public static final Settings DEFAULT = new Settings(500, Sensitivity.RESTRICTED, Metrics.Thresholds.DEFAULT);

        public Settings {
            Objects.requireNonNull(ceiling, "ceiling");
            Objects.requireNonNull(thresholds, "thresholds");
            if (maxNodes < 1) {
                throw new IllegalArgumentException("maxNodes is at least 1");
            }
        }
    }

    /** The structure now: listed, or only counted when it is too big. */
    public record Current(boolean aggregated, long lastSequence, List<NodeView> nodes, List<EdgeView> edges,
            Map<String, Integer> nodesByType, int nodeCount) {
    }

    /** A change as the timeline shows it: no hash, no text. */
    public record Change(long sequence, String at, String op, String kind, String targetId, String elementType, String level,
            String instanceId, String chainId, String definition, String schemaVersion, String documentId, String reason) {
    }

    /** What to keep of the log; {@code null} keeps all. */
    public record ChangeFilter(String chainId, String instanceId, String elementType, String documentId) {
        public static final ChangeFilter ALL = new ChangeFilter(null, null, null, null);

        boolean matches(ChangeEntry e) {
            return (chainId == null || chainId.equals(e.chainId())) && (instanceId == null || instanceId.equals(e.instanceId()))
                    && (elementType == null || elementType.equals(e.elementType()))
                    && (documentId == null || documentId.equals(e.documentId()));
        }
    }

    private final KnowledgeInspector inspector;
    private final InspectorAccess access;
    private final Settings settings;
    private final Map<KnowledgeScope, Metrics> metrics = new ConcurrentHashMap<>();

    public InspectorService(KnowledgeInspector inspector, InspectorAccess access, Settings settings) {
        this.inspector = Objects.requireNonNull(inspector, "inspector");
        this.access = Objects.requireNonNull(access, "access");
        this.settings = Objects.requireNonNull(settings, "settings");
    }

    public boolean mayInspect(String caller, KnowledgeScope scope) {
        return access.mayInspect(caller, scope);
    }

    private void require(String caller, KnowledgeScope scope) {
        if (!access.mayInspect(caller, scope)) {
            throw new InspectorAccessDeniedException();
        }
    }

    // --- the structure now ------------------------------------------------------------------------------------------

    public Current current(String caller, KnowledgeScope scope) {
        require(caller, scope);
        List<NodeView> nodes = new ArrayList<>();
        List<EdgeView> edges = new ArrayList<>();
        long last = 0;
        String after = null;
        do {
            GraphView page = await(inspector.view(scope, new ViewQuery(settings.ceiling(), PAGE, after, null)));
            nodes.addAll(page.nodes());
            edges.addAll(page.edges());
            last = page.lastSequence();
            after = page.next();
        } while (after != null);
        Map<String, Integer> byType = new TreeMap<>();
        nodes.forEach(n -> byType.merge(n.type(), 1, Integer::sum));
        if (nodes.size() > settings.maxNodes()) {
            return new Current(true, last, List.of(), List.of(), byType, nodes.size());
        }
        return new Current(false, last, nodes, edges, byType, nodes.size());
    }

    // --- the structure at a point -----------------------------------------------------------------------------------

    /** @param sequence the point as a log sequence, or {@code null}; @param at the point as an instant, or {@code null} for the end */
    public Snapshot snapshot(String caller, KnowledgeScope scope, Long sequence, Instant at) {
        require(caller, scope);
        List<ChangeEntry> log = readLog(scope, 0);
        long upTo = sequence != null ? sequence : at != null ? Timeline.sequenceAt(log, at) : Long.MAX_VALUE;
        return Timeline.at(log, upTo, labels(allNodes(scope)), settings.ceiling(), settings.maxNodes());
    }

    // --- the timeline -----------------------------------------------------------------------------------------------

    public List<Change> changes(String caller, KnowledgeScope scope, long after, int limit, ChangeFilter filter) {
        require(caller, scope);
        int wanted = Math.max(1, Math.min(limit, 5000));
        List<Change> out = new ArrayList<>();
        long cursor = after;
        while (out.size() < wanted) {
            List<ChangeEntry> page = await(inspector.changes(scope, cursor, PAGE));
            if (page.isEmpty()) {
                break;
            }
            for (ChangeEntry e : page) {
                cursor = e.sequence();
                if (visible(e) && filter.matches(e) && out.size() < wanted) {
                    out.add(new Change(e.sequence(), e.at().toString(), e.op().name(), e.kind().name(), e.targetId(),
                            e.elementType(), e.sensitivity() == null ? null : e.sensitivity().name(), e.instanceId(), e.chainId(),
                            e.definition(), e.schemaVersion(), e.documentId(), e.reason()));
                }
            }
            if (page.size() < PAGE) {
                break;
            }
        }
        return out;
    }

    private boolean visible(ChangeEntry e) {
        return e.sensitivity() == null || e.sensitivity().compareTo(settings.ceiling()) <= 0;
    }

    // --- metrics and duplicates -------------------------------------------------------------------------------------

    public Metrics.Report metrics(String caller, KnowledgeScope scope) {
        require(caller, scope);
        Metrics m = metrics.computeIfAbsent(scope, s -> new Metrics(settings.thresholds()));
        synchronized (m) {
            for (ChangeEntry e : readLog(scope, m.watermark())) {
                m.accept(e);
            }
            return m.report(allNodes(scope));
        }
    }

    public List<Duplicates.Candidate> duplicates(String caller, KnowledgeScope scope) {
        require(caller, scope);
        return Duplicates.find(allNodes(scope));
    }

    // --- export -----------------------------------------------------------------------------------------------------

    public String graphMl(String caller, KnowledgeScope scope) {
        require(caller, scope);
        LogState state = new LogState();
        readLog(scope, 0).forEach(state::apply);
        return GraphMlExport.write(state, labels(allNodes(scope)), settings.ceiling());
    }

    // --- reading through the port -----------------------------------------------------------------------------------

    private List<ChangeEntry> readLog(KnowledgeScope scope, long after) {
        List<ChangeEntry> all = new ArrayList<>();
        long cursor = after;
        while (true) {
            List<ChangeEntry> page = await(inspector.changes(scope, cursor, PAGE));
            all.addAll(page);
            if (page.size() < PAGE) {
                return all;
            }
            cursor = page.get(page.size() - 1).sequence();
        }
    }

    private List<NodeView> allNodes(KnowledgeScope scope) {
        List<NodeView> all = new ArrayList<>();
        String after = null;
        do {
            GraphView page = await(inspector.view(scope, new ViewQuery(settings.ceiling(), PAGE, after, null)));
            all.addAll(page.nodes());
            after = page.next();
        } while (after != null);
        return all;
    }

    private static Map<String, String> labels(List<NodeView> nodes) {
        Map<String, String> labels = new HashMap<>();
        nodes.stream().filter(n -> n.label() != null).forEach(n -> labels.put(n.id(), n.label()));
        return labels;
    }

    private static <T> T await(java.util.concurrent.CompletionStage<T> stage) {
        try {
            return stage.toCompletableFuture().get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while reading the base");
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException("The base could not be read");
        }
    }
}
