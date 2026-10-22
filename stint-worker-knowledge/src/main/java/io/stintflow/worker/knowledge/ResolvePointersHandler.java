package io.stintflow.worker.knowledge;

import java.net.URI;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.knowledge.ChangeEntry;
import io.stintflow.knowledge.ChangeEntry.Kind;
import io.stintflow.knowledge.ChangeEntry.Op;
import io.stintflow.knowledge.GraphPointer;
import io.stintflow.knowledge.KnowledgeNodes;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.KnowledgeStore;
import io.stintflow.knowledge.KnowledgeValidationException;
import io.stintflow.knowledge.NodeContent;
import io.stintflow.knowledge.NodesQuery;
import io.stintflow.spi.AuditLog;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.RetentionClass;
import io.stintflow.spi.Sensitivity;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;

/**
 * The task {@code resolve-pointers} (SDD 3c.2, 4.3): the origin of a node answers another domain's pointer — by its own policy,
 * through the view it publishes, and to the definition that asks.
 * <p>
 * <b>Input.</b> {@code {"pointers": [ "stint-kg://…" ], "depth"?: 0 | 1}}; one to {@value #MAX_POINTERS} pointers, each well formed
 * ({@code validation} 400 for the whole batch otherwise: it is the asker's mistake, not a denial). <b>Who asks</b> is the
 * definition of the invocation — never a field of the input.
 * <p>
 * <b>Answer.</b> {@code {"viewRef", "results": [{"pointer", "status", "changed"?, "currentVersion"?}], "counts"}} — status and a
 * pointer to a blob, and <em>no content</em>: what was resolved rests in a blob of a retention class (and can be discarded at
 * once, {@link ResolvedViews}), never in the state of the asker. {@code DENIED} is one answer for everything the asker may not
 * know — no grant, a type not granted, a level above the ceiling, a restricted or unknown node, a base this worker does not serve.
 * {@code REMOVED} is for a node that was there, was not restricted, and is gone.
 */
public final class ResolvePointersHandler implements TaskHandler {

    public static final String AUDIT_KIND = "pointer-resolve";
    public static final int MAX_POINTERS = 50;
    public static final int MAX_NEIGHBORS = 20;
    /** What the platform allows as the shortest life of a class of blobs (a day). */
    public static final RetentionClass DEFAULT_CLASS = new RetentionClass("resolved-view", java.time.Duration.ofDays(1));

    static final int LOG_PAGE = 1_000;
    static final int LOG_PAGES = 100;
    private static final Logger LOG = LoggerFactory.getLogger(ResolvePointersHandler.class);

    private enum Status { RESOLVED, DENIED, REMOVED }

    private final KnowledgeStore store;
    private final KnowledgeNodes nodes;
    private final ResolutionPolicy policy;
    private final BlobStore blobs;
    private final RetentionClass viewClass;
    private final AuditLog audit;
    private final InstantSource clock;

    public ResolvePointersHandler(KnowledgeStore store, KnowledgeNodes nodes, ResolutionPolicy policy, BlobStore blobs,
            RetentionClass viewClass, AuditLog audit, InstantSource clock) {
        this.store = Objects.requireNonNull(store, "store");
        this.nodes = Objects.requireNonNull(nodes, "nodes");
        this.policy = Objects.requireNonNull(policy, "policy");
        this.blobs = Objects.requireNonNull(blobs, "blobs");
        this.viewClass = viewClass == null ? DEFAULT_CLASS : viewClass;
        this.audit = audit == null ? AuditLog.NONE : audit;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext ctx) {
        try {
            return CompletableFuture.completedFuture(resolve(ctx));
        } catch (TaskFailure failure) {
            return CompletableFuture.failedFuture(failure);
        } catch (RuntimeException e) {
            LOG.warn("Resolution of task {} in instance {} failed ({})", ctx.taskId(), ctx.instanceId(), e.getClass().getName());
            return CompletableFuture.failedFuture(Failures.unavailable("The resolution failed (" + e.getClass().getSimpleName()
                    + ")"));
        }
    }

    private record Entry(GraphPointer pointer, Status status, boolean changed, long currentVersion, String viewVersion,
            ObjectNode view, Sensitivity level) {
    }

    private JsonNode resolve(TaskContext ctx) {
        JsonNode in = ctx.input();
        JsonNode asked = in.path("pointers");
        if (!asked.isArray() || asked.isEmpty() || asked.size() > MAX_POINTERS) {
            throw Failures.validation("pointers is one to " + MAX_POINTERS + " pointers");
        }
        int depth = in.path("depth").asInt(0);
        if (!in.path("depth").isMissingNode() && (!in.path("depth").isInt() || depth < 0 || depth > 1)) {
            throw Failures.validation("depth is 0 or 1");
        }
        List<GraphPointer> pointers = new ArrayList<>();
        for (int i = 0; i < asked.size(); i++) {
            try {
                pointers.add(GraphPointer.parse(asked.get(i).asText(null)));
            } catch (KnowledgeValidationException e) {
                throw Failures.validation("pointers[" + i + "]: " + e.getMessage());
            }
        }
        String namespace = ctx.definition() == null ? "" : ctx.definition().namespace();
        String name = ctx.definition() == null ? "" : ctx.definition().name();

        // by base: what the policy lets this asker see there, read once
        Map<KnowledgeScope, Base> bases = new LinkedHashMap<>();
        for (GraphPointer pointer : pointers) {
            bases.computeIfAbsent(pointer.scope(), scope -> base(scope, namespace, name, pointers, depth));
        }

        List<Entry> entries = new ArrayList<>();
        for (GraphPointer pointer : pointers) {
            entries.add(entry(pointer, bases.get(pointer.scope()), depth));
        }

        ArrayNode results = Json.MAPPER.createArrayNode();
        ArrayNode stored = Json.MAPPER.createArrayNode();
        Map<Status, Integer> counts = new EnumMap<>(Status.class);
        Map<Sensitivity, Integer> byLevel = new EnumMap<>(Sensitivity.class);
        for (Entry entry : entries) {
            counts.merge(entry.status, 1, Integer::sum);
            ObjectNode result = results.addObject();
            result.put("pointer", entry.pointer.format()).put("status", entry.status.name());
            ObjectNode blob = stored.addObject();
            blob.put("pointer", entry.pointer.format()).put("status", entry.status.name());
            if (entry.status == Status.RESOLVED) {
                result.put("changed", entry.changed).put("currentVersion", entry.currentVersion);
                blob.put("changed", entry.changed).put("currentVersion", entry.currentVersion);
                blob.put("viewVersion", entry.viewVersion);
                blob.set("view", entry.view);
                byLevel.merge(entry.level, 1, Integer::sum);
            }
        }
        ObjectNode document = Json.obj();
        document.set("entries", stored);
        URI ref = blobs.put(Json.bytes(document), "resolved/" + ctx.instanceId() + "-" + ctx.taskId() + ".json", viewClass)
                .toCompletableFuture().join();

        ObjectNode out = Json.obj();
        out.put("viewRef", ref.toString());
        out.set("results", results);
        ObjectNode summary = out.putObject("counts");
        for (Status status : Status.values()) {
            summary.put(status.name(), counts.getOrDefault(status, 0));
        }
        record(ctx, namespace, name, bases, counts, byLevel, depth);
        return out;
    }

    /** What the asker may see in one base, and the nodes and the log read for it; empty when it may see nothing there. */
    private record Base(KnowledgeScope scope, Grant grant, PublishedView view, Map<String, NodeContent> found,
            Map<String, ChangeEntry> lastEntry, Map<String, Long> versions) {
    }

    private Base base(KnowledgeScope scope, String namespace, String name, List<GraphPointer> pointers, int depth) {
        Optional<Grant> grant = policy.grantFor(namespace, name, scope);
        Optional<PublishedView> view = grant.flatMap(g -> policy.viewOf(scope, g));
        if (grant.isEmpty() || view.isEmpty()) {
            return new Base(scope, null, null, Map.of(), Map.of(), Map.of());
        }
        Set<String> ids = new LinkedHashSet<>();
        pointers.stream().filter(p -> p.scope().equals(scope)).forEach(p -> ids.add(p.nodeId()));
        Map<String, NodeContent> found = new HashMap<>();
        List<ChangeEntry> log = log(scope);
        try {
            List<String> chunk = new ArrayList<>(ids);
            nodes.nodes(scope, new NodesQuery(new LinkedHashSet<>(chunk), grant.get().ceiling(), depth == 1 ? MAX_NEIGHBORS : 0))
                    .toCompletableFuture().join().forEach(n -> found.put(n.id(), n));
        } catch (CompletionException e) {
            throw Failures.unavailable("The base could not be read (" + e.getCause().getClass().getSimpleName() + ")");
        }
        Map<String, ChangeEntry> last = new HashMap<>();
        Map<String, Long> versions = new HashMap<>();
        for (ChangeEntry entry : log) {
            if (entry.kind() == Kind.ENTITY) {
                last.put(entry.targetId(), entry);
                if (entry.op() == Op.REMOVE || entry.op() == Op.ERASE) {
                    versions.remove(entry.targetId());
                } else {
                    versions.put(entry.targetId(), entry.sequence());
                }
            }
        }
        return new Base(scope, grant.get(), view.get(), found, last, versions);
    }

    private List<ChangeEntry> log(KnowledgeScope scope) {
        List<ChangeEntry> all = new ArrayList<>();
        long after = 0;
        for (int page = 0; page < LOG_PAGES; page++) {
            List<ChangeEntry> entries;
            try {
                entries = store.changes(scope, after, LOG_PAGE).toCompletableFuture().join();
            } catch (CompletionException e) {
                throw Failures.unavailable("The base could not be read (" + e.getCause().getClass().getSimpleName() + ")");
            }
            all.addAll(entries);
            if (entries.size() < LOG_PAGE) {
                return all;
            }
            after = entries.getLast().sequence();
        }
        throw Failures.unavailable("The change log is longer than a resolution reads");
    }

    private Entry entry(GraphPointer pointer, Base base, int depth) {
        if (base.grant == null) {
            return denied(pointer);
        }
        NodeContent node = base.found.get(pointer.nodeId());
        if (node == null) {
            ChangeEntry last = base.lastEntry.get(pointer.nodeId());
            boolean gone = last != null && (last.op() == Op.REMOVE || last.op() == Op.ERASE)
                    && last.sensitivity() != Sensitivity.RESTRICTED && base.grant.types().contains(last.elementType())
                    && last.sensitivity().compareTo(base.grant.ceiling()) <= 0;
            return gone ? new Entry(pointer, Status.REMOVED, false, 0, null, null, null) : denied(pointer);
        }
        PublishedType published = base.view.types().get(node.type());
        if (published == null || !base.grant.types().contains(node.type())) {
            return denied(pointer);
        }
        long current = base.versions.getOrDefault(node.id(), 0L);
        ObjectNode view = show(node, published, base);
        if (depth == 1) {
            ArrayNode related = view.putArray("related");
            for (NodeContent.Neighbor neighbor : node.neighbors()) {
                NodeContent other = neighbor.node();
                PublishedType shown = base.view.types().get(other.type());
                if (!published.relations().contains(neighbor.relation()) || shown == null
                        || !base.grant.types().contains(other.type())) {
                    continue;   // what does not pass is not there at all
                }
                ObjectNode one = related.addObject();
                one.put("relation", neighbor.relation()).put("direction", neighbor.direction().name());
                one.put("pointer", GraphPointer.of(pointer.scope(), other.id(), base.versions.getOrDefault(other.id(), 1L)).format());
                one.setAll(show(other, shown, base));
            }
        }
        return new Entry(pointer, Status.RESOLVED, pointer.version() < current, current, base.view.version(), view, node.level());
    }

    private static ObjectNode show(NodeContent node, PublishedType published, Base base) {
        ObjectNode view = Json.obj();
        view.put("type", node.type());
        if (published.label()) {
            view.put("label", node.label());
        }
        ObjectNode properties = view.putObject("properties");
        for (String property : new TreeSet<>(published.properties())) {
            String value = node.properties().get(property);
            if (value != null) {
                properties.put(property, value);
            }
        }
        return view;
    }

    private static Entry denied(GraphPointer pointer) {
        return new Entry(pointer, Status.DENIED, false, 0, null, null, null);
    }

    private void record(TaskContext ctx, String namespace, String name, Map<KnowledgeScope, Base> bases,
            Map<Status, Integer> counts, Map<Sensitivity, Integer> byLevel, int depth) {
        Map<String, String> detail = new LinkedHashMap<>();
        detail.put("requester", namespace + ":" + name);
        detail.put("bases", String.join(",", bases.keySet().stream().map(KnowledgeScope::key).toList()));
        for (Status status : Status.values()) {
            detail.put(status.name().toLowerCase(java.util.Locale.ROOT), Integer.toString(counts.getOrDefault(status, 0)));
        }
        for (Sensitivity level : Sensitivity.values()) {
            detail.put("level." + level.wireName(), Integer.toString(byLevel.getOrDefault(level, 0)));
        }
        detail.put("depth", Integer.toString(depth));
        boolean sensitive = bases.values().stream().anyMatch(b -> b.view != null && b.view.allowSensitive());
        if (sensitive) {
            detail.put("allowSensitive", "true");
        }
        audit.append(new AuditLog.AuditRecord(ctx.instanceId(), ctx.chainId(), clock.instant(), AUDIT_KIND, ctx.taskId(),
                ctx.sensitivity(), "resolve-pointers", "answered", detail)).toCompletableFuture().join();
    }
}
