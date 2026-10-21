package io.stintflow.worker.knowledge;

import java.time.InstantSource;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.knowledge.ChangeEntry;
import io.stintflow.knowledge.ChangeEntry.Kind;
import io.stintflow.knowledge.ChangeEntry.Op;
import io.stintflow.knowledge.GraphPointer;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.KnowledgeStore;
import io.stintflow.spi.AuditLog;
import io.stintflow.spi.Sensitivity;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;

/**
 * The task {@code knowledge-announce} (SDD 3c.1, 4.3): which nodes of a base a document has just added or changed, as pointers
 * another domain may hold — and nothing else.
 * <p>
 * <b>Input.</b> {@code {"scope": {"namespace", "base"}, "documentId", "types": [node type], "maxLevel"?: "confidential"}}.
 * <b>Answer.</b> {@code {"items": [{"pointer", "type", "level"}], "upTo", "withheld"}}. Everything comes from the base's change
 * log: the id, the type and the level of a node are in it, its name and properties never are. A node that is restricted is never
 * announced (a pointer to a person says the person is there), nor one above {@code maxLevel} — which only lowers the default —
 * nor one that is gone; only how many were held back is said. The version of a pointer is the node's ({@code GraphPointers}).
 * <p>
 * <b>Audit.</b> A record of kind {@value #AUDIT_KIND} with the base, the counts by level and {@code withheld}; no node id.
 */
public final class KnowledgeAnnounceHandler implements TaskHandler {

    public static final String AUDIT_KIND = "knowledge-announce";

    static final int PAGE = 1_000;
    static final int MAX_PAGES = 100;
    private static final Pattern TYPE = Pattern.compile("[A-Za-z][A-Za-z0-9]{0,40}");
    private static final Logger LOG = LoggerFactory.getLogger(KnowledgeAnnounceHandler.class);

    private final KnowledgeStore store;
    private final ScopeAllowList scopes;
    private final AuditLog audit;
    private final InstantSource clock;

    public KnowledgeAnnounceHandler(KnowledgeStore store, ScopeAllowList scopes, AuditLog audit, InstantSource clock) {
        this.store = Objects.requireNonNull(store, "store");
        this.scopes = Objects.requireNonNull(scopes, "scopes");
        this.audit = audit == null ? AuditLog.NONE : audit;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext ctx) {
        try {
            return CompletableFuture.completedFuture(announce(ctx));
        } catch (TaskFailure failure) {
            return CompletableFuture.failedFuture(failure);
        } catch (RuntimeException e) {
            LOG.warn("Announcement of task {} in instance {} failed ({})", ctx.taskId(), ctx.instanceId(), e.getClass().getName());
            return CompletableFuture.failedFuture(Failures.unavailable("The announcement failed (" + e.getClass().getSimpleName()
                    + ")"));
        }
    }

    private JsonNode announce(TaskContext ctx) {
        JsonNode in = ctx.input();
        KnowledgeScope scope = scopes.require(in);
        String documentId = in.path("documentId").asText("");
        if (documentId.isBlank() || documentId.length() > 200) {
            throw Failures.validation("documentId is required");
        }
        Set<String> types = new LinkedHashSet<>();
        in.path("types").forEach(t -> types.add(t.asText("")));
        if (types.isEmpty() || types.size() > 10 || !types.stream().allMatch(t -> TYPE.matcher(t).matches())) {
            throw Failures.validation("types is one to ten node types");
        }
        Sensitivity ceiling = Sensitivity.CONFIDENTIAL;
        if (in.hasNonNull("maxLevel")) {
            Sensitivity asked = Sensitivity.parse(in.get("maxLevel").asText()).orElseThrow(() ->
                    Failures.validation("maxLevel is internal, confidential or restricted"));
            ceiling = asked.compareTo(ceiling) < 0 ? asked : ceiling;   // it only lowers, and restricted is never announced
        }

        // the last entry of each entity, and the entities this document added or changed
        Map<String, ChangeEntry> last = new LinkedHashMap<>();
        Set<String> touched = new LinkedHashSet<>();
        long upTo = 0;
        long after = 0;
        boolean complete = false;
        for (int page = 0; page < MAX_PAGES && !complete; page++) {
            List<ChangeEntry> entries = read(scope, after);
            for (ChangeEntry entry : entries) {
                upTo = entry.sequence();
                if (entry.kind() == Kind.ENTITY) {
                    last.put(entry.targetId(), entry);
                    if ((entry.op() == Op.ADD || entry.op() == Op.UPDATE) && documentId.equals(entry.documentId())
                            && types.contains(entry.elementType())) {
                        touched.add(entry.targetId());
                    }
                }
            }
            after = upTo;
            complete = entries.size() < PAGE;
        }
        if (!complete) {
            throw Failures.unavailable("The change log is longer than an announcement reads");
        }

        ArrayNode items = Json.MAPPER.createArrayNode();
        Map<Sensitivity, Integer> byLevel = new EnumMap<>(Sensitivity.class);
        int withheld = 0;
        for (String nodeId : touched) {
            ChangeEntry latest = last.get(nodeId);
            if (latest.op() == Op.REMOVE || latest.op() == Op.ERASE) {
                continue;   // gone: there is nothing to point at
            }
            Sensitivity level = latest.sensitivity();
            if (level == Sensitivity.RESTRICTED || level.compareTo(ceiling) > 0) {
                withheld++;
                continue;
            }
            byLevel.merge(level, 1, Integer::sum);
            items.addObject().put("pointer", GraphPointer.of(scope, nodeId, latest.sequence()).format())
                    .put("type", latest.elementType()).put("level", level.wireName());
        }
        ObjectNode out = Json.obj();
        out.set("items", items);
        out.put("upTo", upTo).put("withheld", withheld);
        record(ctx, scope, byLevel, withheld);
        return out;
    }

    private List<ChangeEntry> read(KnowledgeScope scope, long after) {
        try {
            return store.changes(scope, after, PAGE).toCompletableFuture().join();
        } catch (RuntimeException e) {
            Throwable cause = e instanceof CompletionException && e.getCause() != null ? e.getCause() : e;
            throw Failures.unavailable("The base could not be read (" + cause.getClass().getSimpleName() + ")");
        }
    }

    private void record(TaskContext ctx, KnowledgeScope scope, Map<Sensitivity, Integer> byLevel, int withheld) {
        Map<String, String> detail = new LinkedHashMap<>();
        detail.put("base", scope.key());
        for (Sensitivity level : Sensitivity.values()) {
            detail.put(level.wireName(), Integer.toString(byLevel.getOrDefault(level, 0)));
        }
        detail.put("withheld", Integer.toString(withheld));
        audit.append(new AuditLog.AuditRecord(ctx.instanceId(), ctx.chainId(), clock.instant(), AUDIT_KIND, ctx.taskId(),
                ctx.sensitivity(), "knowledge-announce", "answered", detail)).toCompletableFuture().join();
    }
}
