package io.stintflow.worker.knowledge;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.knowledge.CatalogTypes;
import io.stintflow.knowledge.GraphView;
import io.stintflow.knowledge.KnowledgeInspector;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.NodeView;
import io.stintflow.knowledge.ViewQuery;
import io.stintflow.spi.Sensitivity;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;

/**
 * The task {@code catalog-mentions} (SDD 3c.3, 4.3), served by an area: the names of its own nodes of the types the catalog holds
 * ({@code System}, {@code Product}, {@code Team}), for the workflow to ask the catalog what they are. It reads only what the
 * inspection port shows — id, type, level and label, no property — and only {@code internal} nodes come back: what is above is
 * counted in {@code skipped} by level and never named, and a restricted node is never seen.
 * <p>
 * <b>Input.</b> {@code {"scope": {"namespace", "base"}, "types"?: [...], "after"?: "<node id>"}}. <b>Answer.</b> {@code {"mentions":
 * [{"entityId", "type", "name"}], "skipped": {"<level>": n}, "next": "<node id>" | null}}, at most {@value #MAX_MENTIONS} a call;
 * {@code next} is the {@code after} of the following one.
 */
public final class CatalogMentionsHandler implements TaskHandler {

    static final int MAX_MENTIONS = 50;
    private static final int PAGE = 200;
    private static final Logger LOG = LoggerFactory.getLogger(CatalogMentionsHandler.class);

    private final KnowledgeInspector inspector;
    private final ScopeAllowList scopes;

    public CatalogMentionsHandler(KnowledgeInspector inspector, ScopeAllowList scopes) {
        this.inspector = Objects.requireNonNull(inspector, "inspector");
        this.scopes = Objects.requireNonNull(scopes, "scopes");
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext ctx) {
        try {
            return CompletableFuture.completedFuture(mentions(ctx));
        } catch (TaskFailure failure) {
            return CompletableFuture.failedFuture(failure);
        } catch (RuntimeException e) {
            LOG.warn("Catalog mentions of task {} in instance {} failed ({})", ctx.taskId(), ctx.instanceId(), e.getClass().getName());
            return CompletableFuture.failedFuture(Failures.unavailable("The base could not be read ("
                    + e.getClass().getSimpleName() + ")"));
        }
    }

    private JsonNode mentions(TaskContext ctx) {
        JsonNode in = ctx.input();
        KnowledgeScope scope = scopes.require(in);
        Set<String> types = CatalogTypes.NAMES;
        if (in.hasNonNull("types")) {
            if (!in.get("types").isArray() || in.get("types").isEmpty()) {
                throw Failures.validation("types is a list of node types");
            }
            Set<String> asked = new java.util.TreeSet<>();
            in.get("types").forEach(t -> asked.add(t.asText("")));
            if (!CatalogTypes.NAMES.containsAll(asked)) {
                throw Failures.validation("types are of " + CatalogTypes.NAMES.stream().sorted().toList());
            }
            types = asked;
        }
        String after = in.hasNonNull("after") ? in.get("after").asText() : null;

        ObjectNode out = Json.obj();
        ArrayNode mentions = out.putArray("mentions");
        Map<String, Integer> skipped = new TreeMap<>();
        String next = null;
        boolean more = true;
        while (more && mentions.size() < MAX_MENTIONS) {
            GraphView page;
            try {
                page = inspector.view(scope, new ViewQuery(Sensitivity.CONFIDENTIAL, PAGE, after, null)).toCompletableFuture().join();
            } catch (CompletionException e) {
                throw Failures.unavailable("The base could not be read (" + e.getCause().getClass().getSimpleName() + ")");
            }
            for (NodeView node : page.nodes()) {
                if (mentions.size() == MAX_MENTIONS) {
                    next = after;
                    break;
                }
                after = node.id();
                if (!types.contains(node.type())) {
                    continue;
                }
                if (node.level() != Sensitivity.INTERNAL) {
                    skipped.merge(node.level().name().toLowerCase(java.util.Locale.ROOT), 1, Integer::sum);
                } else if (node.label() != null) {
                    mentions.addObject().put("entityId", node.id()).put("type", node.type()).put("name", node.label());
                }
            }
            more = page.next() != null;
            if (!more) {
                after = null;
            }
        }
        if (more && next == null) {
            next = after;
        }
        ObjectNode skippedOut = out.putObject("skipped");
        skipped.forEach(skippedOut::put);
        if (next == null) {
            out.putNull("next");
        } else {
            out.put("next", next);
        }
        return out;
    }
}
