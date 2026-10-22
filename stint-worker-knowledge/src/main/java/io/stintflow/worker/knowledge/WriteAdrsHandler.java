package io.stintflow.worker.knowledge;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.knowledge.Entity;
import io.stintflow.knowledge.EntityIds;
import io.stintflow.knowledge.GraphPointer;
import io.stintflow.knowledge.KnowledgeBatch;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.KnowledgeStore;
import io.stintflow.knowledge.KnowledgeValidationException;
import io.stintflow.knowledge.PassageIds;
import io.stintflow.knowledge.Relation;
import io.stintflow.knowledge.Source;
import io.stintflow.knowledge.WriteReceipt;
import io.stintflow.spi.AuditLog;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.RetentionClass;
import io.stintflow.spi.Sensitivity;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;

/**
 * The task {@code write-adrs} (SDD 3c.4, 4.5), served by the architecture domain: joins the verdicts of the pairs by decision and
 * writes, for each decision of the draft, the ADR (a markdown document in a blob, for {@code publish-adr}) and, in one batch per
 * meeting, the {@code Decision} and its {@code motivatedBy} relations to the references of the pointers (the same reference that
 * {@code knowledge-reference} writes for a pointer, so the graph holds one) and {@code affects} to the system the draft names.
 * <p>
 * <b>Input.</b> {@code {"meetingId", "draftRef", "viewRef"?, "results": [{"pairId", "verdict", "reviewed"?}], "scope"}}. Every pair
 * of the draft has exactly one result with the verdict {@code conflicts} or {@code compatible}, or the call is refused (400). The
 * status of an ADR is {@code conflicting} when any of its pairs is, {@code proposed} otherwise.
 * <p>
 * <b>What the ADR carries from the product domain:</b> the pointer and the label the view published, nothing else — never the
 * summary or the status of the product decision. The decision is {@code confidential}, the level of the meeting.
 */
public final class WriteAdrsHandler implements TaskHandler {

    public static final String AUDIT_KIND = "adr-write";
    public static final String SCHEMA_VERSION = "architecture@1.1.0";
    public static final String DECISION_TYPE = "Decision";
    public static final String REFERENCE_TYPE = "DomainReference";
    public static final String MOTIVATED_BY = "motivatedBy";
    public static final String AFFECTS = "affects";

    private static final Logger LOG = LoggerFactory.getLogger(WriteAdrsHandler.class);

    private final KnowledgeStore store;
    private final BlobStore blobs;
    private final ScopeAllowList scopes;
    private final OriginAllowList origins;
    private final RetentionClass documentClass;
    private final AuditLog audit;
    private final InstantSource clock;

    public WriteAdrsHandler(KnowledgeStore store, BlobStore blobs, ScopeAllowList scopes, OriginAllowList origins,
            RetentionClass documentClass, AuditLog audit, InstantSource clock) {
        this.store = Objects.requireNonNull(store, "store");
        this.blobs = Objects.requireNonNull(blobs, "blobs");
        this.scopes = Objects.requireNonNull(scopes, "scopes");
        this.origins = Objects.requireNonNull(origins, "origins");
        this.documentClass = Objects.requireNonNull(documentClass, "documentClass");
        this.audit = audit == null ? AuditLog.NONE : audit;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext ctx) {
        try {
            return CompletableFuture.completedFuture(write(ctx));
        } catch (TaskFailure failure) {
            return CompletableFuture.failedFuture(failure);
        } catch (RuntimeException e) {
            LOG.warn("ADR write of task {} in instance {} failed ({})", ctx.taskId(), ctx.instanceId(), e.getClass().getName());
            return CompletableFuture.failedFuture(Failures.unavailable("The ADRs could not be written ("
                    + e.getClass().getSimpleName() + ")"));
        }
    }

    private record Verdict(String value, boolean reviewed) {
    }

    private JsonNode write(TaskContext ctx) {
        JsonNode in = ctx.input();
        KnowledgeScope scope = scopes.require(in);
        String meetingId = in.path("meetingId").asText("");
        if (meetingId.isBlank() || meetingId.length() > 200) {
            throw Failures.validation("meetingId is required");
        }
        List<AdrDrafts.Decision> decisions = AdrDrafts.read(blobs, in.path("draftRef").asText(""), origins);
        Map<String, Verdict> verdicts = new LinkedHashMap<>();
        for (JsonNode result : in.path("results")) {
            String value = result.path("verdict").asText("");
            if (!value.equals("conflicts") && !value.equals("compatible")) {
                throw Failures.validation("a verdict is conflicts or compatible");
            }
            if (verdicts.put(result.path("pairId").asText(""), new Verdict(value, result.path("reviewed").asBoolean(false))) != null) {
                throw Failures.validation("a pair has two results");
            }
        }
        int pairs = decisions.stream().mapToInt(d -> d.motivatedBy().size()).sum();
        if (verdicts.size() != pairs) {
            throw Failures.validation("Every pair of the draft has one result");
        }
        String viewRef = in.path("viewRef").asText(null);

        List<Entity> entities = new ArrayList<>();
        List<Relation> relations = new ArrayList<>();
        Map<String, Entity> shared = new LinkedHashMap<>();
        ObjectNode out = Json.obj();
        ArrayNode adrs = out.putArray("adrs");
        int conflicting = 0;
        int reviewedCount = 0;
        for (AdrDrafts.Decision decision : decisions) {
            List<String> lines = new ArrayList<>();
            boolean conflicts = false;
            boolean reviewed = false;
            StringBuilder motivated = new StringBuilder();
            for (int i = 0; i < decision.motivatedBy().size(); i++) {
                GraphPointer pointer = decision.motivatedBy().get(i);
                Verdict verdict = verdicts.get(AdrDrafts.pairId(decision.id(), i));
                if (verdict == null) {
                    throw Failures.validation("Every pair of the draft has one result");
                }
                conflicts |= verdict.value().equals("conflicts");
                reviewed |= verdict.reviewed();
                String label = AdrDrafts.resolved(blobs, viewRef, pointer).map(v -> v.path("label").asText("")).orElse("");
                motivated.append("- ").append(pointer.format()).append(" — ").append(label.isBlank() ? "(not available)" : label)
                        .append('\n');
                lines.add("- " + pointer.format() + ": " + verdict.value() + (verdict.reviewed() ? " (reviewed)" : ""));
                Entity reference = reference(pointer);
                shared.putIfAbsent(reference.entityId(), reference);
                relations.add(new Relation(MOTIVATED_BY, EntityIds.of(DECISION_TYPE, decision.id()), reference.entityId(),
                        Sensitivity.CONFIDENTIAL));
            }
            String status = conflicts ? "conflicting" : "proposed";
            conflicting += conflicts ? 1 : 0;
            reviewedCount += reviewed ? 1 : 0;
            Map<String, String> properties = new LinkedHashMap<>();
            properties.put("key", decision.id());
            properties.put("summary", decision.summary());
            properties.put("status", status);
            entities.add(new Entity(EntityIds.of(DECISION_TYPE, decision.id()), DECISION_TYPE, decision.title(), properties,
                    Sensitivity.CONFIDENTIAL));
            if (decision.affects() != null) {
                Entity system = new Entity(EntityIds.of("System", decision.affects()), "System", decision.affects(), Sensitivity.INTERNAL);
                shared.putIfAbsent(system.entityId(), system);
                relations.add(new Relation(AFFECTS, EntityIds.of(DECISION_TYPE, decision.id()), system.entityId(),
                        Sensitivity.CONFIDENTIAL));
            }
            String document = "# ADR " + decision.id() + ": " + decision.title() + "\n\nStatus: " + status + "\n\n## Decision\n"
                    + decision.summary() + "\n\n## Motivated by\n" + motivated + "\n## Conflict check\n" + String.join("\n", lines)
                    + "\n";
            URI documentRef = blobs.put(document.getBytes(StandardCharsets.UTF_8),
                    "adr/" + ctx.instanceId() + "-" + decision.id() + ".md", documentClass).toCompletableFuture().join();
            ObjectNode adr = adrs.addObject();
            adr.put("decisionId", decision.id()).put("title", decision.title()).put("status", status).put("reviewed", reviewed)
                    .put("documentRef", documentRef.toString());
        }
        entities.addAll(shared.values());

        String hash = PassageIds.sha256Hex(entities.toString() + relations);
        KnowledgeBatch batch = new KnowledgeBatch("adr:" + ctx.instanceId() + ":" + ctx.taskId(), "adr:" + meetingId, hash, null,
                new Source(ctx.instanceId(), ctx.chainId(), ctx.definition()), SCHEMA_VERSION, List.of(), entities, relations,
                List.of());
        WriteReceipt receipt;
        try {
            receipt = store.write(scope, batch).toCompletableFuture().join();
        } catch (RuntimeException e) {
            Throwable cause = e instanceof CompletionException && e.getCause() != null ? e.getCause() : e;
            if (cause instanceof KnowledgeValidationException invalid) {
                throw Failures.validation("The base refused the ADRs: " + invalid.getMessage());
            }
            throw Failures.unavailable("The base could not be written (" + cause.getClass().getSimpleName() + ")");
        }
        out.put("status", receipt.status().name());
        record(ctx, scope, Map.of("decisions", Integer.toString(decisions.size()), "pairs", Integer.toString(pairs),
                "conflicting", Integer.toString(conflicting), "reviewed", Integer.toString(reviewedCount), "status",
                receipt.status().name()));
        return out;
    }

    /** The reference of a pointer, the same one {@code knowledge-reference} writes: its id, its label and its properties. */
    private static Entity reference(GraphPointer pointer) {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("pointer", pointer.format());
        properties.put("remoteType", DECISION_TYPE);
        properties.put("domain", pointer.scope().key());
        properties.put("observedVersion", Long.toString(pointer.version()));
        return new Entity(EntityIds.of(REFERENCE_TYPE, pointer.nodeKey()), REFERENCE_TYPE,
                DECISION_TYPE + " @ " + pointer.scope().key() + " " + PassageIds.sha256Hex(pointer.nodeId()).substring(0, 8),
                properties, Sensitivity.CONFIDENTIAL);
    }

    private void record(TaskContext ctx, KnowledgeScope scope, Map<String, String> more) {
        Map<String, String> detail = new LinkedHashMap<>(more);
        detail.put("base", scope.key());
        audit.append(new AuditLog.AuditRecord(ctx.instanceId(), ctx.chainId(), clock.instant(), AUDIT_KIND, ctx.taskId(),
                ctx.sensitivity(), "write-adrs", "written", detail)).toCompletableFuture().join();
    }
}
