package io.stintflow.knowledge;

import java.time.Instant;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.stintflow.spi.AuditLog;
import io.stintflow.spi.Sensitivity;

/**
 * SDD 3b.3, 4.6 and SDD 3b.4, 4.2: the record of a read of the base, written the same way by the task and by the content
 * retriever. Base, ceiling, {@code topK}, mode, {@code hops}, how many were left out by the budget, and the counts by level — never
 * the question and never a content.
 */
public final class KnowledgeReadAudit {

    /** The kind of the audit record of a read of the base. */
    public static final String KIND = "knowledge-read";

    private KnowledgeReadAudit() {
    }

    public static Map<Sensitivity, Integer> countByLevel(List<ContextItem> items) {
        Map<Sensitivity, Integer> byLevel = new EnumMap<>(Sensitivity.class);
        items.forEach(item -> byLevel.merge(item.sensitivity(), 1, Integer::sum));
        return byLevel;
    }

    /** Appends the record; the call waits for the log. */
    public static void record(AuditLog audit, String instanceId, String chainId, Instant at, String taskId,
            Sensitivity instanceLevel, String route, KnowledgeScope scope, RetrievalQuery query, String mode, int trimmed,
            Map<Sensitivity, Integer> byLevel) {
        Map<String, String> detail = new LinkedHashMap<>();
        detail.put("base", scope.key());
        detail.put("topK", Integer.toString(query.topK()));
        detail.put("ceiling", query.ceiling().wireName());
        detail.put("mode", mode);
        detail.put("hops", Integer.toString(query.hops()));
        detail.put("trimmed", Integer.toString(trimmed));
        for (Sensitivity level : Sensitivity.values()) {
            detail.put(level.wireName(), Integer.toString(byLevel.getOrDefault(level, 0)));
        }
        audit.append(new AuditLog.AuditRecord(instanceId, chainId, at, KIND, taskId, instanceLevel, route, "answered", detail))
                .toCompletableFuture().join();
    }
}
