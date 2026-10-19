package io.stintflow.spi;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Port: the audit trail of an instance (SDD 3a.5, RF8) — what the policy allowed or refused, what a redaction found,
 * which provider answered each call of AI. Records carry ids, levels, routes, counts and provider names — never a
 * value of the data. Kept apart from the instance: it outlives it, for its own retention.
 */
public interface AuditLog {

    CompletionStage<Void> append(AuditRecord record);

    /** The records of one instance, oldest first. */
    CompletionStage<List<AuditRecord>> of(String instanceId);

    /** What the engine records with no audit log configured: nothing. */
    AuditLog NONE = new AuditLog() {
        @Override
        public CompletionStage<Void> append(AuditRecord record) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<List<AuditRecord>> of(String instanceId) {
            return CompletableFuture.completedFuture(List.of());
        }
    };

    /**
     * One audited fact.
     *
     * @param instanceId  the instance
     * @param chainId     its business chain (SDD 2.5), or {@code null}
     * @param at          when
     * @param kind        {@code policy}, {@code redaction}, {@code ai-call} or {@code cancel}
     * @param pointer     the task's position
     * @param sensitivity the instance's level at that moment, or {@code null}
     * @param route       the routing key involved, or {@code null}
     * @param outcome     e.g. {@code allowed}, {@code refused}, {@code lowered}, {@code kept}, {@code answered}
     * @param detail      more ids and numbers (provider, model, counts by type…) — never a value of the data
     */
    record AuditRecord(String instanceId, String chainId, Instant at, String kind, String pointer,
            Sensitivity sensitivity, String route, String outcome, Map<String, String> detail) {

        public static final String POLICY = "policy";
        public static final String REDACTION = "redaction";
        public static final String AI_CALL = "ai-call";
        /** SDD 3a.11: a cancellation of an instance (done, or refused when asked by an event of a source not allowed). */
        public static final String CANCEL = "cancel";

        public AuditRecord {
            detail = detail == null ? Map.of() : Map.copyOf(detail);
        }
    }
}
