package io.stintflow.worker.langchain4j;

import java.time.InstantSource;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.rag.content.Content;
import dev.langchain4j.rag.content.retriever.ContentRetriever;
import dev.langchain4j.rag.query.Query;
import io.stintflow.knowledge.Audience;
import io.stintflow.knowledge.ContextBudget;
import io.stintflow.knowledge.ContextItem;
import io.stintflow.knowledge.KnowledgeReadAudit;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.KnowledgeSeeds;
import io.stintflow.knowledge.KnowledgeStore;
import io.stintflow.knowledge.RetrievalQuery;
import io.stintflow.spi.AuditLog;
import io.stintflow.spi.ModelProvider;
import io.stintflow.spi.Sensitivity;
import io.stintflow.worker.TaskContext;

/**
 * SDD 3b.4, 4.5: a LangChain4j {@link ContentRetriever} over the {@link KnowledgeStore} port — and only that port, so it can read
 * and nothing else. <b>One per task</b>: the ceiling is worked out when it is built, from the level of the instance and the model
 * the context goes to ({@link Audience#ceiling}), and never changes, so a retriever is never shared between instances of different
 * levels. Nothing above it is returned, and nothing that tells it is there.
 * <p>
 * A question is answered as {@code auto}: relational — the passages that mention what it names, and the graph around that, to
 * {@link Builder#hops} — when it names an entity the base knows ({@link KnowledgeSeeds}), else by meaning. The content is a
 * {@link TextSegment} per item, and its metadata is the origin to cite: {@code kind}, {@code id}, {@code level}, {@code score}, and
 * {@code documentId}, {@code instanceId}, {@code chainId} for a passage or {@code evidence} (document ids, comma separated) for
 * what the graph adds. A failure of the base is an empty answer and a warning with the class name only: the caller carries on
 * without context instead of failing.
 */
public final class KnowledgeContentRetriever implements ContentRetriever {

    private static final Logger LOG = LoggerFactory.getLogger(KnowledgeContentRetriever.class);

    private final KnowledgeStore store;
    private final KnowledgeScope scope;
    private final Sensitivity ceiling;
    private final int topK;
    private final int hops;
    private final Integer budgetTokens;
    private final Set<String> excluded;
    private final AuditLog audit;
    private final TaskContext task;
    private final InstantSource clock;

    private KnowledgeContentRetriever(Builder b) {
        this.store = b.store;
        this.scope = b.scope;
        this.ceiling = Audience.ceiling(b.instanceLevel, b.provider, b.approvedRemote);
        this.topK = b.topK;
        this.hops = b.hops;
        this.budgetTokens = b.budgetTokens;
        this.excluded = Set.copyOf(b.excluded);
        this.audit = b.audit;
        this.task = b.task;
        this.clock = b.clock;
    }

    public static Builder builder(KnowledgeStore store, KnowledgeScope scope) {
        return new Builder(store, scope);
    }

    /** The highest level this retriever returns. */
    public Sensitivity ceiling() {
        return ceiling;
    }

    @Override
    public List<Content> retrieve(Query query) {
        if (query == null || query.text() == null || query.text().isBlank()) {
            return List.of();
        }
        try {
            return answer(query.text());
        } catch (RuntimeException e) {
            Throwable cause = e.getCause() != null && e instanceof java.util.concurrent.CompletionException ? e.getCause() : e;
            LOG.warn("The knowledge base could not be read for a retrieval ({}); going on without context",
                    cause.getClass().getName());
            return List.of();
        }
    }

    private List<Content> answer(String question) {
        Set<String> seeds = KnowledgeSeeds.find(store, scope, question, null, ceiling);
        String mode = seeds.isEmpty() ? "semantic" : "relational";
        int hopsUsed = mode.equals("relational") ? hops : Math.min(hops, 1);
        // a few more than asked: the excluded documents come out afterwards
        RetrievalQuery asked = new RetrievalQuery(question, Math.min(RetrievalQuery.MAX_TOP_K, topK + excluded.size() + 5), 0,
                ceiling, null, seeds, null, hopsUsed);
        List<ContextItem> found = store.retrieve(scope, asked).toCompletableFuture().join();

        List<ContextItem> kept = new ArrayList<>();
        for (ContextItem item : found) {
            ContextItem allowed = withoutExcluded(item);
            if (allowed != null) {
                kept.add(allowed);
            }
        }
        ContextBudget.Fit fit = ContextBudget.fit(kept, budgetTokens);
        record(asked, mode, fit);
        List<Content> out = new ArrayList<>();
        for (ContextItem item : fit.kept()) {
            out.add(Content.from(TextSegment.from(item.content(), metadata(item))));
        }
        return out;
    }

    /** The item, without the excluded documents in its evidence; {@code null} when it came only from them. */
    private ContextItem withoutExcluded(ContextItem item) {
        ContextItem.Origin origin = item.origin();
        if (origin.documentId() != null) {
            return excluded.contains(origin.documentId()) ? null : item;
        }
        if (origin.evidence().isEmpty()) {
            return item;
        }
        List<String> evidence = origin.evidence().stream().filter(d -> !excluded.contains(d)).toList();
        if (evidence.isEmpty()) {
            return null;
        }
        return evidence.size() == origin.evidence().size() ? item : new ContextItem(item.content(),
                new ContextItem.Origin(origin.kind(), origin.id(), origin.documentId(), origin.scope(), origin.instanceId(),
                        origin.chainId(), evidence), item.score(), item.entities(), item.sensitivity());
    }

    private static Metadata metadata(ContextItem item) {
        ContextItem.Origin origin = item.origin();
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("kind", origin.kind().name());
        values.put("id", origin.id());
        values.put("level", item.sensitivity().wireName());
        values.put("score", item.score());
        if (origin.documentId() != null) {
            values.put("documentId", origin.documentId());
        }
        if (origin.instanceId() != null) {
            values.put("instanceId", origin.instanceId());
        }
        if (origin.chainId() != null) {
            values.put("chainId", origin.chainId());
        }
        if (!origin.evidence().isEmpty()) {
            values.put("evidence", String.join(",", origin.evidence()));
        }
        return Metadata.from(values);
    }

    private void record(RetrievalQuery asked, String mode, ContextBudget.Fit fit) {
        if (audit == null) {
            return;
        }
        try {
            KnowledgeReadAudit.record(audit, task.instanceId(), task.chainId(), clock.instant(), task.taskId(),
                    task.sensitivity(), "knowledge-retriever", scope,
                    new RetrievalQuery(asked.text(), topK, 0, asked.ceiling(), null, Set.of(), null, asked.hops()), mode,
                    fit.trimmed(), KnowledgeReadAudit.countByLevel(fit.kept()));
        } catch (RuntimeException e) {
            LOG.warn("The audit of a knowledge read of task {} could not be written ({})", task.taskId(),
                    e.getClass().getName());
        }
    }

    public static final class Builder {

        private final KnowledgeStore store;
        private final KnowledgeScope scope;
        private Sensitivity instanceLevel;
        private ModelProvider provider;
        private boolean approvedRemote;
        private int topK = 5;
        private int hops = 2;
        private Integer budgetTokens;
        private Collection<String> excluded = Set.of();
        private AuditLog audit;
        private TaskContext task;
        private InstantSource clock = InstantSource.system();

        private Builder(KnowledgeStore store, KnowledgeScope scope) {
            this.store = Objects.requireNonNull(store, "store");
            this.scope = Objects.requireNonNull(scope, "scope");
        }

        /** The level of the instance asking; absent reads as restricted. */
        public Builder instanceLevel(Sensitivity level) {
            this.instanceLevel = level;
            return this;
        }

        /** The model the context goes to, and whether it is on the approved list when it is remote. Absent: a model that stays. */
        public Builder provider(ModelProvider provider) {
            this.provider = provider;
            return this;
        }

        public Builder approvedRemote(boolean approvedRemote) {
            this.approvedRemote = approvedRemote;
            return this;
        }

        /** How many passages at most (1 to {@value RetrievalQuery#MAX_TOP_K}); the graph's items come on top of them. */
        public Builder topK(int topK) {
            this.topK = topK;
            return this;
        }

        /** How far the graph is followed when the question names something the base knows (0 to {@value RetrievalQuery#MAX_HOPS}). */
        public Builder hops(int hops) {
            this.hops = hops;
            return this;
        }

        /** The estimated tokens at most ({@link ContextBudget}); absent is no cut. */
        public Builder budgetTokens(int tokens) {
            this.budgetTokens = tokens;
            return this;
        }

        /** Documents never returned (what is being written about now). */
        public Builder excludeDocuments(Collection<String> documentIds) {
            this.excluded = Set.copyOf(documentIds);
            return this;
        }

        /** Records each read ({@link KnowledgeReadAudit}) for {@code task}. */
        public Builder audit(AuditLog audit, TaskContext task, InstantSource clock) {
            this.audit = Objects.requireNonNull(audit, "audit");
            this.task = Objects.requireNonNull(task, "task");
            this.clock = Objects.requireNonNull(clock, "clock");
            return this;
        }

        public KnowledgeContentRetriever build() {
            new RetrievalQuery("-", topK, Sensitivity.INTERNAL);   // the same limits as the port
            if (hops < 0 || hops > RetrievalQuery.MAX_HOPS) {
                throw new IllegalArgumentException("hops is 0 to " + RetrievalQuery.MAX_HOPS);
            }
            return new KnowledgeContentRetriever(this);
        }
    }
}
