package io.stintflow.worker.knowledge;

import java.time.InstantSource;

import io.stintflow.knowledge.Embedder;
import io.stintflow.knowledge.KnowledgeStore;
import io.stintflow.spi.AuditLog;
import io.stintflow.spi.BlobStore;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.redaction.Pseudonyms;

/** The routes of the knowledge tasks (SDD 3b.3), registered together. The route {@code extract-knowledge} lives in the LLM worker module. */
public final class KnowledgeWorkers {

    public static final String WRITE_ROUTE = "knowledge-write";
    public static final String RETRIEVE_ROUTE = "knowledge-retrieve";
    public static final String ERASE_ROUTE = "knowledge-erase";
    public static final String VERIFY_ROUTE = "knowledge-verify";
    public static final String REINDEX_ROUTE = "knowledge-reindex";
    public static final String RESOLVE_PERSON_ROUTE = "resolve-person";
    public static final String FORGET_SUBJECT_ROUTE = "forget-subject";

    private KnowledgeWorkers() {
    }

    /** Registers {@code knowledge-write} and {@code knowledge-retrieve} over {@code store}, serving only {@code scopes}. */
    public static TaskHandlerRegistry register(TaskHandlerRegistry registry, KnowledgeStore store, BlobStore blobs,
            ScopeAllowList scopes, AuditLog audit, InstantSource clock) {
        return registry.register(WRITE_ROUTE, new KnowledgeWriteHandler(store, blobs, scopes))
                .register(RETRIEVE_ROUTE, new KnowledgeRetrieveHandler(store, scopes, audit, clock));
    }

    /**
     * SDD 3b.5: registers {@code knowledge-erase}, {@code knowledge-verify} and {@code forget-subject}, and — when given — {@code
     * knowledge-reindex} (the embedder this worker serves) and {@code resolve-person} (the secret of the stable pseudonyms,
     * which only the worker that holds it can serve).
     *
     * @param embedder   the embedding model this worker moves a base to, or {@code null}: then there is no reindex route
     * @param pseudonyms the redactor's secret, or {@code null}: then there is no way to find a person from who they are
     */
    public static TaskHandlerRegistry registerErasure(TaskHandlerRegistry registry, KnowledgeStore store, BlobStore blobs,
            ScopeAllowList scopes, AuditLog audit, InstantSource clock, Embedder embedder, Pseudonyms pseudonyms) {
        registry.register(ERASE_ROUTE, new KnowledgeEraseHandler(store, scopes, audit, clock))
                .register(VERIFY_ROUTE, new KnowledgeVerifyHandler(store, scopes))
                .register(FORGET_SUBJECT_ROUTE, new ForgetSubjectHandler(blobs));
        if (embedder != null) {
            registry.register(REINDEX_ROUTE, new KnowledgeReindexHandler(store, embedder, scopes, audit, clock));
        }
        if (pseudonyms != null) {
            registry.register(RESOLVE_PERSON_ROUTE, new ResolvePersonHandler(pseudonyms, blobs, scopes));
        }
        return registry;
    }
}
