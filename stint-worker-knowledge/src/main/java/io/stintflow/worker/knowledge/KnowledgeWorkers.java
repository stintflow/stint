package io.stintflow.worker.knowledge;

import java.time.InstantSource;

import io.stintflow.knowledge.KnowledgeStore;
import io.stintflow.spi.AuditLog;
import io.stintflow.spi.BlobStore;
import io.stintflow.worker.TaskHandlerRegistry;

/** The routes of the knowledge tasks (SDD 3b.3), registered together. The route {@code extract-knowledge} lives in the LLM worker module. */
public final class KnowledgeWorkers {

    public static final String WRITE_ROUTE = "knowledge-write";
    public static final String RETRIEVE_ROUTE = "knowledge-retrieve";

    private KnowledgeWorkers() {
    }

    /** Registers {@code knowledge-write} and {@code knowledge-retrieve} over {@code store}, serving only {@code scopes}. */
    public static TaskHandlerRegistry register(TaskHandlerRegistry registry, KnowledgeStore store, BlobStore blobs,
            ScopeAllowList scopes, AuditLog audit, InstantSource clock) {
        return registry.register(WRITE_ROUTE, new KnowledgeWriteHandler(store, blobs, scopes))
                .register(RETRIEVE_ROUTE, new KnowledgeRetrieveHandler(store, scopes, audit, clock));
    }
}
