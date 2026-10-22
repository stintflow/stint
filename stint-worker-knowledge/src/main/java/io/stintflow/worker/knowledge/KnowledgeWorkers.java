package io.stintflow.worker.knowledge;

import java.time.InstantSource;
import java.util.Set;

import io.stintflow.knowledge.Embedder;
import io.stintflow.knowledge.KnowledgeStore;
import io.stintflow.spi.AreaKeys;
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
    public static final String AREA_KEY_DESTROY_ROUTE = "area-key-destroy";
    public static final String ANNOUNCE_ROUTE = "knowledge-announce";
    public static final String REFERENCE_ROUTE = "knowledge-reference";
    public static final String RESOLVE_ROUTE = "resolve-pointers";
    public static final String CATALOG_LOOKUP_ROUTE = "catalog-lookup";
    public static final String CATALOG_MENTIONS_ROUTE = "catalog-mentions";
    public static final String CATALOG_LINK_ROUTE = "catalog-link";
    public static final String PAIR_DECISIONS_ROUTE = "pair-decisions";
    public static final String PAIR_MATERIAL_ROUTE = "pair-material";
    public static final String WRITE_ADRS_ROUTE = "write-adrs";

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

    /**
     * SDD 3c.1: registers {@code knowledge-announce} — the pointers to the nodes a document of one of {@code scopes} added, for
     * another domain to hold.
     */
    public static TaskHandlerRegistry registerAnnounce(TaskHandlerRegistry registry, KnowledgeStore store, ScopeAllowList scopes,
            AuditLog audit, InstantSource clock) {
        return registry.register(ANNOUNCE_ROUTE, new KnowledgeAnnounceHandler(store, scopes, audit, clock));
    }

    /**
     * SDD 3c.1: registers {@code knowledge-reference} — the references to nodes of another domain, written in one of
     * {@code scopes}, from announcements of the domains in {@code origins} only (an empty list accepts none).
     */
    public static TaskHandlerRegistry registerReference(TaskHandlerRegistry registry, KnowledgeStore store, ScopeAllowList scopes,
            OriginAllowList origins, AuditLog audit, InstantSource clock) {
        return registry.register(REFERENCE_ROUTE, new KnowledgeReferenceHandler(store, scopes, origins, audit, clock));
    }

    /**
     * SDD 3c.2: registers {@code resolve-pointers} — this origin answers the pointers of other domains by {@code policy}: the
     * content of a node through the view it publishes, to the definitions it grants, into a blob of {@code viewClass} (or the
     * default of one day).
     */
    public static TaskHandlerRegistry registerResolve(TaskHandlerRegistry registry, KnowledgeStore store,
            io.stintflow.knowledge.KnowledgeNodes nodes, ResolutionPolicy policy, BlobStore blobs,
            io.stintflow.spi.RetentionClass viewClass, AuditLog audit, InstantSource clock) {
        return registry.register(RESOLVE_ROUTE, new ResolvePointersHandler(store, nodes, policy, blobs, viewClass, audit, clock));
    }

    /**
     * SDD 3c.3: registers {@code catalog-lookup} — this domain holds the catalog of entities and answers, to the definitions in
     * {@code requesters}, which node of the catalog a name is (an exact match, or {@code NONE}).
     */
    public static TaskHandlerRegistry registerCatalog(TaskHandlerRegistry registry, KnowledgeStore store, ScopeAllowList scopes,
            CatalogRequesters requesters, AuditLog audit, InstantSource clock) {
        return registry.register(CATALOG_LOOKUP_ROUTE, new CatalogLookupHandler(store, scopes, requesters, audit, clock));
    }

    /**
     * SDD 3c.3: registers {@code catalog-mentions} and {@code catalog-link} — an area names its own nodes of the types the catalog
     * holds, and links them to the catalog's nodes by pointer, from the catalogs of the domains in {@code origins} only.
     */
    public static TaskHandlerRegistry registerCatalogLinks(TaskHandlerRegistry registry, KnowledgeStore store,
            io.stintflow.knowledge.KnowledgeInspector inspector, io.stintflow.knowledge.KnowledgeNodes nodes, ScopeAllowList scopes,
            OriginAllowList origins, AuditLog audit, InstantSource clock) {
        return registry.register(CATALOG_MENTIONS_ROUTE, new CatalogMentionsHandler(inspector, scopes))
                .register(CATALOG_LINK_ROUTE, new CatalogLinkHandler(store, nodes, scopes, origins, audit, clock));
    }

    /**
     * SDD 3c.4: registers {@code pair-decisions}, {@code pair-material} and {@code write-adrs} — the architecture domain reads the
     * draft of the decisions of a meeting, puts each pair in a blob of {@code materialClass}, and writes the ADRs (documents in
     * {@code documentClass}) and the decisions of the graph, listening to the domains in {@code origins} only.
     */
    public static TaskHandlerRegistry registerAdrs(TaskHandlerRegistry registry, KnowledgeStore store, BlobStore blobs,
            ScopeAllowList scopes, OriginAllowList origins, io.stintflow.spi.RetentionClass materialClass,
            io.stintflow.spi.RetentionClass documentClass, AuditLog audit, InstantSource clock) {
        return registry.register(PAIR_DECISIONS_ROUTE, new PairDecisionsHandler(blobs, origins))
                .register(PAIR_MATERIAL_ROUTE, new PairMaterialHandler(blobs, origins, materialClass))
                .register(WRITE_ADRS_ROUTE, new WriteAdrsHandler(store, blobs, scopes, origins, documentClass, audit, clock));
    }

    /**
     * SDD 3b.5b: registers {@code area-key-destroy}, which destroys the key of one of {@code areas} (and so makes unreadable
     * every copy of the blobs encrypted under it). An empty list serves no area.
     */
    public static TaskHandlerRegistry registerAreaKeys(TaskHandlerRegistry registry, AreaKeys keys, Set<String> areas,
            AuditLog audit, InstantSource clock) {
        return registry.register(AREA_KEY_DESTROY_ROUTE, new AreaKeyDestroyHandler(keys, areas, audit, clock));
    }
}
