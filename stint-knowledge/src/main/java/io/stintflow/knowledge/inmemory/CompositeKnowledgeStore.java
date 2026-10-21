package io.stintflow.knowledge.inmemory;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import io.stintflow.knowledge.BaseInfo;
import io.stintflow.knowledge.ChangeEntry;
import io.stintflow.knowledge.ChangeEntry.Kind;
import io.stintflow.knowledge.ChangeEntry.Op;
import io.stintflow.knowledge.ContextItem;
import io.stintflow.knowledge.DeleteReceipt;
import io.stintflow.knowledge.DeleteRequest;
import io.stintflow.knowledge.Embedder;
import io.stintflow.knowledge.ErasureCheck;
import io.stintflow.knowledge.Entity;
import io.stintflow.knowledge.EntityIds;
import io.stintflow.knowledge.EntityMatch;
import io.stintflow.knowledge.EntityQuery;
import io.stintflow.knowledge.EdgeView;
import io.stintflow.knowledge.GraphView;
import io.stintflow.knowledge.KnowledgeBatch;
import io.stintflow.knowledge.KnowledgeInspector;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.KnowledgeStore;
import io.stintflow.knowledge.KnowledgeValidationException;
import io.stintflow.knowledge.NodeView;
import io.stintflow.knowledge.ViewQuery;
import io.stintflow.knowledge.Link;
import io.stintflow.knowledge.Passage;
import io.stintflow.knowledge.PassageIds;
import io.stintflow.knowledge.Relation;
import io.stintflow.knowledge.ReindexReceipt;
import io.stintflow.knowledge.ReindexRequest;
import io.stintflow.knowledge.RetrievalQuery;
import io.stintflow.knowledge.Source;
import io.stintflow.knowledge.WriteReceipt;
import io.stintflow.spi.BlobNotFoundException;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.RetentionClass;
import io.stintflow.spi.Sensitivity;

/**
 * The reference {@link KnowledgeStore} (SDD 3b.1, 4.9): a vector index, a graph and a change log, all in memory, written
 * together.
 * <p>
 * <b>Atomicity (4.5).</b> A scope's whole state — index, graph, log, documents, tombstones — is immutable once published.
 * A write (or an erasure) copies it, changes the copy under the scope's lock, appends the log entries to the copy, and only
 * then publishes the copy in one assignment. Readers take the published state and never see a half: the change and its log
 * entries happen together, or (a failure before the assignment) neither. {@link #faultBeforeSwap} injects that failure.
 * <p>
 * <b>E1 (4.4).</b> The text of a restricted passage and the label and properties of a restricted entity are put in the
 * {@link BlobStore} under the retention class and nowhere else: the index, the graph and the log hold ids, levels and the
 * pointer. A store with no class refuses to write a restricted item. A vector is derived data and is kept at the item's level.
 * <p>
 * Meant for tests and the local mode: it copies a scope's state on every write, so it suits some thousands of passages per scope.
 */
public final class CompositeKnowledgeStore implements KnowledgeStore, KnowledgeInspector {

    static final int MAX_PASSAGES = 500;
    static final int MAX_ENTITIES = 2_000;
    static final int MAX_RELATIONS = 5_000;
    static final int MAX_PASSAGE_BYTES = 8 * 1024;
    private static final Pattern LANGUAGE = Pattern.compile("[A-Za-z]{2,3}(-[A-Za-z0-9]{1,8})*");
    /** Entities a retrieval adds through the graph score this fraction of the passage they come from (relations, half of that again; deeper, halved each step). */
    private static final double ENTITY_FRACTION = 0.5;
    private static final double SEED_SCORE = 0.8;

    private final Embedder embedder;
    private final BlobStore blobs;
    private final RetentionClass restrictedClass;
    private final InstantSource clock;
    private final Map<KnowledgeScope, Holder> scopes = new ConcurrentHashMap<>();
    private final AtomicLong expiredSkipped = new AtomicLong();
    private volatile Runnable faultBeforeSwap = () -> { };

    /**
     * @param restrictedClass the retention class restricted content rests under, or {@code null}: then a write of a restricted
     *                        item is refused (there is no default class, SDD 3b.1, 8e)
     */
    public CompositeKnowledgeStore(Embedder embedder, BlobStore blobs, RetentionClass restrictedClass,
            InstantSource clock) {
        this.embedder = embedder;
        this.blobs = blobs;
        this.restrictedClass = restrictedClass;
        this.clock = clock;
    }

    /** Test hook: runs just before a write or an erasure publishes its state; if it throws, nothing changes. */
    public void faultBeforeSwap(Runnable fault) {
        this.faultBeforeSwap = fault == null ? () -> { } : fault;
    }

    /** How many restricted items a retrieval left out because their blob had expired or was gone. */
    public long expiredSkipped() {
        return expiredSkipped.get();
    }

    // --- the state of a scope --------------------------------------------------------------------------------------

    /** What a document gave, so it can be replaced or erased as a whole. */
    private record Doc(String documentId, String contentHash, Long revision, String batchId, Source source,
            Set<String> passageIds, Set<String> entityIds, Set<String> relationKeys, Set<String> links,
            Set<String> derivedFrom) {
    }

    private static final class State {
        final InMemoryVectorIndex index;
        final InMemoryGraph graph;
        final InMemoryChangeLog log;
        final Map<String, Doc> docs;
        final Set<String> erasedDocs;
        final Set<String> erasedEntities;
        final Set<String> erasureRequests;
        /** Pointers (as text) to the blobs each erasure request retired, so {@link #verifyErased} can look for them. */
        final Map<String, List<String>> erasedBlobs = new LinkedHashMap<>();
        String model;
        int dimension;
        /** The embedder a reindexation put in charge of this scope, or {@code null}: then the store's own. */
        Embedder embedder;

        State() {
            this(new InMemoryVectorIndex(), new InMemoryGraph(), new InMemoryChangeLog(), new LinkedHashMap<>(),
                    new HashSet<>(), new HashSet<>(), new HashSet<>(), null, 0);
        }

        private State(InMemoryVectorIndex index, InMemoryGraph graph, InMemoryChangeLog log, Map<String, Doc> docs,
                Set<String> erasedDocs, Set<String> erasedEntities, Set<String> erasureRequests, String model,
                int dimension) {
            this.index = index;
            this.graph = graph;
            this.log = log;
            this.docs = docs;
            this.erasedDocs = erasedDocs;
            this.erasedEntities = erasedEntities;
            this.erasureRequests = erasureRequests;
            this.model = model;
            this.dimension = dimension;
        }

        State copy() {
            State copy = new State(index.copy(), graph.copy(), log.copy(), new LinkedHashMap<>(docs),
                    new HashSet<>(erasedDocs), new HashSet<>(erasedEntities), new HashSet<>(erasureRequests), model,
                    dimension);
            copy.erasedBlobs.putAll(erasedBlobs);
            copy.embedder = embedder;
            return copy;
        }
    }

    private static final class Holder {
        final ReentrantLock lock = new ReentrantLock();
        volatile State state = new State();
    }

    private Holder holder(KnowledgeScope scope) {
        return scopes.computeIfAbsent(scope, s -> new Holder());
    }

    /** The embedder in charge of a state: the one a reindexation chose, else the store's own. */
    private Embedder active(State s) {
        return s.embedder != null ? s.embedder : embedder;
    }

    /** One change in flight: the copy being changed, its log entries and the blobs it created or retires. */
    private final class Tx {
        final KnowledgeScope scope;
        final State s;
        final String batchId;
        final Source source;
        final String schemaVersion;
        final Map<String, Integer> counts = new TreeMap<>();
        final List<URI> created = new ArrayList<>();
        final List<URI> retired = new ArrayList<>();
        /** The node type of the entity the next {@link #log} entry is about (set just before it). */
        String entityType;
        boolean changed;
        /** The embedder this change uses for the vectors it computes. */
        Embedder embedder;

        Tx(KnowledgeScope scope, State s, String batchId, Source source, String schemaVersion) {
            this.scope = scope;
            this.s = s;
            this.batchId = batchId;
            this.source = source;
            this.schemaVersion = schemaVersion;
        }

        void log(Op op, Kind kind, String targetId, String documentId, Sensitivity level, String contentHash,
                String reason) {
            String elementType = switch (kind) {
                case PASSAGE -> "Passage";
                case LINK -> "Link";
                case RELATION -> targetId.substring(0, targetId.indexOf('|'));
                case ENTITY -> entityType;
            };
            s.log.append(new ChangeEntry(s.log.nextSequence(), clock.instant(), batchId, source.instanceId(),
                    source.chainId(), source.definition() == null ? null : source.definition().canonical(),
                    schemaVersion, op, kind, targetId, documentId, level,
                    level == Sensitivity.RESTRICTED ? null : contentHash, reason, elementType));
            entityType = null;
            counts.merge(op + "." + kind, 1, Integer::sum);
            changed = true;
        }
    }

    // --- write -----------------------------------------------------------------------------------------------------

    @Override
    public CompletionStage<WriteReceipt> write(KnowledgeScope scope, KnowledgeBatch batch) {
        try {
            validate(scope, batch);
            return CompletableFuture.completedFuture(doWrite(scope, batch));
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private void validate(KnowledgeScope scope, KnowledgeBatch batch) {
        if (batch.documentId().isBlank() || batch.contentHash().isBlank() || batch.schemaVersion().isBlank()) {
            throw new KnowledgeValidationException("documentId, contentHash and schemaVersion are required");
        }
        if (batch.passages().size() > MAX_PASSAGES || batch.entities().size() > MAX_ENTITIES
                || batch.relations().size() > MAX_RELATIONS) {
            throw new KnowledgeValidationException("The batch is over the limits (" + MAX_PASSAGES + " passages, "
                    + MAX_ENTITIES + " entities, " + MAX_RELATIONS + " relations)");
        }
        Embedder embedder = active(holder(scope).state);
        Set<String> passageIds = new HashSet<>();
        Set<Integer> ordinals = new HashSet<>();
        boolean restricted = false;
        for (Passage p : batch.passages()) {
            if (p.text().getBytes(StandardCharsets.UTF_8).length > MAX_PASSAGE_BYTES) {
                throw new KnowledgeValidationException("A passage is over " + MAX_PASSAGE_BYTES + " bytes (ordinal "
                        + p.ordinal() + ")");
            }
            if (!passageIds.add(p.passageId()) || !ordinals.add(p.ordinal())) {
                throw new KnowledgeValidationException("A passage id or ordinal is repeated in the batch");
            }
            if (!p.passageId().equals(PassageIds.of(scope, batch.documentId(), p.ordinal(), p.text()))) {
                throw new KnowledgeValidationException("passageId of ordinal " + p.ordinal()
                        + " is not PassageIds.of(scope, documentId, ordinal, text)");
            }
            if (!LANGUAGE.matcher(p.language()).matches()
                    || !embedder.languages().contains(p.language().split("-")[0].toLowerCase(java.util.Locale.ROOT))) {
                throw new KnowledgeValidationException("The language of passage " + p.ordinal()
                        + " is not one the embedder supports " + embedder.languages());
            }
            restricted |= p.sensitivity() == Sensitivity.RESTRICTED;
        }
        Set<String> entityIds = new HashSet<>();
        for (Entity e : batch.entities()) {
            if (!entityIds.add(e.entityId())) {
                throw new KnowledgeValidationException("An entity id is repeated in the batch");
            }
            restricted |= e.sensitivity() == Sensitivity.RESTRICTED;
        }
        for (Link link : batch.links()) {
            if (!passageIds.contains(link.passageId())) {
                throw new KnowledgeValidationException("A link names a passage that is not in the batch");
            }
        }
        if (restricted && restrictedClass == null) {
            throw new KnowledgeValidationException("Restricted content needs a retention class: this store has none "
                    + "configured, so it refuses to write a restricted item");
        }
    }

    private WriteReceipt doWrite(KnowledgeScope scope, KnowledgeBatch batch) {
        Holder holder = holder(scope);
        holder.lock.lock();
        Tx tx = null;
        try {
            State current = holder.state;
            Embedder embedder = active(current);
            if (current.embedder == null && current.model != null && (!current.model.equals(embedder.model())
                    || current.dimension != embedder.dimension())) {
                throw new KnowledgeValidationException("The base holds vectors of model " + current.model
                        + "; this embedder is " + embedder.model() + " (reindex to change it)");
            }
            if (current.erasedDocs.contains(batch.documentId())) {
                return new WriteReceipt(WriteReceipt.Status.REJECTED_ERASED, Map.of(), current.log.lastSequence(), 0);
            }
            Doc prior = current.docs.get(batch.documentId());
            if (prior != null && batch.revision() != null && prior.revision() != null) {
                if (batch.revision() < prior.revision()) {
                    return new WriteReceipt(WriteReceipt.Status.STALE, Map.of(), current.log.lastSequence(), 0);
                }
                if (batch.revision().equals(prior.revision()) && !batch.contentHash().equals(prior.contentHash())) {
                    throw new KnowledgeValidationException("Revision " + batch.revision() + " of the document was "
                            + "applied with another contentHash");
                }
            }
            State next = current.copy();
            next.model = embedder.model();
            next.dimension = embedder.dimension();
            tx = new Tx(scope, next, batch.batchId(), batch.source(), batch.schemaVersion());
            tx.embedder = embedder;
            int dropped = apply(tx, batch, prior);
            if (!tx.changed) {
                boolean sameMeta = prior != null && prior.contentHash().equals(batch.contentHash())
                        && java.util.Objects.equals(prior.revision(), batch.revision())
                        && prior.derivedFrom().equals(next.docs.get(batch.documentId()).derivedFrom());
                if (!sameMeta || current.model == null) {
                    holder.state = next;   // only the document's own bookkeeping moved: nothing to log
                }
                return new WriteReceipt(WriteReceipt.Status.UNCHANGED, Map.of(), current.log.lastSequence(), dropped);
            }
            deleteBlobs(tx.retired);
            faultBeforeSwap.run();
            holder.state = next;
            return new WriteReceipt(WriteReceipt.Status.APPLIED, tx.counts, next.log.lastSequence(), dropped);
        } catch (RuntimeException e) {
            if (tx != null) {
                tx.created.forEach(this::deleteQuietly);   // blobs of a write that did not happen
            }
            throw e;
        } finally {
            holder.lock.unlock();
        }
    }

    /** Makes the copy hold exactly {@code batch} for its document; returns how many references to erased entities it left out. */
    private int apply(Tx tx, KnowledgeBatch batch, Doc prior) {
        State s = tx.s;
        String doc = batch.documentId();
        int dropped = 0;

        List<Entity> entities = new ArrayList<>();
        for (Entity e : batch.entities()) {
            if (s.erasedEntities.contains(e.entityId())) {
                dropped++;
            } else {
                entities.add(e);
            }
        }
        List<Relation> relations = new ArrayList<>();
        for (Relation r : batch.relations()) {
            if (s.erasedEntities.contains(r.fromId()) || s.erasedEntities.contains(r.toId())) {
                dropped++;
            } else {
                relations.add(r);
            }
        }
        Map<String, Passage> passages = new LinkedHashMap<>();
        batch.passages().forEach(p -> passages.put(p.passageId(), p));
        Set<String> desiredLinks = new LinkedHashSet<>();
        for (Link link : batch.links()) {
            if (s.erasedEntities.contains(link.entityId())) {
                dropped++;
            } else {
                desiredLinks.add(link.passageId() + ">" + link.entityId());
            }
        }
        Set<String> desiredEntities = new LinkedHashSet<>();
        entities.forEach(e -> desiredEntities.add(e.entityId()));
        Set<String> desiredRelations = new LinkedHashSet<>();
        relations.forEach(r -> desiredRelations.add(r.key()));

        Set<String> priorPassages = prior == null ? Set.of() : prior.passageIds();
        Set<String> priorEntities = prior == null ? Set.of() : prior.entityIds();
        Set<String> priorRelations = prior == null ? Set.of() : prior.relationKeys();
        Set<String> priorLinks = prior == null ? Set.of() : prior.links();

        // 1. what the document no longer holds
        for (String link : priorLinks) {
            if (!desiredLinks.contains(link)) {
                removeLink(tx, link, Op.REMOVE, null, doc);
            }
        }
        for (String key : priorRelations) {
            if (!desiredRelations.contains(key)) {
                dropEdgeEvidence(tx, key, doc, Op.REMOVE, null);
            }
        }
        for (String id : priorEntities) {
            if (!desiredEntities.contains(id)) {
                dropNodeEvidence(tx, id, doc, Op.REMOVE, null);
            }
        }
        for (String id : priorPassages) {
            if (!passages.containsKey(id)) {
                removePassage(tx, id, Op.REMOVE, null);
            }
        }

        // 2. entities
        for (Entity e : entities) {
            putEntity(tx, e, doc);
        }
        // 3. passages
        for (Passage p : passages.values()) {
            putPassage(tx, p, doc);
        }
        // 4. relations
        for (Relation r : relations) {
            putRelation(tx, r, doc);
        }
        // 5. links
        for (String link : desiredLinks) {
            if (!priorLinks.contains(link) || !hasLink(s, link)) {
                addLink(tx, link, doc);
            }
        }

        // the blobs the document came from accumulate: a republication has a new batch, and the old one still holds the text
        Set<String> derived = new LinkedHashSet<>(prior == null ? Set.of() : prior.derivedFrom());
        batch.derivedFrom().forEach(uri -> derived.add(uri.toString()));
        s.docs.put(doc, new Doc(doc, batch.contentHash(), batch.revision(), batch.batchId(), batch.source(),
                new LinkedHashSet<>(passages.keySet()), desiredEntities, desiredRelations, desiredLinks, derived));
        reconcileDocs(s);
        return dropped;
    }

    // --- passages --------------------------------------------------------------------------------------------------

    private void putPassage(Tx tx, Passage p, String doc) {
        State s = tx.s;
        InMemoryVectorIndex.Indexed existing = s.index.get(p.passageId());
        boolean restricted = p.sensitivity() == Sensitivity.RESTRICTED;
        URI ref = null;
        if (restricted) {
            ref = putBlob(tx, "passage", p.passageId(), p.text());
        }
        if (existing != null && existing.level() == p.sensitivity()
                && java.util.Objects.equals(existing.contentRef(), ref)
                && java.util.Objects.equals(existing.text(), restricted ? null : p.text())) {
            return;   // the same passage: nothing to write, nothing to log
        }
        if (ref != null) {
            tx.created.add(ref);   // new or changed content: undone if this write does not happen
        }
        float[] vector = existing != null && java.util.Objects.equals(existing.text(), p.text()) && existing.vector() != null
                ? existing.vector() : tx.embedder.embed(p.text());
        if (existing != null && existing.contentRef() != null && !existing.contentRef().equals(ref)) {
            tx.retired.add(existing.contentRef());
        }
        s.index.put(new InMemoryVectorIndex.Indexed(p.passageId(), doc, p.ordinal(), p.language(), p.sensitivity(),
                vector, restricted ? null : p.text(), ref));
        tx.log(existing == null ? Op.ADD : Op.UPDATE, Kind.PASSAGE, p.passageId(), doc, p.sensitivity(),
                PassageIds.sha256Hex(p.text()), null);
    }

    private void removePassage(Tx tx, String passageId, Op op, String reason) {
        State s = tx.s;
        InMemoryVectorIndex.Indexed removed = s.index.remove(passageId);
        if (removed == null) {
            return;
        }
        for (String entityId : List.copyOf(s.graph.entitiesOf(passageId))) {
            removeLink(tx, passageId + ">" + entityId, op, reason, removed.documentId());
        }
        if (removed.contentRef() != null) {
            tx.retired.add(removed.contentRef());
        }
        tx.log(op, Kind.PASSAGE, passageId, removed.documentId(), removed.level(), null, reason);
    }

    // --- entities and relations ------------------------------------------------------------------------------------

    private void putEntity(Tx tx, Entity e, String doc) {
        State s = tx.s;
        InMemoryGraph.Node existing = s.graph.node(e.entityId());
        if (existing != null && !existing.type().equals(e.type())) {
            throw new KnowledgeValidationException("An entity id is used with two types");
        }
        // a level only rises by a write: a later document cannot lower what an earlier one marked
        Sensitivity level = existing == null ? e.sensitivity() : Sensitivity.max(existing.level(), e.sensitivity());
        boolean restricted = level == Sensitivity.RESTRICTED;
        URI ref = restricted ? putBlob(tx, "entity", e.entityId(), ContentCodec.encode(e.label(), e.properties())) : null;
        Set<String> sources = new LinkedHashSet<>(existing == null ? Set.of() : existing.sources());
        boolean newEvidence = sources.add(doc);
        String label = restricted ? null : e.label();
        Map<String, String> properties = restricted ? Map.of() : e.properties();
        boolean sameContent = existing != null && existing.level() == level
                && java.util.Objects.equals(existing.contentRef(), ref)
                && java.util.Objects.equals(existing.label(), label) && existing.properties().equals(properties);
        if (sameContent && !newEvidence) {
            return;
        }
        if (ref != null) {
            tx.created.add(ref);
        }
        if (existing != null && existing.contentRef() != null && !existing.contentRef().equals(ref)) {
            tx.retired.add(existing.contentRef());
        }
        s.graph.putNode(new InMemoryGraph.Node(e.entityId(), e.type(), label, properties, ref, level, sources));
        tx.entityType = e.type();
        tx.log(existing == null ? Op.ADD : Op.UPDATE, Kind.ENTITY, e.entityId(), doc, level,
                PassageIds.sha256Hex(e.label() + '\0' + new TreeMap<>(e.properties())),
                existing != null && sameContent ? "evidence" : null);
    }

    private void putRelation(Tx tx, Relation r, String doc) {
        State s = tx.s;
        InMemoryGraph.Node from = s.graph.node(r.fromId());
        InMemoryGraph.Node to = s.graph.node(r.toId());
        if (from == null || to == null) {
            throw new KnowledgeValidationException("A relation names an entity that is neither in the batch nor in the base");
        }
        Sensitivity floor = Sensitivity.max(from.level(), to.level());
        if (r.sensitivity().compareTo(floor) < 0) {
            throw new KnowledgeValidationException("A relation is below the level of its ends");
        }
        InMemoryGraph.Edge existing = s.graph.edge(r.key());
        Sensitivity level = existing == null ? r.sensitivity() : Sensitivity.max(existing.level(), r.sensitivity());
        Set<String> sources = new LinkedHashSet<>(existing == null ? Set.of() : existing.sources());
        boolean newEvidence = sources.add(doc);
        if (existing != null && existing.level() == level && !newEvidence) {
            return;
        }
        s.graph.putEdge(new InMemoryGraph.Edge(r.key(), r.type(), r.fromId(), r.toId(), level, sources));
        tx.log(existing == null ? Op.ADD : Op.UPDATE, Kind.RELATION, r.key(), doc, level,
                PassageIds.sha256Hex(r.key()), existing != null && existing.level() == level ? "evidence" : null);
    }

    private void dropEdgeEvidence(Tx tx, String key, String doc, Op op, String reason) {
        State s = tx.s;
        InMemoryGraph.Edge edge = s.graph.edge(key);
        if (edge == null) {
            return;
        }
        Set<String> sources = new LinkedHashSet<>(edge.sources());
        if (!sources.remove(doc)) {
            return;
        }
        if (sources.isEmpty()) {
            s.graph.removeEdge(key);
            tx.log(op, Kind.RELATION, key, doc, edge.level(), null, reason);
        } else {
            s.graph.putEdge(new InMemoryGraph.Edge(key, edge.type(), edge.from(), edge.to(), edge.level(), sources));
            tx.log(Op.UPDATE, Kind.RELATION, key, doc, edge.level(), null, "evidence");
        }
    }

    private void dropNodeEvidence(Tx tx, String entityId, String doc, Op op, String reason) {
        State s = tx.s;
        InMemoryGraph.Node node = s.graph.node(entityId);
        if (node == null) {
            return;
        }
        Set<String> sources = new LinkedHashSet<>(node.sources());
        if (!sources.remove(doc)) {
            return;
        }
        if (sources.isEmpty()) {
            removeNode(tx, entityId, op, reason, doc);
        } else {
            s.graph.putNode(new InMemoryGraph.Node(entityId, node.type(), node.label(), node.properties(),
                    node.contentRef(), node.level(), sources));
            tx.entityType = node.type();
            tx.log(Op.UPDATE, Kind.ENTITY, entityId, doc, node.level(), null, "evidence");
        }
    }

    /** Removes a node with its edges and the links that name it. */
    private void removeNode(Tx tx, String entityId, Op op, String reason, String doc) {
        State s = tx.s;
        InMemoryGraph.Node node = s.graph.removeNode(entityId);
        if (node == null) {
            return;
        }
        for (InMemoryGraph.Edge edge : s.graph.edgesOf(entityId)) {
            s.graph.removeEdge(edge.key());
            tx.log(op, Kind.RELATION, edge.key(), doc, edge.level(), null, reason);
        }
        for (String passage : s.graph.passagesOf(entityId)) {
            removeLink(tx, passage + ">" + entityId, op, reason, doc);
        }
        if (node.contentRef() != null) {
            tx.retired.add(node.contentRef());
        }
        tx.entityType = node.type();
        tx.log(op, Kind.ENTITY, entityId, doc, node.level(), null, reason);
    }

    // --- links -----------------------------------------------------------------------------------------------------

    private static boolean hasLink(State s, String link) {
        int cut = link.indexOf('>');
        return s.graph.entitiesOf(link.substring(0, cut)).contains(link.substring(cut + 1));
    }

    private void addLink(Tx tx, String link, String doc) {
        State s = tx.s;
        int cut = link.indexOf('>');
        String passageId = link.substring(0, cut);
        String entityId = link.substring(cut + 1);
        InMemoryVectorIndex.Indexed passage = s.index.get(passageId);
        InMemoryGraph.Node node = s.graph.node(entityId);
        if (passage == null || node == null) {
            throw new KnowledgeValidationException("A link names an entity that is neither in the batch nor in the base");
        }
        if (s.graph.addLink(passageId, entityId)) {
            tx.log(Op.ADD, Kind.LINK, link, doc, Sensitivity.max(passage.level(), node.level()), null, null);
        }
    }

    private void removeLink(Tx tx, String link, Op op, String reason, String doc) {
        int cut = link.indexOf('>');
        String passageId = link.substring(0, cut);
        String entityId = link.substring(cut + 1);
        InMemoryVectorIndex.Indexed passage = tx.s.index.get(passageId);
        InMemoryGraph.Node node = tx.s.graph.node(entityId);
        Sensitivity level = Sensitivity.max(passage == null ? null : passage.level(), node == null ? null : node.level());
        if (tx.s.graph.removeLink(passageId, entityId)) {
            tx.log(op, Kind.LINK, link, doc, level == null ? Sensitivity.RESTRICTED : level, null, reason);
        }
    }

    /** Every document's bookkeeping is cut down to what still exists (a node removed for another document's sake). */
    private static void reconcileDocs(State s) {
        Set<String> passages = new HashSet<>();
        s.index.all().forEach(p -> passages.add(p.id()));
        Set<String> links = new HashSet<>();
        s.graph.allLinks().forEach(l -> links.add(l[0] + ">" + l[1]));
        for (Map.Entry<String, Doc> entry : new ArrayList<>(s.docs.entrySet())) {
            Doc d = entry.getValue();
            Set<String> entities = new LinkedHashSet<>();
            d.entityIds().forEach(id -> {
                InMemoryGraph.Node n = s.graph.node(id);
                if (n != null && n.sources().contains(d.documentId())) {
                    entities.add(id);
                }
            });
            Set<String> relations = new LinkedHashSet<>();
            d.relationKeys().forEach(key -> {
                InMemoryGraph.Edge e = s.graph.edge(key);
                if (e != null && e.sources().contains(d.documentId())) {
                    relations.add(key);
                }
            });
            Set<String> keptPassages = new LinkedHashSet<>(d.passageIds());
            keptPassages.retainAll(passages);
            Set<String> keptLinks = new LinkedHashSet<>(d.links());
            keptLinks.retainAll(links);
            entry.setValue(new Doc(d.documentId(), d.contentHash(), d.revision(), d.batchId(), d.source(), keptPassages,
                    entities, relations, keptLinks, d.derivedFrom()));
        }
    }

    // --- blobs -----------------------------------------------------------------------------------------------------

    private URI putBlob(Tx tx, String kind, String id, String content) {
        String key = "knowledge-" + PassageIds.sha256Hex(tx.scope.key() + '\0' + kind + '\0' + id + '\0'
                + PassageIds.sha256Hex(content));
        URI uri = blobs.put(content.getBytes(StandardCharsets.UTF_8), key, restrictedClass).toCompletableFuture().join();
        return uri;
    }

    private void deleteBlobs(List<URI> uris) {
        for (URI uri : uris) {
            try {
                blobs.delete(uri).toCompletableFuture().join();
            } catch (CompletionException e) {
                if (!(e.getCause() instanceof BlobNotFoundException)) {
                    throw e;   // an erasure that could not delete a blob has not happened
                }
            }
        }
    }

    private void deleteQuietly(URI uri) {
        try {
            blobs.delete(uri).toCompletableFuture().join();
        } catch (RuntimeException e) {
            // an orphan of a write that did not happen: harmless, and its key is derived from its content
        }
    }

    /** The content of a restricted blob, or {@code null} if it expired or is gone. */
    private String readBlob(URI uri) {
        try {
            return new String(blobs.get(uri).toCompletableFuture().join(), StandardCharsets.UTF_8);
        } catch (RuntimeException e) {
            expiredSkipped.incrementAndGet();
            return null;
        }
    }

    // --- retrieve --------------------------------------------------------------------------------------------------

    @Override
    public CompletionStage<List<ContextItem>> retrieve(KnowledgeScope scope, RetrievalQuery query) {
        try {
            return CompletableFuture.completedFuture(doRetrieve(scope, query));
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private List<ContextItem> doRetrieve(KnowledgeScope scope, RetrievalQuery q) {
        State s = holder(scope).state;
        Embedder embedder = active(s);
        if (s.embedder == null && s.model != null && (!s.model.equals(embedder.model()) || s.dimension != embedder.dimension())) {
            throw new KnowledgeValidationException("The base holds vectors of model " + s.model);
        }
        float[] question = embedder.embed(q.text());
        String language = q.language() == null ? null : q.language().split("-")[0].toLowerCase(java.util.Locale.ROOT);
        List<InMemoryVectorIndex.Hit> hits = s.index.nearest(question, p -> p.level().compareTo(q.ceiling()) <= 0
                && (language == null || p.language().split("-")[0].toLowerCase(java.util.Locale.ROOT).equals(language))
                && (q.entityIds().isEmpty() || mentionsAny(s, p.id(), q.entityIds())), q.topK(), q.minScore());

        List<ContextItem> items = new ArrayList<>();
        Map<String, Double> entityScores = new LinkedHashMap<>();
        for (InMemoryVectorIndex.Hit hit : hits) {
            InMemoryVectorIndex.Indexed p = hit.passage();
            String text = p.text() != null ? p.text() : readBlob(p.contentRef());
            if (text == null) {
                continue;   // expired: left out, never an error and never garbage
            }
            List<ContextItem.EntityRef> refs = new ArrayList<>();
            for (String entityId : s.graph.entitiesOf(p.id())) {
                InMemoryGraph.Node node = s.graph.node(entityId);
                if (node != null && node.level().compareTo(q.ceiling()) <= 0) {
                    String label = labelOf(node);
                    if (label != null) {
                        refs.add(new ContextItem.EntityRef(entityId, node.type(), label));
                        entityScores.merge(entityId, hit.score(), Math::max);
                    }
                }
            }
            Doc doc = s.docs.get(p.documentId());
            items.add(new ContextItem(text, new ContextItem.Origin(ContextItem.Kind.PASSAGE, p.id(), p.documentId(),
                    scope, doc == null ? null : doc.source().instanceId(), doc == null ? null : doc.source().chainId()),
                    hit.score(), refs, p.level()));
        }
        if (q.hops() >= 1) {
            items.addAll(expand(scope, s, q, entityScores));
        }
        return items;
    }

    /**
     * The graph's share of a retrieval (SDD 3b.4, 4.1), {@code hops} counted from the passage. 1: the entities the passages mention
     * and the edges among them. 2 and 3: each step adds the neighbours of the last step's entities that the ceiling lets through,
     * and the edges that now have both ends. An item scores a fixed fraction of the best passage it comes from (entities
     * {@code 0.5^d}, edges {@code 0.5^(d+1)}); the entities a query names ({@code entityIds}) start from {@link #SEED_SCORE} when
     * no passage found them. Each item says which documents still hold it.
     */
    private List<ContextItem> expand(KnowledgeScope scope, State s, RetrievalQuery q, Map<String, Double> entityScores) {
        Map<String, Double> base = new LinkedHashMap<>(entityScores);
        if (q.hops() >= 2) {
            for (String id : new java.util.TreeSet<>(q.entityIds())) {
                if (!base.containsKey(id) && visibleLabel(s, id, q) != null) {
                    base.put(id, SEED_SCORE);
                }
            }
        }
        if (base.isEmpty()) {
            return List.of();
        }
        List<ContextItem> added = new ArrayList<>();
        Map<String, String> labels = new LinkedHashMap<>();
        for (Map.Entry<String, Double> entry : base.entrySet()) {
            String label = visibleLabel(s, entry.getKey(), q);
            if (label != null) {
                labels.put(entry.getKey(), label);
                added.add(entityItem(scope, s.graph.node(entry.getKey()), label, Math.pow(ENTITY_FRACTION, 1) * entry.getValue()));
            }
        }
        Set<String> edgesDone = new HashSet<>();
        addEdges(scope, s, q, labels, base, edgesDone, 1, added);
        Set<String> frontier = new LinkedHashSet<>(labels.keySet());
        for (int d = 2; d <= q.hops(); d++) {
            Set<String> reached = new LinkedHashSet<>();
            for (String from : frontier) {
                for (InMemoryGraph.Edge edge : s.graph.edgesOf(from)) {
                    String other = edge.from().equals(from) ? edge.to() : edge.from();
                    if (labels.containsKey(other) || edge.level().compareTo(q.ceiling()) > 0) {
                        continue;
                    }
                    String label = visibleLabel(s, other, q);
                    if (label != null) {
                        labels.put(other, label);
                        base.merge(other, base.get(from), Math::max);
                        reached.add(other);
                    }
                }
            }
            for (String id : reached) {
                added.add(entityItem(scope, s.graph.node(id), labels.get(id), Math.pow(ENTITY_FRACTION, d) * base.get(id)));
            }
            addEdges(scope, s, q, labels, base, edgesDone, d, added);
            frontier = reached;
        }
        added.sort(Comparator.comparingDouble(ContextItem::score).reversed()
                .thenComparing(item -> item.origin().id()));
        return added.size() > q.topK() ? new ArrayList<>(added.subList(0, q.topK())) : added;
    }

    /** The label of {@code id} if it is a node the ceiling lets through and whose label is not gone, else {@code null}. */
    private String visibleLabel(State s, String id, RetrievalQuery q) {
        InMemoryGraph.Node node = s.graph.node(id);
        return node == null || node.level().compareTo(q.ceiling()) > 0 ? null : labelOf(node);
    }

    private static ContextItem entityItem(KnowledgeScope scope, InMemoryGraph.Node node, String label, double score) {
        return new ContextItem(node.type() + ": " + label, new ContextItem.Origin(ContextItem.Kind.ENTITY, node.id(), null,
                scope, null, null, new java.util.TreeSet<>(node.sources()).stream().toList()), score, List.of(), node.level());
    }

    /** The edges not yet added whose two ends are both shown and which the ceiling lets through, at depth {@code d}. */
    private static void addEdges(KnowledgeScope scope, State s, RetrievalQuery q, Map<String, String> labels,
            Map<String, Double> base, Set<String> done, int d, List<ContextItem> out) {
        for (InMemoryGraph.Edge edge : s.graph.edges()) {
            if (edge.level().compareTo(q.ceiling()) <= 0 && labels.containsKey(edge.from()) && labels.containsKey(edge.to())
                    && done.add(edge.key())) {
                double parent = Math.max(base.get(edge.from()), base.get(edge.to()));
                out.add(new ContextItem(labels.get(edge.from()) + " -[" + edge.type() + "]-> " + labels.get(edge.to()),
                        new ContextItem.Origin(ContextItem.Kind.RELATION, edge.key(), null, scope, null, null,
                                new java.util.TreeSet<>(edge.sources()).stream().toList()),
                        Math.pow(ENTITY_FRACTION, d + 1) * parent, List.of(), edge.level()));
            }
        }
    }

    private static boolean mentionsAny(State s, String passageId, Set<String> entityIds) {
        for (String id : s.graph.entitiesOf(passageId)) {
            if (entityIds.contains(id)) {
                return true;
            }
        }
        return false;
    }

    /** The label of a node, read from its blob when restricted; {@code null} if that blob is gone. */
    private String labelOf(InMemoryGraph.Node node) {
        if (node.contentRef() == null) {
            return node.label();
        }
        String content = readBlob(node.contentRef());
        return content == null ? null : ContentCodec.decode(content).label();
    }

    // --- entities --------------------------------------------------------------------------------------------------

    @Override
    public CompletionStage<List<EntityMatch>> entities(KnowledgeScope scope, EntityQuery query) {
        try {
            State s = holder(scope).state;
            List<EntityMatch> matches = new ArrayList<>();
            for (InMemoryGraph.Node node : s.graph.nodes()) {
                if (node.level().compareTo(query.ceiling()) > 0 || query.type() != null && !query.type().equals(node.type())) {
                    continue;
                }
                String label = node.label();
                Map<String, String> properties = node.properties();
                if (node.contentRef() != null) {
                    String content = readBlob(node.contentRef());
                    if (content == null) {
                        continue;
                    }
                    ContentCodec.Content decoded = ContentCodec.decode(content);
                    label = decoded.label();
                    properties = decoded.properties();
                }
                List<String> aliases = properties.containsKey("aliases") ? List.of(properties.get("aliases").split("\\|")) : List.of();
                boolean matched = EntityIds.normalize(label).equals(query.normalizedName());
                for (String alias : aliases) {
                    matched |= EntityIds.normalize(alias).equals(query.normalizedName());
                }
                if (matched) {
                    matches.add(new EntityMatch(node.id(), node.type(), label, aliases, node.level()));
                }
            }
            matches.sort(Comparator.comparing(EntityMatch::entityId));
            return CompletableFuture.completedFuture(matches.size() > query.limit()
                    ? new ArrayList<>(matches.subList(0, query.limit())) : matches);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    // --- inspection (SDD 3b.6) -------------------------------------------------------------------------------------

    @Override
    public CompletionStage<GraphView> view(KnowledgeScope scope, ViewQuery query) {
        try {
            State s = holder(scope).state;
            // the sequence that added what is alive: the last ADD of an id that was not removed since
            Map<String, Long> added = new java.util.HashMap<>();
            for (ChangeEntry e : s.log.all()) {
                if (e.kind() != ChangeEntry.Kind.ENTITY && e.kind() != ChangeEntry.Kind.RELATION) {
                    continue;
                }
                switch (e.op()) {
                    case ADD -> added.putIfAbsent(e.targetId(), e.sequence());
                    case REMOVE, EXPIRE, ERASE -> added.remove(e.targetId());
                    default -> { }
                }
            }
            List<InMemoryGraph.Node> visible = new ArrayList<>();
            Set<String> visibleIds = new HashSet<>();
            for (InMemoryGraph.Node node : s.graph.nodes()) {
                if (node.level().compareTo(query.ceiling()) <= 0) {
                    visibleIds.add(node.id());
                    if ((query.type() == null || query.type().equals(node.type()))
                            && (query.afterNodeId() == null || node.id().compareTo(query.afterNodeId()) > 0)) {
                        visible.add(node);
                    }
                }
            }
            visible.sort(Comparator.comparing(InMemoryGraph.Node::id));
            boolean more = visible.size() > query.limit();
            List<InMemoryGraph.Node> page = more ? visible.subList(0, query.limit()) : visible;
            Set<String> pageIds = new HashSet<>();
            List<NodeView> nodes = new ArrayList<>();
            for (InMemoryGraph.Node node : page) {
                pageIds.add(node.id());
                // a restricted label rests in a blob this method never opens
                String label = node.level() == Sensitivity.RESTRICTED || node.contentRef() != null ? null : node.label();
                nodes.add(new NodeView(node.id(), node.type(), node.level(), label, added.getOrDefault(node.id(), 0L),
                        node.sources().size()));
            }
            List<EdgeView> edges = new ArrayList<>();
            for (InMemoryGraph.Edge edge : s.graph.edges()) {
                if (pageIds.contains(edge.from()) && visibleIds.contains(edge.to())
                        && edge.level().compareTo(query.ceiling()) <= 0) {
                    edges.add(new EdgeView(edge.key(), edge.type(), edge.from(), edge.to(), edge.level(),
                            added.getOrDefault(edge.key(), 0L)));
                }
            }
            edges.sort(Comparator.comparing(EdgeView::key));
            return CompletableFuture.completedFuture(new GraphView(nodes, edges,
                    more ? page.get(page.size() - 1).id() : null, s.log.lastSequence()));
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    // --- changes ---------------------------------------------------------------------------------------------------

    @Override
    public CompletionStage<List<ChangeEntry>> changes(KnowledgeScope scope, long afterSequence, int limit) {
        return CompletableFuture.completedFuture(holder(scope).state.log.after(afterSequence, limit));
    }

    // --- erasure ---------------------------------------------------------------------------------------------------

    @Override
    public CompletionStage<DeleteReceipt> delete(KnowledgeScope scope, DeleteRequest request) {
        try {
            return CompletableFuture.completedFuture(doDelete(scope, request));
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private DeleteReceipt doDelete(KnowledgeScope scope, DeleteRequest request) {
        Holder holder = holder(scope);
        holder.lock.lock();
        try {
            State current = holder.state;
            if (current.erasureRequests.contains(request.requestId())) {
                return new DeleteReceipt(request.requestId(), Map.of(), current.log.lastSequence());   // a repeat erases nothing more
            }
            State next = current.copy();
            Tx tx = new Tx(scope, next, request.requestId(), Source.NONE, null);
            switch (request) {
                case DeleteRequest.ByDocument d -> eraseDocument(tx, d);
                case DeleteRequest.ByPerson p -> erasePerson(tx, p);
            }
            next.erasureRequests.add(request.requestId());
            next.erasedBlobs.put(request.requestId(), tx.retired.stream().map(URI::toString).toList());
            reconcileDocs(next);
            deleteBlobs(tx.retired);   // before the swap: an erasure that left a blob behind has not happened
            faultBeforeSwap.run();
            holder.state = next;
            return new DeleteReceipt(request.requestId(), tx.counts, next.log.lastSequence());
        } finally {
            holder.lock.unlock();
        }
    }

    private void eraseDocument(Tx tx, DeleteRequest.ByDocument request) {
        State s = tx.s;
        String documentId = request.documentId();
        Doc doc = s.docs.get(documentId);
        if (doc != null) {
            for (String link : doc.links()) {
                removeLink(tx, link, Op.ERASE, request.reason(), documentId);
            }
            for (String key : doc.relationKeys()) {
                dropEdgeEvidence(tx, key, documentId, Op.ERASE, request.reason());
            }
            for (String id : doc.entityIds()) {
                dropNodeEvidence(tx, id, documentId, Op.ERASE, request.reason());
            }
            for (String id : doc.passageIds()) {
                removePassage(tx, id, Op.ERASE, request.reason());
            }
            doc.derivedFrom().forEach(uri -> tx.retired.add(URI.create(uri)));   // the batch and the text it came from
            s.docs.remove(documentId);
        }
        s.erasedDocs.add(documentId);
    }

    private void erasePerson(Tx tx, DeleteRequest.ByPerson request) {
        State s = tx.s;
        String entityId = request.entityId();
        List<String> mentioning = s.graph.passagesOf(entityId);
        // the documents that asserted the person: the blobs they came from hold the person's pseudonym in the text
        InMemoryGraph.Node node = s.graph.node(entityId);
        Set<String> touched = new LinkedHashSet<>(node == null ? Set.of() : node.sources());
        removeNode(tx, entityId, Op.ERASE, request.reason(), null);
        if (request.passages() == DeleteRequest.PassagePolicy.LINKED) {
            for (String passageId : mentioning) {
                InMemoryVectorIndex.Indexed passage = s.index.get(passageId);
                if (passage != null) {
                    touched.add(passage.documentId());
                }
                removePassage(tx, passageId, Op.ERASE, request.reason());
            }
        }
        for (String documentId : touched) {
            Doc doc = s.docs.get(documentId);
            if (doc != null) {
                doc.derivedFrom().forEach(uri -> tx.retired.add(URI.create(uri)));
                s.docs.put(documentId, new Doc(doc.documentId(), doc.contentHash(), doc.revision(), doc.batchId(),
                        doc.source(), doc.passageIds(), doc.entityIds(), doc.relationKeys(), doc.links(), Set.of()));
            }
        }
        s.erasedEntities.add(entityId);
    }

    // --- describe, reindex and verification (SDD 3b.5) -------------------------------------------------------------

    @Override
    public CompletionStage<BaseInfo> describe(KnowledgeScope scope) {
        State s = holder(scope).state;
        Set<String> versions = new java.util.TreeSet<>();
        s.log.all().forEach(e -> {
            if (e.schemaVersion() != null) {
                versions.add(e.schemaVersion());
            }
        });
        return CompletableFuture.completedFuture(new BaseInfo(s.model, s.dimension, s.docs.size(), s.index.size(),
                s.graph.nodes().size(), s.graph.edges().size(), versions));
    }

    @Override
    public CompletionStage<ReindexReceipt> reindex(KnowledgeScope scope, ReindexRequest request) {
        try {
            return CompletableFuture.completedFuture(doReindex(scope, request));
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private ReindexReceipt doReindex(KnowledgeScope scope, ReindexRequest request) {
        Holder holder = holder(scope);
        holder.lock.lock();
        try {
            State current = holder.state;
            Embedder target = request.target();
            if (current.model != null && current.model.equals(target.model()) && current.dimension == target.dimension()) {
                return new ReindexReceipt(ReindexReceipt.Status.UNCHANGED, current.model, current.model, 0, 0,
                        current.log.lastSequence());
            }
            for (InMemoryVectorIndex.Indexed p : current.index.all()) {
                if (!target.languages().contains(p.language().split("-")[0].toLowerCase(java.util.Locale.ROOT))) {
                    throw new KnowledgeValidationException("The target model does not embed language " + p.language()
                            + ", which the base holds " + target.languages());
                }
            }
            State next = current.copy();
            Tx tx = new Tx(scope, next, request.requestId(), Source.NONE, null);
            tx.embedder = target;
            int done = 0;
            int skipped = 0;
            for (InMemoryVectorIndex.Indexed p : current.index.all()) {
                String text = p.text() != null ? p.text() : readBlob(p.contentRef());
                if (text == null) {
                    removePassage(tx, p.id(), Op.EXPIRE, "retention");   // no text to embed: it left the index, as on a read
                    skipped++;
                    continue;
                }
                next.index.put(new InMemoryVectorIndex.Indexed(p.id(), p.documentId(), p.ordinal(), p.language(), p.level(),
                        target.embed(text), p.text(), p.contentRef()));
                done++;
            }
            next.model = target.model();
            next.dimension = target.dimension();
            next.embedder = target;
            tx.log(Op.REINDEX, Kind.PASSAGE, current.model + "->" + target.model(), null, Sensitivity.INTERNAL, null,
                    request.reason());
            reconcileDocs(next);
            deleteBlobs(tx.retired);
            faultBeforeSwap.run();
            holder.state = next;
            return new ReindexReceipt(ReindexReceipt.Status.APPLIED, current.model, target.model(), done, skipped,
                    next.log.lastSequence());
        } finally {
            holder.lock.unlock();
        }
    }

    @Override
    public CompletionStage<ErasureCheck> verifyErased(KnowledgeScope scope, DeleteRequest request) {
        State s = holder(scope).state;
        Map<String, Integer> remaining = new TreeMap<>();
        boolean tombstone;
        switch (request) {
            case DeleteRequest.ByDocument d -> {
                tombstone = s.erasedDocs.contains(d.documentId());
                count(remaining, "DOCUMENT", s.docs.containsKey(d.documentId()) ? 1 : 0);
                count(remaining, "PASSAGE", (int) s.index.all().stream().filter(p -> p.documentId().equals(d.documentId())).count());
                count(remaining, "ENTITY", (int) s.graph.nodes().stream().filter(n -> n.sources().contains(d.documentId())).count());
                count(remaining, "RELATION", (int) s.graph.edges().stream().filter(e -> e.sources().contains(d.documentId())).count());
                count(remaining, "LINK", (int) s.graph.allLinks().stream().filter(l -> {
                    InMemoryVectorIndex.Indexed p = s.index.get(l[0]);
                    return p != null && p.documentId().equals(d.documentId());
                }).count());
            }
            case DeleteRequest.ByPerson p -> {
                tombstone = s.erasedEntities.contains(p.entityId());
                count(remaining, "ENTITY", s.graph.node(p.entityId()) != null ? 1 : 0);
                count(remaining, "RELATION", s.graph.edgesOf(p.entityId()).size());
                count(remaining, "LINK", s.graph.passagesOf(p.entityId()).size());
            }
        }
        int blobsLeft = 0;
        for (String uri : s.erasedBlobs.getOrDefault(request.requestId(), List.of())) {
            if (exists(URI.create(uri))) {
                blobsLeft++;
            }
        }
        count(remaining, "BLOB", blobsLeft);
        return CompletableFuture.completedFuture(new ErasureCheck(request.requestId(), remaining, tombstone));
    }

    private static void count(Map<String, Integer> into, String kind, int n) {
        if (n > 0) {
            into.put(kind, n);
        }
    }

    // --- retention -------------------------------------------------------------------------------------------------

    /**
     * Removes the restricted passages and entities whose blob has expired or is gone, logging an {@code EXPIRE} entry for each
     * (and for the edges and links that went with an entity). Retrieval already leaves them out; this keeps the index honest.
     *
     * @return how many things expired
     */
    public int reapExpired(KnowledgeScope scope) {
        Holder holder = holder(scope);
        holder.lock.lock();
        try {
            State current = holder.state;
            State next = current.copy();
            Tx tx = new Tx(scope, next, "expire-" + clock.instant().toEpochMilli(), Source.NONE, null);
            for (InMemoryVectorIndex.Indexed p : current.index.all()) {
                if (p.contentRef() != null && !exists(p.contentRef())) {
                    removePassage(tx, p.id(), Op.EXPIRE, "retention");
                }
            }
            for (InMemoryGraph.Node node : current.graph.nodes()) {
                if (node.contentRef() != null && !exists(node.contentRef())) {
                    removeNode(tx, node.id(), Op.EXPIRE, "retention", null);
                }
            }
            if (!tx.changed) {
                return 0;
            }
            reconcileDocs(next);
            holder.state = next;
            return tx.counts.entrySet().stream().filter(e -> e.getKey().startsWith("EXPIRE.PASSAGE")
                    || e.getKey().startsWith("EXPIRE.ENTITY")).mapToInt(Map.Entry::getValue).sum();
        } finally {
            holder.lock.unlock();
        }
    }

    private boolean exists(URI uri) {
        try {
            blobs.get(uri).toCompletableFuture().join();
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    // --- inspection ------------------------------------------------------------------------------------------------

    /**
     * Everything this scope keeps in memory that is text — ids, labels of unrestricted nodes, texts of unrestricted passages,
     * the log — for a scanner to look for content that must not be there. Restricted content is in blobs, not here.
     */
    public String dump(KnowledgeScope scope) {
        State s = holder(scope).state;
        StringBuilder out = new StringBuilder();
        s.index.all().forEach(p -> out.append(p.id()).append('|').append(p.documentId()).append('|').append(p.level())
                .append('|').append(p.text()).append('|').append(p.contentRef()).append('\n'));
        s.graph.nodes().forEach(n -> out.append(n.id()).append('|').append(n.type()).append('|').append(n.label())
                .append('|').append(n.properties()).append('|').append(n.contentRef()).append('|').append(n.sources())
                .append('\n'));
        s.graph.edges().forEach(e -> out.append(e).append('\n'));
        s.graph.allLinks().forEach(l -> out.append(l[0]).append('>').append(l[1]).append('\n'));
        s.log.all().forEach(e -> out.append(e).append('\n'));
        s.docs.values().forEach(d -> out.append(d).append('\n'));
        out.append(s.erasedDocs).append(s.erasedEntities).append(s.erasureRequests);
        return out.toString();
    }
}
