package io.stintflow.knowledge;

import java.util.List;
import java.util.Objects;

/**
 * What one write carries (SDD 3b.1, 4.2 and 4.6): the <em>complete</em> state of one document — its passages, the links
 * from them to entities, and the entities and relations the document asserts. Writing the same state again changes nothing.
 *
 * @param batchId       identifies this write, for the log (a redelivery repeats it)
 * @param documentId    the stable id of the source document, e.g. {@code meeting:m-001}
 * @param contentHash   a hash of the document content as the producer saw it
 * @param revision      the producer's version of the document, or {@code null}; an older revision than the last applied is
 *                      {@link WriteReceipt.Status#STALE}
 * @param source        where it comes from
 * @param schemaVersion the version of the graph schema the entities and relations follow; copied into the change log
 */
public record KnowledgeBatch(String batchId, String documentId, String contentHash, Long revision, Source source,
        String schemaVersion, List<Passage> passages, List<Entity> entities, List<Relation> relations,
        List<Link> links) {

    public KnowledgeBatch {
        Objects.requireNonNull(batchId, "batchId");
        Objects.requireNonNull(documentId, "documentId");
        Objects.requireNonNull(contentHash, "contentHash");
        Objects.requireNonNull(schemaVersion, "schemaVersion");
        source = source == null ? Source.NONE : source;
        passages = passages == null ? List.of() : List.copyOf(passages);
        entities = entities == null ? List.of() : List.copyOf(entities);
        relations = relations == null ? List.of() : List.copyOf(relations);
        links = links == null ? List.of() : List.copyOf(links);
    }
}
