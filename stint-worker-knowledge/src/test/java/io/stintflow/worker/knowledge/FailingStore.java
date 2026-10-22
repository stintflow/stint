package io.stintflow.worker.knowledge;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.stintflow.knowledge.BaseInfo;
import io.stintflow.knowledge.ChangeEntry;
import io.stintflow.knowledge.ContextItem;
import io.stintflow.knowledge.DeleteReceipt;
import io.stintflow.knowledge.DeleteRequest;
import io.stintflow.knowledge.EntityMatch;
import io.stintflow.knowledge.EntityQuery;
import io.stintflow.knowledge.ErasureCheck;
import io.stintflow.knowledge.KnowledgeBatch;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.KnowledgeStore;
import io.stintflow.knowledge.ReindexReceipt;
import io.stintflow.knowledge.ReindexRequest;
import io.stintflow.knowledge.RetrievalQuery;
import io.stintflow.knowledge.WriteReceipt;

/** A store that fails every call, for the tests of what a worker does when its base is down. */
class FailingStore implements KnowledgeStore {

    private static <T> CompletionStage<T> down() {
        return CompletableFuture.failedFuture(new IllegalStateException("the base is down"));
    }

    @Override
    public CompletionStage<List<ContextItem>> retrieve(KnowledgeScope scope, RetrievalQuery query) {
        return down();
    }

    @Override
    public CompletionStage<WriteReceipt> write(KnowledgeScope scope, KnowledgeBatch batch) {
        return down();
    }

    @Override
    public CompletionStage<DeleteReceipt> delete(KnowledgeScope scope, DeleteRequest request) {
        return down();
    }

    @Override
    public CompletionStage<BaseInfo> describe(KnowledgeScope scope) {
        return down();
    }

    @Override
    public CompletionStage<ReindexReceipt> reindex(KnowledgeScope scope, ReindexRequest request) {
        return down();
    }

    @Override
    public CompletionStage<ErasureCheck> verifyErased(KnowledgeScope scope, DeleteRequest request) {
        return down();
    }

    @Override
    public CompletionStage<List<ChangeEntry>> changes(KnowledgeScope scope, long afterSequence, int limit) {
        return down();
    }

    @Override
    public CompletionStage<List<EntityMatch>> entities(KnowledgeScope scope, EntityQuery query) {
        return down();
    }
}
