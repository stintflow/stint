package io.stintflow.worker.knowledge;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;

import io.stintflow.knowledge.ChangeEntry;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.KnowledgeStore;

/** The change log of a base, read in pages (SDD 3c.3): ids, kinds and sequences, never a content. */
final class ChangeLogs {

    private static final int PAGE = 1000;
    private static final int PAGES = 100;

    private ChangeLogs() {
    }

    static List<ChangeEntry> all(KnowledgeStore store, KnowledgeScope scope) {
        List<ChangeEntry> all = new ArrayList<>();
        long after = 0;
        for (int page = 0; page < PAGES; page++) {
            List<ChangeEntry> entries;
            try {
                entries = store.changes(scope, after, PAGE).toCompletableFuture().join();
            } catch (CompletionException e) {
                throw Failures.unavailable("The base could not be read (" + e.getCause().getClass().getSimpleName() + ")");
            }
            all.addAll(entries);
            if (entries.size() < PAGE) {
                return all;
            }
            after = entries.getLast().sequence();
        }
        throw Failures.unavailable("The change log is longer than a catalog task reads");
    }
}
