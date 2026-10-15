package io.stintflow.worker.publishing;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link PagePublisher} in memory, for tests and local runs (SDD 3a.6, 4.5): one page per space and key, a second
 * publish of the same key returns the first page. Never calls anything.
 */
public final class InMemoryPagePublisher implements PagePublisher {

    /** A page as it was published. */
    public record Stored(String id, Page page) {
    }

    private final Map<String, Stored> pages = new ConcurrentHashMap<>();
    private final AtomicInteger ids = new AtomicInteger();
    private final AtomicInteger calls = new AtomicInteger();

    @Override
    public PublishedPage publish(Page page) {
        calls.incrementAndGet();
        String slot = page.space() + "/" + page.key();
        boolean[] created = {false};
        Stored stored = pages.computeIfAbsent(slot, s -> {
            created[0] = true;
            return new Stored(String.valueOf(1000 + ids.incrementAndGet()), page);
        });
        return new PublishedPage(stored.id(), URI.create("memory://pages/" + page.space() + "/" + stored.id()), 1,
                created[0]);
    }

    public List<Stored> pages() {
        return List.copyOf(pages.values());
    }

    public int calls() {
        return calls.get();
    }
}
