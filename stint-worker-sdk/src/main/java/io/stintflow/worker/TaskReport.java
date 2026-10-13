package io.stintflow.worker;

import java.util.concurrent.atomic.AtomicReference;

import io.stintflow.spi.ModelProvider;

/**
 * What a handler tells the runtime about its execution besides its output (SDD 3a.5, RF8): the AI provider that
 * produced it, which goes on the result event as {@code stintprovider} for the audit trail. Ids and names only, never
 * content. One per execution; a handler that calls no model reports nothing.
 */
public final class TaskReport {

    private final AtomicReference<ModelProvider> provider = new AtomicReference<>();

    /** The provider that produced this execution's output; the last one reported wins. */
    public void provider(ModelProvider provider) {
        this.provider.set(provider);
    }

    /** The reported provider, or {@code null}. */
    public ModelProvider provider() {
        return provider.get();
    }
}
