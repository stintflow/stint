package io.stintflow.worker.decision;

import java.util.concurrent.CompletionStage;

import io.stintflow.wire.decision.DecisionRequest;

/**
 * Something that decides (SDD 3a.2, 4.2): a model, a service, a set of rules. It answers a {@link DecisionRequest} with
 * a raw {@link Decision}; the {@link DecisionHandler} around it applies the adapter's threshold, builds the decision
 * contract's output and records the usage. An adapter never computes {@code uncertain} itself.
 */
public interface DecisionAdapter {

    /** Who decides: id, provider, model, version — the provenance, and what the SDD 3a.5 policy sees. */
    AdapterDescriptor descriptor();

    /**
     * The raw answer to {@code request}. A provider failure fails the stage with a {@code TaskFailure} (a standard
     * error); an answer that can't be read fails it with a {@code validation} one ({@link DecisionFailures}).
     */
    CompletionStage<Decision> decide(DecisionRequest request, DecisionTask task);
}
