package io.stintflow.worker.decision;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import io.stintflow.wire.decision.DecisionRequest;

/**
 * A deterministic adapter for tests and CI (SDD 3a.2, 4.4): it answers by a rule over the request — a value, a
 * confidence, a distribution, or an exception (a {@code TaskFailure} 429 with its {@code retryAfter}, say). No network,
 * no provider; synthetic data only. It records every request it got.
 */
public final class FakeDecisionAdapter implements DecisionAdapter {

    private final AdapterDescriptor descriptor;
    private final Function<DecisionRequest, Decision> rule;
    public final List<DecisionRequest> requests = new CopyOnWriteArrayList<>();

    public FakeDecisionAdapter(String id, Function<DecisionRequest, Decision> rule) {
        this.descriptor = new AdapterDescriptor(id, null, null, "1", false);
        this.rule = rule;
    }

    @Override
    public AdapterDescriptor descriptor() {
        return descriptor;
    }

    @Override
    public CompletionStage<Decision> decide(DecisionRequest request, DecisionTask task) {
        requests.add(request);
        try {
            Decision decision = rule.apply(request);
            return CompletableFuture.completedFuture(decision.usage() != null ? decision
                    : new Decision(decision.value(), decision.confidence(), decision.distribution(), decision.model(),
                            new DecisionUsage(1, 0, 0, Duration.ZERO, null)));
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}
