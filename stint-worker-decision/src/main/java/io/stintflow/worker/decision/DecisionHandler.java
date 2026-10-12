package io.stintflow.worker.decision;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.spi.ErrorInfo.StandardType;
import io.stintflow.wire.decision.DecisionContract;
import io.stintflow.wire.decision.DecisionKind;
import io.stintflow.wire.decision.DecisionRequest;
import io.stintflow.wire.decision.DecisionResult;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;

/**
 * The task handler of one decision route (SDD 3a.2, 4.2): reads the {@link DecisionRequest}, asks the route's
 * {@link DecisionAdapter}, and answers with the decision contract's output — {@code uncertain} computed here, from the
 * adapter's threshold:
 * <ol>
 *   <li>no confidence → confidence 0, uncertain;</li>
 *   <li>a distribution whose two highest probabilities tie (within {@value #TIE}) → uncertain;</li>
 *   <li>otherwise uncertain when the calibrated confidence is below the threshold of the adapter (and kind) —
 *       a confidence equal to the threshold is certain.</li>
 * </ol>
 * A value that doesn't fit the request — not one of the options (SDD 3a.2, 8c option A), the wrong type — is a
 * {@code validation} error, never an uncertain decision: a low confidence is not an error, a malformed answer is.
 */
public final class DecisionHandler implements TaskHandler {

    static final double TIE = 1e-9;
    private static final Logger LOG = LoggerFactory.getLogger(DecisionHandler.class);

    private final DecisionConfig.Route route;
    private final DecisionAdapter adapter;
    private final DecisionConfig.AdapterSettings settings;
    private final ConfidenceCalibration calibration;
    private final DecisionPolicy policy;
    private final DecisionUsageSink usage;

    DecisionHandler(DecisionConfig.Route route, DecisionAdapter adapter, DecisionConfig.AdapterSettings settings,
            ConfidenceCalibration calibration, DecisionPolicy policy, DecisionUsageSink usage) {
        this.route = route;
        this.adapter = adapter;
        this.settings = settings;
        this.calibration = calibration;
        this.policy = policy;
        this.usage = usage;
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext ctx) {
        try {
            DecisionRequest request = request(ctx.input());
            AdapterDescriptor descriptor = adapter.descriptor();
            Optional<String> refusal = policy.refusal(ctx, descriptor);
            if (refusal.isPresent()) {
                throw DecisionFailures.refused("The decision policy refused adapter '" + descriptor.id() + "': "
                        + refusal.get());
            }
            checkTokenCeiling(request);
            return adapter.decide(request, new DecisionTask(ctx, route.instructions()))
                    .thenApply(decision -> answer(ctx, request, descriptor, decision))
                    .exceptionallyCompose(failure -> CompletableFuture.failedFuture(standard(failure)));
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(standard(e));
        }
    }

    private static DecisionRequest request(JsonNode input) {
        try {
            return DecisionRequest.fromJson(input);
        } catch (RuntimeException e) {
            throw DecisionFailures.validation("The task input is not a decision request (SDD 3a.1)");
        }
    }

    /** SDD 3a.2, 4.6: before the call, so a request over the ceiling costs nothing. Estimate: 4 characters a token. */
    private void checkTokenCeiling(DecisionRequest request) {
        Integer max = settings.maxInputTokens();
        if (max == null) {
            return;
        }
        long estimate = (request.toJson().toString().length() + (long) route.instructions().length()) / 4;
        if (estimate > max) {
            throw DecisionFailures.configuration(413, "The decision input is about " + estimate
                    + " tokens, over the adapter's max-input-tokens of " + max);
        }
    }

    private JsonNode answer(TaskContext ctx, DecisionRequest request, AdapterDescriptor descriptor, Decision decision) {
        checkValue(request, decision.value());
        Double raw = decision.confidence();
        if (raw != null && (raw.isNaN() || raw < 0 || raw > 1)) {
            throw DecisionFailures.validation("The decision's confidence must be a number in [0,1]");
        }
        double threshold = settings.thresholdFor(request.kind());
        double confidence;
        boolean uncertain;
        if (raw == null) {
            confidence = 0;
            uncertain = true;
        } else {
            confidence = Math.clamp(calibration.calibrate(request.kind(), raw, decision.distribution()), 0.0, 1.0);
            uncertain = confidence < threshold || tie(decision.distribution());
        }
        String model = decision.model() != null ? decision.model() : descriptor.model();
        DecisionResult result = new DecisionResult(decision.value(), confidence, uncertain,
                new DecisionResult.Provenance(descriptor.id(), descriptor.provider(), model, descriptor.version()));
        JsonNode output = result.toJson();
        List<String> violations = DecisionContract.violations(request, output);
        if (!violations.isEmpty()) {
            throw DecisionFailures.validation("The decision is out of its contract: " + String.join("; ", violations));
        }
        if (decision.usage() != null) {
            DecisionUsage used = decision.usage().withEstimatedCost(
                    settings.cost(decision.usage().inputTokens(), decision.usage().outputTokens()));
            usage.record(ctx, descriptor, model, used);
        }
        LOG.debug("Task {} of instance {} decided by adapter {}: confidence {} against threshold {}, uncertain {}",
                ctx.taskId(), ctx.instanceId(), descriptor.id(), confidence, threshold, uncertain);
        return output;
    }

    /** The answer fits the request's kind, and a choice is one of its options. Field names only, never values. */
    private static void checkValue(DecisionRequest request, JsonNode value) {
        DecisionKind kind = request.kind();
        boolean fits = switch (kind) {
            case CHOICE -> value != null && value.isTextual();
            case BOOLEAN -> value != null && value.isBoolean();
            case SCORE -> value != null && value.isNumber() && Double.isFinite(value.doubleValue());
        };
        if (!fits) {
            throw DecisionFailures.validation("The decision's value does not fit a " + kind.wireName() + " decision");
        }
        if (kind == DecisionKind.CHOICE
                && request.options().stream().noneMatch(o -> o.name().equals(value.textValue()))) {
            throw DecisionFailures.validation("The decision's value is not one of the options");
        }
    }

    static boolean tie(Map<String, Double> distribution) {
        if (distribution == null || distribution.size() < 2) {
            return false;
        }
        List<Double> sorted = new ArrayList<>(distribution.values());
        sorted.sort(Comparator.reverseOrder());
        return Math.abs(sorted.get(0) - sorted.get(1)) < TIE;
    }

    /** A failure as a standard error: a TaskFailure as is; anything else by its class only (its message may quote). */
    private static Throwable standard(Throwable failure) {
        Throwable cause = failure instanceof CompletionException && failure.getCause() != null
                ? failure.getCause() : failure;
        if (cause instanceof TaskFailure) {
            return cause;
        }
        LOG.warn("A decision adapter failed with {} (its message is not logged: it may quote the input)",
                cause.getClass().getName());
        return new TaskFailure(StandardType.RUNTIME.uri(), 500,
                "The decision adapter failed (" + cause.getClass().getSimpleName() + ")", null);
    }
}
