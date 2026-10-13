package io.stintflow.worker.decision;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import io.stintflow.spi.ModelProvider;
import io.stintflow.spi.Sensitivity;
import io.stintflow.worker.TaskContext;

/**
 * The worker side of SDD 3a.5 (ADR 3a.5-A, option C; 4.7): which provider may see an instance's data, by the level
 * the invoke carries. The engine already refused routes the level doesn't allow; this refuses a worker whose route
 * points at the wrong provider.
 * <ul>
 *   <li>{@code restricted} — and no level at all, the safe side — only a local provider ({@code remote = false});</li>
 *   <li>{@code confidential}: a local provider, or a remote one on {@code stint.policy.confidential.providers};</li>
 *   <li>{@code internal}: any configured provider.</li>
 * </ul>
 * A refusal is an {@code authorization} 403, without calling the provider. Restricted data with low confidence goes
 * to a human (SDD 3a.4), never to a remote provider: this policy would refuse that too.
 */
public final class SensitivityPolicy implements DecisionPolicy {

    public static final String PREFIX = "stint.policy.";
    public static final String CONFIDENTIAL_PROVIDERS = PREFIX + "confidential.providers";

    private final Set<String> confidentialProviders;

    public SensitivityPolicy(Set<String> confidentialProviders) {
        this.confidentialProviders = Set.copyOf(confidentialProviders);
    }

    /**
     * Reads every property under {@value #PREFIX}; the others are ignored.
     *
     * @throws IllegalArgumentException with every problem found, each with its key
     */
    public static SensitivityPolicy fromProperties(Map<String, String> properties) {
        List<String> problems = new ArrayList<>();
        Set<String> confidential = new LinkedHashSet<>();
        for (Map.Entry<String, String> entry : new TreeMap<>(properties).entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith(PREFIX)) {
                continue;
            }
            if (!key.equals(CONFIDENTIAL_PROVIDERS)) {
                problems.add(key + ": unknown policy key (restricted is local only and internal allows any "
                        + "provider: neither is configurable)");
                continue;
            }
            for (String provider : (entry.getValue() == null ? "" : entry.getValue()).split(",")) {
                if (!provider.isBlank()) {
                    confidential.add(provider.trim());
                }
            }
        }
        if (!problems.isEmpty()) {
            throw new IllegalArgumentException("Invalid policy configuration:\n  - " + String.join("\n  - ", problems));
        }
        return new SensitivityPolicy(confidential);
    }

    @Override
    public Optional<String> refusal(TaskContext task, AdapterDescriptor adapter) {
        String name = adapter.provider() != null ? adapter.provider() : adapter.id();
        return refusal(task.sensitivity(), name, adapter.remote());
    }

    /** Why {@code provider} may not see data of {@code level} ({@code null} = restricted), or empty. */
    public Optional<String> refusal(Sensitivity level, ModelProvider provider) {
        return refusal(level, provider.name(), provider.remote());
    }

    private Optional<String> refusal(Sensitivity level, String provider, boolean remote) {
        Sensitivity effective = level == null ? Sensitivity.RESTRICTED : level;
        if (!remote || effective == Sensitivity.INTERNAL) {
            return Optional.empty();
        }
        if (effective == Sensitivity.CONFIDENTIAL && confidentialProviders.contains(provider)) {
            return Optional.empty();
        }
        return Optional.of(effective == Sensitivity.RESTRICTED
                ? "the remote provider '" + provider + "' may not see restricted data (local providers only)"
                : "the remote provider '" + provider + "' is not approved for confidential data");
    }
}
