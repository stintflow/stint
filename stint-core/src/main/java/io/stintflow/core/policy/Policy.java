package io.stintflow.core.policy;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.stintflow.core.expr.Expr;
import io.stintflow.spi.Sensitivity;

/**
 * SDD 3a.5, RF3: a definition's compliance policy — the Stint extension {@code document.metadata.x-stint-policy}, not
 * part of the CNCF DSL. It names routes, never providers (ADR 3a.1-A: the provider exists only in the worker):
 * <ul>
 *   <li>{@code sensitivityFrom}: where a started instance's level comes from when its trigger event carries none
 *       (an expression over the workflow input); nothing found — or not a level — is {@code restricted};</li>
 *   <li>{@code routes}: the routing keys a dispatch may use at each level ({@value #ANY_ROUTE} = any);</li>
 *   <li>{@code retention}: blob retention classes, by name;</li>
 *   <li>{@code taskMax}: a task's own ceiling ({@code metadata.x-stint-policy.maxSensitivity}), by pointer.</li>
 * </ul>
 */
public record Policy(Expr sensitivityFrom, Map<Sensitivity, Set<String>> routes, Map<String, Duration> retention,
        Map<String, Sensitivity> taskMax) {

    /** The {@code document.metadata} (and task {@code metadata}) key. */
    public static final String METADATA_KEY = "x-stint-policy";
    public static final String ANY_ROUTE = "*";

    public Policy {
        EnumMap<Sensitivity, Set<String>> byLevel = new EnumMap<>(Sensitivity.class);
        routes.forEach((level, keys) -> byLevel.put(level, Set.copyOf(keys)));
        routes = Map.copyOf(byLevel);
        retention = Map.copyOf(retention);
        taskMax = Map.copyOf(taskMax);
    }

    /**
     * Why a dispatch of {@code routingKey} by the task at {@code pointer} is refused while the instance is at
     * {@code level}, or empty when it is allowed. A {@code null} level is {@code restricted}.
     */
    public Optional<String> refusal(String pointer, String routingKey, Sensitivity level) {
        Sensitivity at = level == null ? Sensitivity.RESTRICTED : level;
        Sensitivity ceiling = taskMax.get(pointer);
        if (ceiling != null && at.compareTo(ceiling) > 0) {
            return Optional.of("the task accepts data up to " + ceiling.wireName() + ", the instance is "
                    + at.wireName());
        }
        Set<String> allowed = routes.getOrDefault(at, Set.of());
        if (allowed.contains(ANY_ROUTE) || allowed.contains(routingKey)) {
            return Optional.empty();
        }
        return Optional.of("route '" + routingKey + "' is not allowed for " + at.wireName() + " data");
    }

    /** The retention of a blob class, if the policy declares it. */
    public Optional<Duration> retentionOf(String blobClass) {
        return Optional.ofNullable(retention.get(blobClass));
    }
}
