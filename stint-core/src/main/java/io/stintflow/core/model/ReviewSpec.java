package io.stintflow.core.model;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * SDD 3a.4 (8c): what makes a {@code listen} a phase of a review — set by the {@code stint-dsl} expansion of a
 * {@code call: review} or an {@code onUncertain.review}. With it, the engine correlates on the review's id (not on an
 * expression), checks an answer before it consumes it (role, source, contract) and records the ones it ignores, and
 * records the correction. A {@code listen} without it is unchanged.
 *
 * @param reviewPointer   the review's own pointer: the key of its record in the instance's decisions
 * @param decisionPointer the decision it reviews, or {@code null} for an approval or a standalone choice
 * @param kind            {@code choice} or {@code approval}
 * @param role            the role this phase waits for
 * @param earlierRoles    the roles asked in earlier phases: an answer from one of them now is {@code late}
 * @param options         a choice's option names
 * @param sources         the {@code source}s trusted to publish answers
 */
public record ReviewSpec(String reviewPointer, String decisionPointer, String kind, String role,
        List<String> earlierRoles, List<String> options, Set<String> sources) {

    public ReviewSpec {
        Objects.requireNonNull(reviewPointer, "reviewPointer");
        Objects.requireNonNull(role, "role");
        earlierRoles = earlierRoles == null ? List.of() : List.copyOf(earlierRoles);
        options = options == null ? List.of() : List.copyOf(options);
        sources = sources == null ? Set.of() : Set.copyOf(sources);
    }
}
