package io.stintflow.core;

/**
 * Naming convention for typed {@link io.stintflow.spi.Wait} keys (SDD 1.2, RF2). Only the
 * {@code task:} kind is produced by this SDD; {@code timer:} and {@code event:<type>:} are for
 * SDD 1.3 and SDD 2.3 respectively, which is why the contract they share ({@code StateStore}) is
 * key-shape agnostic.
 */
final class WaitKeys {

    private static final String TASK_PREFIX = "task:";

    private WaitKeys() {
    }

    static String task(String correlationId) {
        return TASK_PREFIX + correlationId;
    }
}
