package io.stintflow.spi;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Port: recurring schedules that start workflows (SDD 2.4, RF2) — DSL 1.0 {@code schedule.cron} and
 * {@code schedule.every}. One service manages one set of schedules (on AWS, one schedule group per
 * application); {@link #list} and {@link #remove} only ever see that set.
 * <p>
 * Delivery is at-least-once: an occurrence may reach {@link #onFire}'s handler more than once, and is
 * acknowledged only after the handler's stage completes normally.
 */
public interface ScheduleService {

    /** Creates the schedule if no schedule with its name exists. Idempotent and safe to call concurrently. */
    CompletionStage<Void> ensure(ScheduleSpec spec);

    /** Names of every schedule in this service's set. */
    CompletionStage<List<String>> list();

    /** Deletes a schedule. Idempotent — removing a missing one is a no-op. */
    CompletionStage<Void> remove(String name);

    /** Registers the handler for occurrences. */
    void onFire(ScheduleFireHandler handler);
}
