package io.stintflow.core.schedule;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.spi.ScheduleFire;
import io.stintflow.spi.ScheduleFireHandler;
import io.stintflow.spi.ScheduleSpec;
import io.stintflow.spi.wire.StintEvents;

/**
 * Starts an instance for each schedule occurrence (SDD 2.4, RF6, sec. 8e) through the idempotent start of
 * SDD 2.1. The occurrence becomes a synthetic CloudEvent — a Stint extension, never published:
 * <ul>
 *   <li>{@code type}: {@value StintEvents#TYPE_SCHEDULE_FIRED};</li>
 *   <li>{@code source}: {@code stint:schedule/<definition>};</li>
 *   <li>{@code id}: {@code <schedule>@<scheduled instant, ISO-8601 UTC>};</li>
 *   <li>{@code time}: the scheduled instant.</li>
 * </ul>
 * The instance id is therefore a function of the occurrence: a duplicated delivery is a no-op, and each
 * occurrence starts a new chain (SDD 2.5).
 * <p>
 * Sometimes the connector can't tell the scheduled instant (sec. 8e fallback: the scheduler left its
 * placeholder unsubstituted). It is then the last occurrence of the cron at or before the delivery's
 * {@code observedAt}. That value is stable across redeliveries of the same message, and across repeated
 * invocations delayed by less than one period.
 */
public final class ScheduledStart implements ScheduleFireHandler {

    private static final Logger LOG = System.getLogger(ScheduledStart.class.getName());

    private final WorkflowRegistry registry;
    private final WorkflowEngine engine;

    public ScheduledStart(WorkflowRegistry registry, WorkflowEngine engine) {
        this.registry = registry;
        this.engine = engine;
    }

    @Override
    public CompletionStage<Void> handle(ScheduleFire fire) {
        ScheduleSpec spec = registry.scheduleSpec(fire.scheduleName()).orElse(null);
        if (spec == null) {
            // Like an SDD 2.1 binding to an unknown definition (deploy skew): not acked, so it is redelivered.
            return CompletableFuture.failedFuture(new IllegalStateException("Schedule " + fire.scheduleName()
                    + " (" + fire.definition() + ") is not wanted by any definition registered here"));
        }
        Instant scheduled = fire.scheduledTime();
        if (scheduled == null) {
            scheduled = spec.cron().lastAtOrBefore(fire.observedAt(), spec.zone());
            LOG.log(Level.WARNING, "Schedule {0} delivered without its scheduled time; derived {1} from {2}",
                    spec.name(), scheduled, fire.observedAt());
        }
        return engine.startFromEvent(spec.definition(), syntheticEvent(spec, scheduled)).thenApply(id -> null);
    }

    /** The CloudEvent an occurrence starts its instance with. */
    public static CloudEvent syntheticEvent(ScheduleSpec spec, Instant scheduled) {
        return CloudEventBuilder.v1()
                .withId(spec.name() + "@" + scheduled)
                .withSource(URI.create("stint:schedule/" + spec.definition().canonical()))
                .withType(StintEvents.TYPE_SCHEDULE_FIRED)
                .withTime(scheduled.atOffset(ZoneOffset.UTC))
                .build();
    }
}
