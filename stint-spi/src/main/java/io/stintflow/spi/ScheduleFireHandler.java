package io.stintflow.spi;

import java.util.concurrent.CompletionStage;

/** Receives schedule occurrences; the connector acknowledges one only after the returned stage completes. */
@FunctionalInterface
public interface ScheduleFireHandler {

    CompletionStage<Void> handle(ScheduleFire fire);
}
