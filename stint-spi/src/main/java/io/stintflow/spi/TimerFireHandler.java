package io.stintflow.spi;

import java.util.concurrent.CompletionStage;

/** Callback the orchestrator registers with a {@link TimerService} to be notified when a timer fires. */
@FunctionalInterface
public interface TimerFireHandler {
    CompletionStage<Void> handle(TimerFire fire);
}
