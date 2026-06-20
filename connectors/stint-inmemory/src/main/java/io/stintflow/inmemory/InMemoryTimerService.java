package io.stintflow.inmemory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import io.stintflow.spi.TimerRequest;
import io.stintflow.spi.TimerService;

/**
 * Records armed timers and lets them be cancelled. The fire→retry path is the documented MVP follow-up
 * (see WorkflowEngine), so this connector intentionally does not yet invoke a callback on expiry.
 */
public final class InMemoryTimerService implements TimerService {

    private final Map<String, TimerRequest> timers = new ConcurrentHashMap<>();

    @Override
    public CompletionStage<String> schedule(TimerRequest req) {
        timers.put(req.timerId(), req);
        return CompletableFuture.completedFuture(req.timerId());
    }

    @Override
    public CompletionStage<Void> cancel(String timerId) {
        timers.remove(timerId);
        return CompletableFuture.completedFuture(null);
    }
}
