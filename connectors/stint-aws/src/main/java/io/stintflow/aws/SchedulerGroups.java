package io.stintflow.aws;

import java.util.concurrent.atomic.AtomicBoolean;

import software.amazon.awssdk.services.scheduler.SchedulerClient;
import software.amazon.awssdk.services.scheduler.model.ConflictException;

/** Creates an EventBridge Scheduler group once per process; an existing group is fine (SDD 2.4). */
final class SchedulerGroups {

    private SchedulerGroups() {
    }

    static void ensure(SchedulerClient scheduler, String group, AtomicBoolean done) {
        if (done.get() || "default".equals(group)) {
            return;
        }
        try {
            scheduler.createScheduleGroup(r -> r.name(group));
        } catch (ConflictException alreadyThere) {
            // created by another replica or an earlier run
        }
        done.set(true);
    }
}
