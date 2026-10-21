package io.stintflow.worker.knowledge;

import io.stintflow.spi.ErrorInfo.StandardType;
import io.stintflow.worker.TaskFailure;

/** The standard errors of the knowledge tasks (SDD 3b.3, 4.4): fixed sentences, ids and counts — never content. */
final class Failures {

    private Failures() {
    }

    static TaskFailure validation(String detail) {
        return new TaskFailure(StandardType.VALIDATION.uri(), 400, detail, null);
    }

    static TaskFailure refused(String detail) {
        return new TaskFailure(StandardType.AUTHORIZATION.uri(), 403, detail, null);
    }

    static TaskFailure unavailable(String detail) {
        return new TaskFailure(StandardType.COMMUNICATION.uri(), 503, detail, null);
    }
}
