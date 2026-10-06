package io.stintflow.example.archive;

import io.stintflow.spi.WorkflowRef;

/**
 * Constants shared between {@code archive-report.yaml} and its worker (SDD 2.6, RF2). The fact type is the
 * only thing this workflow knows about the outside world: a contract, not a reference to who publishes it.
 */
public final class ArchiveReport {

    public static final WorkflowRef REF = new WorkflowRef("archive", "archive-report", "1.0.0");

    public static final String ROUTE_ARCHIVE_RECORD = "archive-record";

    /** The domain fact that starts this workflow. */
    public static final String REPORT_SENT_TYPE = "io.stintflow.reports.report-sent.v1";

    private ArchiveReport() {
    }
}
