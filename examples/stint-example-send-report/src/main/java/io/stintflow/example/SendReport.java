package io.stintflow.example;

import io.stintflow.spi.WorkflowRef;

/**
 * Constants shared between {@code send-report.yaml} and its registered worker (SDD 1.4, CA2/CA3).
 * Unlike {@code BuildReport}, there is no Java-model {@code definition()} here — this workflow is
 * YAML-only, loaded through {@link io.stintflow.dsl.DslLoader}.
 */
public final class SendReport {

    public static final WorkflowRef REF = new WorkflowRef("reports", "send-report", "1.0.0");

    public static final String ROUTE_SEND_EMAIL = "send-email";

    private SendReport() {
    }
}
