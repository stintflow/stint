package io.stintflow.example;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.core.Json;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.DataFlow;
import io.stintflow.spi.WorkflowRef;

/**
 * The recurring real-world workflow, modelled on the Stint engine:
 * <ol>
 *   <li><b>extractData</b> — query the DB and stage the result as a document, returning an S3 pointer;</li>
 *   <li><b>formatFile</b> — read the pointer and produce the final formatted file.</li>
 * </ol>
 * Both steps are {@code remote}: the orchestrator dispatches them over the transport and suspends.
 * <p>
 * SDD 1.1: migrated from the flat {@code List<RemoteStep>} model to the tree model, built here via
 * {@link WorkflowBuilder}. Most per-step mappers use the lambda {@link Expr} adapter (RF7), since
 * they are plain Java and there is no YAML front-end yet (SDD 1.4); {@code extractData}'s
 * {@code input.from} uses a real jq expression instead, so this example — the one CA7 builds native —
 * actually exercises the default {@code ExpressionEvaluator} at runtime, not just the adapter. Each
 * step's {@code export.as} merges its output into {@code $context} (read back by the next step's
 * {@code input.from}), reproducing the original accumulating-context behaviour exactly — this is
 * why {@link io.stintflow.core.WorkflowEngine}'s completion value is the final {@code $context}.
 * <p>
 * The equivalent CNCF Serverless Workflow definition lives in {@code resources/build-report.yaml}.
 */
public final class BuildReport {

    public static final WorkflowRef REF = new WorkflowRef("reports", "build-report", "1.0.0");

    public static final String ROUTE_QUERY = "query-and-stage";
    public static final String ROUTE_FORMAT = "format-file";

    private BuildReport() {
    }

    public static WorkflowDefinition definition() {
        DataFlow extractDataFlow = new DataFlow(
                Expr.jq("{query: $context.reportQuery}"),
                null, // output.as: identity — the raw {pointer, rows} becomes the flowing data
                Expr.of((context, output) -> {
                    ObjectNode merged = ((ObjectNode) context).deepCopy();
                    merged.set("pointer", output.get("pointer"));
                    merged.set("rows", output.get("rows"));
                    return merged;
                }));

        DataFlow formatFlow = new DataFlow(
                Expr.of((context, data) -> {
                    ObjectNode in = Json.obj();
                    in.set("source", context.get("pointer"));
                    in.set("format", context.get("outputFormat"));
                    return in;
                }),
                null,
                Expr.of((context, output) -> {
                    ObjectNode merged = ((ObjectNode) context).deepCopy();
                    merged.set("file", output.get("file"));
                    return merged;
                }));

        return WorkflowBuilder.create()
                .callRemote("extractData", ROUTE_QUERY, extractDataFlow)
                .callRemote("formatFile", ROUTE_FORMAT, formatFlow)
                .build(REF);
    }
}
