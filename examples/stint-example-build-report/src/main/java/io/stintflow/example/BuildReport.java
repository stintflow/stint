package io.stintflow.example;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.core.Json;
import io.stintflow.core.RemoteStep;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.spi.WorkflowRef;

/**
 * The recurring real-world workflow, modelled on the Stint engine:
 * <ol>
 *   <li><b>extractData</b> — query the DB and stage the result as a document, returning an S3 pointer;</li>
 *   <li><b>formatFile</b> — read the pointer and produce the final formatted file.</li>
 * </ol>
 * Both steps are {@code remote}: the orchestrator dispatches them over the transport and suspends.
 * <p>
 * The equivalent CNCF Serverless Workflow definition lives in {@code resources/build-report.yaml};
 * this Java model is what the MVP engine executes (the YAML front-end is a future increment).
 */
public final class BuildReport {

    public static final WorkflowRef REF = new WorkflowRef("reports", "build-report", "1.0.0");

    public static final String ROUTE_QUERY = "query-and-stage";
    public static final String ROUTE_FORMAT = "format-file";

    private BuildReport() {
    }

    public static WorkflowDefinition definition() {
        // Step 1: extractData
        Function<JsonNode, JsonNode> extractInput = ctx -> {
            ObjectNode in = Json.obj();
            in.set("query", ctx.get("reportQuery"));
            return in;
        };
        BiFunction<JsonNode, JsonNode, JsonNode> extractMerge = (ctx, out) -> {
            ObjectNode merged = ((ObjectNode) ctx).deepCopy();
            merged.set("pointer", out.get("pointer"));
            merged.set("rows", out.get("rows"));
            return merged;
        };

        // Step 2: formatFile
        Function<JsonNode, JsonNode> formatInput = ctx -> {
            ObjectNode in = Json.obj();
            in.set("source", ctx.get("pointer"));
            in.set("format", ctx.get("outputFormat"));
            return in;
        };
        BiFunction<JsonNode, JsonNode, JsonNode> formatMerge = (ctx, out) -> {
            ObjectNode merged = ((ObjectNode) ctx).deepCopy();
            merged.set("file", out.get("file"));
            return merged;
        };

        return new WorkflowDefinition(REF, List.of(
                new RemoteStep("extractData", ROUTE_QUERY, extractInput, extractMerge),
                new RemoteStep("formatFile", ROUTE_FORMAT, formatInput, formatMerge)));
    }
}
