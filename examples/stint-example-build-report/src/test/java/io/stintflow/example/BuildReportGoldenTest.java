package io.stintflow.example;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import io.stintflow.core.Json;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.dsl.DslLoader;
import io.stintflow.dsl.LoadOptions;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryStateStore;
import io.stintflow.inmemory.InMemoryTaskTransport;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.spi.BlobStore;
import io.stintflow.worker.TaskHandler;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerRuntime;

/**
 * SDD 1.4, CA1: {@code build-report.yaml} (compiled by {@link DslLoader}) and {@link BuildReport}'s
 * Java model must produce the exact same runtime behaviour for the same input. Criterion (sec. 8g):
 * same final output, same ordered sequence of dispatched tasks, both complete successfully —
 * excluding non-deterministic ids/timestamps, which never appear in either comparison.
 */
class BuildReportGoldenTest {

    @Test
    void yaml_and_java_model_produce_the_same_output_and_dispatch_sequence() throws Exception {
        ObjectNode input = Json.obj();
        input.put("reportQuery", "SELECT * FROM orders");
        input.put("outputFormat", "xlsx");

        Result javaResult = run(BuildReport.definition(), input);

        WorkflowDefinition yamlDef;
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("stint/workflows/build-report.yaml")) {
            yamlDef = new DslLoader().load(in, "build-report.yaml", LoadOptions.STRICT);
        }
        Result yamlResult = run(yamlDef, input);

        // sec. 8g explicitly excludes non-deterministic ids from the comparison; the blob pointer
        // URIs embed a per-run temp dir and a per-run random instanceId, so they're normalized to
        // just their file name (still proving both runs staged/read the same files, in the same
        // order) before comparing — everything else compares byte-for-byte.
        assertThat(normalizeUris(yamlResult.output())).isEqualTo(normalizeUris(javaResult.output()));
        assertThat(yamlResult.dispatches()).isEqualTo(javaResult.dispatches());
    }

    private static final Pattern UUID_PATTERN = Pattern.compile(
            "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");

    private static JsonNode normalizeUris(JsonNode node) {
        if (node.isObject()) {
            ObjectNode copy = Json.obj();
            node.fields().forEachRemaining(e -> copy.set(e.getKey(), normalizeUris(e.getValue())));
            return copy;
        }
        if (node.isTextual() && node.asText().startsWith("file:///")) {
            String v = node.asText();
            String basename = v.substring(v.lastIndexOf('/') + 1);
            return TextNode.valueOf(UUID_PATTERN.matcher(basename).replaceAll("<instance>"));
        }
        return node;
    }

    private record Result(JsonNode output, List<String> dispatches) {
    }

    private Result run(WorkflowDefinition def, ObjectNode input) throws Exception {
        Path blobDir = Files.createTempDirectory("stint-golden");
        BlobStore blob = new FilesystemBlobStore(blobDir);
        List<String> dispatches = new ArrayList<>();

        TaskHandlerRegistry handlers = new TaskHandlerRegistry()
                .register(BuildReport.ROUTE_QUERY, recording(BuildReport.ROUTE_QUERY, new QueryAndStageHandler(blob), dispatches))
                .register(BuildReport.ROUTE_FORMAT, recording(BuildReport.ROUTE_FORMAT, new FormatFileHandler(blob), dispatches));

        InMemoryTaskTransport transport = new InMemoryTaskTransport();
        transport.connectWorker(new WorkerRuntime(handlers, blob)::handle);

        WorkflowRegistry registry = new WorkflowRegistry();
        registry.register(def);

        WorkflowEngine engine = new WorkflowEngine(registry, transport, new InMemoryStateStore(),
                new InMemoryTimerService(), blob);

        JsonNode output = engine.startAndWait(def.ref(), input).toCompletableFuture().get(10, TimeUnit.SECONDS);
        return new Result(output, dispatches);
    }

    private static TaskHandler recording(String routingKey, TaskHandler delegate, List<String> sink) {
        return ctx -> {
            sink.add(routingKey + ":" + normalizeUris(ctx.input()));
            return delegate.execute(ctx);
        };
    }
}
