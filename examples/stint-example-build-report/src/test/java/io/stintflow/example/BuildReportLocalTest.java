package io.stintflow.example;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.core.Json;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryStateStore;
import io.stintflow.inmemory.InMemoryTaskTransport;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.spi.BlobStore;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerRuntime;

/**
 * Proves the full distributed model end-to-end in a single JVM, with zero cloud: two remote tasks,
 * two dispatch→suspend→result cycles, an S3-style pointer flowing between them via the blob store.
 */
class BuildReportLocalTest {

    @Test
    @DisplayName("build_report_runs_two_remote_tasks_and_threads_the_pointer")
    void build_report_runs_two_remote_tasks_and_threads_the_pointer() throws Exception {
        Path blobDir = Files.createTempDirectory("stint-blobs");
        BlobStore blob = new FilesystemBlobStore(blobDir);

        TaskHandlerRegistry handlers = new TaskHandlerRegistry()
                .register(BuildReport.ROUTE_QUERY, new QueryAndStageHandler(blob))
                .register(BuildReport.ROUTE_FORMAT, new FormatFileHandler(blob));

        InMemoryTaskTransport transport = new InMemoryTaskTransport();
        transport.connectWorker(new WorkerRuntime(handlers, blob)::handle);

        WorkflowRegistry registry = new WorkflowRegistry();
        registry.register(BuildReport.definition());

        WorkflowEngine engine = new WorkflowEngine(registry, transport,
                new InMemoryStateStore(), new InMemoryTimerService(), blob);

        ObjectNode input = Json.obj();
        input.put("reportQuery", "SELECT * FROM orders");
        input.put("outputFormat", "xlsx");

        JsonNode result = engine.startAndWait(BuildReport.REF, input)
                .toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertThat(result.get("file").asText()).contains("final.xlsx");
        assertThat(result.get("rows").asInt()).isEqualTo(12_000);
        assertThat(result.get("pointer").asText()).contains("extract.json");
    }
}
