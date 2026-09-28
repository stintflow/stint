package io.stintflow.example;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.wire.Json;
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
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerRuntime;

/**
 * SDD 1.4, CA2: {@code send-report.yaml} loaded permissively (RF3's escape hatch drops the
 * unsupported {@code schedule} and {@code emit} constructs, with a warning) runs end to end with
 * {@link SendEmailHandler} and a fake {@link MailGateway}.
 */
class SendReportLocalTest {

    @Test
    void send_report_yaml_runs_end_to_end_without_schedule_and_emit() throws Exception {
        Path blobDir = Files.createTempDirectory("stint-send-report");
        BlobStore blob = new FilesystemBlobStore(blobDir);
        FakeMailGateway gateway = new FakeMailGateway();

        TaskHandlerRegistry handlers = new TaskHandlerRegistry()
                .register(SendReport.ROUTE_SEND_EMAIL, new SendEmailHandler(gateway));

        InMemoryTaskTransport transport = new InMemoryTaskTransport();
        transport.connectWorker(new WorkerRuntime(handlers, blob)::handle);

        WorkflowRegistry registry = new WorkflowRegistry();
        List<WorkflowDefinition> defs = new DslLoader().loadClasspath("stint/workflows/*.yaml", LoadOptions.PERMISSIVE);
        defs.forEach(registry::register);

        WorkflowEngine engine = new WorkflowEngine(registry, transport, new InMemoryStateStore(),
                new InMemoryTimerService(), blob);

        ObjectNode input = Json.obj();
        input.put("recipient", "ops@example.com");
        input.put("subject", "Weekly report");
        input.put("body", "Everything is green.");

        JsonNode result = engine.startAndWait(SendReport.REF, input)
                .toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertThat(result.get("messageId").asText()).isEqualTo("msg-1");
        assertThat(gateway.sent()).hasSize(1);
        assertThat(gateway.sent().get(0).to()).isEqualTo("ops@example.com");
        assertThat(gateway.sent().get(0).subject()).isEqualTo("Weekly report");
    }
}
