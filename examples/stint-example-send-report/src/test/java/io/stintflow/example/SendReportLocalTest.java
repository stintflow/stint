package io.stintflow.example;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.InstantSource;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.wire.Json;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.dsl.DslLoader;
import io.stintflow.dsl.LoadOptions;
import io.cloudevents.CloudEvent;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryEventPublisher;
import io.stintflow.inmemory.InMemoryStateStore;
import io.stintflow.inmemory.InMemoryTaskTransport;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.spi.BlobStore;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerRuntime;

/**
 * SDD 1.4, CA2: {@code send-report.yaml} loaded permissively (RF3's escape hatch drops the
 * unsupported {@code schedule}, with a warning) runs end to end with {@link SendEmailHandler} and a
 * fake {@link MailGateway}. SDD 2.2, CA8: its {@code emit} now runs too — the {@code report-sent} fact
 * is published on the domain channel.
 */
class SendReportLocalTest {

    @Test
    void send_report_yaml_runs_end_to_end_and_publishes_report_sent() throws Exception {
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

        InMemoryStateStore state = new InMemoryStateStore();
        InMemoryEventPublisher publisher = new InMemoryEventPublisher();
        CompletableFuture<CloudEvent> published = new CompletableFuture<>();
        publisher.subscribe(event -> {
            published.complete(event);
            return CompletableFuture.completedFuture(null);
        });
        WorkflowEngine engine = new WorkflowEngine(registry, transport, state, new InMemoryTimerService(), blob,
                new TreeInterpreter(new JqExpressionEvaluator()), InstantSource.system(), publisher, null);

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

        // SDD 2.2, CA8: the fact goes out on the domain channel right after the save that completed the
        // instance — await it on the publisher rather than racing it with awaitCompletion.
        CloudEvent fact = published.get(10, TimeUnit.SECONDS);
        assertThat(publisher.published()).hasSize(1);
        assertThat(fact.getType()).isEqualTo("io.stintflow.reports.report-sent.v1");
        assertThat(fact.getSource().toString()).isEqualTo("https://stintflow.io/reports/send-report");
        assertThat(Json.read(fact.getData().toBytes()).get("messageId").asText()).isEqualTo("msg-1");
    }
}
