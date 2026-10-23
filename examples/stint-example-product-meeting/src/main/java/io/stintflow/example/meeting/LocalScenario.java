package io.stintflow.example.meeting;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.cloudevents.CloudEvent;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.example.meeting.Corpus.Meeting;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryAuditLog;
import io.stintflow.inmemory.InMemoryStateStore;
import io.stintflow.inmemory.InMemoryTaskTransport;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.otel.OpenTelemetryExecutionTracer;
import io.stintflow.worker.WorkerPool;
import io.stintflow.worker.WorkerRuntime;
import io.stintflow.worker.publishing.FakeConfluenceServer;

/**
 * The scenario of the milestone in the process (SDD 3c.6): one engine over memory, one worker pool holding every worker of the
 * scenario, a fake Confluence with the spaces of both areas, the review channel as the engine's publisher. The stack of
 * floci (DynamoDB, SQS, S3) is assembled by the integration test with the same {@link ScenarioWorkers} and {@link ScenarioRun}.
 */
public final class LocalScenario implements AutoCloseable {

    public final FilesystemBlobStore blobs;
    public final ReviewChannel channel;
    public final FakeConfluenceServer confluence;
    public final InMemoryStateStore state = new InMemoryStateStore();
    public final InMemoryAuditLog audit = new InMemoryAuditLog();
    public final RunMetrics metrics;
    public final ScenarioWorkers workers;
    public final WorkflowEngine engine;
    public final List<WorkflowDefinition> definitions;
    private final WorkerPool pool;

    public LocalScenario(List<Meeting> meetings, ProductMeetingPipeline.Stack stack, RunMetrics metrics, Path workDir,
            InstantSource clock, InMemoryTimerService timer) throws IOException {
        this.metrics = metrics;
        this.blobs = new FilesystemBlobStore(Files.createDirectories(workDir.resolve("blobs")), clock);
        this.channel = new ReviewChannel(URI.create(ProductMeetingPipeline.REVIEW_SOURCE));
        this.confluence = new FakeConfluenceServer(ProductMeetingPipeline.CONFLUENCE_EMAIL, ProductMeetingPipeline.CONFLUENCE_TOKEN,
                ProductMeetingPipeline.SPACE, "HR", ArchitectureAdrPipeline.SPACE);
        OpenTelemetryExecutionTracer tracer = new OpenTelemetryExecutionTracer(SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(metrics)).build().get("stint-example-scenario"));
        this.workers = new ScenarioWorkers(meetings, stack, metrics, blobs, tracer, confluence.baseUri(), clock, audit);
        this.pool = new WorkerPool(new WorkerRuntime(workers.handlers, blobs, tracer), 4);
        InMemoryTaskTransport transport = new InMemoryTaskTransport();
        transport.connectWorker(invoke -> {
            CompletableFuture<CloudEvent> result = new CompletableFuture<>();
            try {
                pool.submit(invoke, event -> {
                    result.complete(event);
                    return CompletableFuture.completedFuture(null);
                }, () -> { });
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                result.completeExceptionally(e);
            }
            return result;
        });
        this.definitions = ScenarioRun.definitions();
        WorkflowRegistry registry = new WorkflowRegistry().requirePolicy();
        definitions.forEach(registry::register);
        this.engine = new WorkflowEngine(registry, transport, state, timer, blobs, new TreeInterpreter(new JqExpressionEvaluator()),
                clock, channel, null, tracer).auditLog(audit);
        channel.answersTo(engine);
    }

    public ScenarioRun.Env env() {
        return new ScenarioRun.Env(engine, state, channel, blobs, metrics, definitions, workers, Duration.ofMinutes(3));
    }

    @Override
    public void close() {
        pool.close();
        channel.close();
        confluence.close();
    }
}
