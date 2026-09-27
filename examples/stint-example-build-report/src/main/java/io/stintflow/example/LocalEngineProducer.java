package io.stintflow.example;

import java.nio.file.Path;
import java.util.List;

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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

/**
 * Wires a fully in-memory engine — the local/dev bundle. The whole distributed model (dispatch,
 * suspend, resume, claim-check) runs in one JVM with zero cloud. Swap this producer for an AWS one to
 * run the same {@link BuildReport} against floci or real AWS.
 * <p>
 * SDD 1.4: the running app registers the YAML-loaded definition (loaded from the classpath via the
 * build-time manifest, sec. 8a) — the YAML is the contract of truth now. {@link BuildReport#definition()}
 * (the Java model) is kept only as CA1's golden-test oracle.
 */
@ApplicationScoped
public class LocalEngineProducer {

    @Produces
    @ApplicationScoped
    public WorkflowEngine engine() {
        BlobStore blob = new FilesystemBlobStore(Path.of(System.getProperty("java.io.tmpdir"), "stint-blobs"));

        TaskHandlerRegistry handlers = new TaskHandlerRegistry()
                .register(BuildReport.ROUTE_QUERY, new QueryAndStageHandler(blob))
                .register(BuildReport.ROUTE_FORMAT, new FormatFileHandler(blob));

        InMemoryTaskTransport transport = new InMemoryTaskTransport();
        transport.connectWorker(new WorkerRuntime(handlers, blob)::handle);

        WorkflowRegistry registry = new WorkflowRegistry();
        List<WorkflowDefinition> defs = new DslLoader().loadClasspath("stint/workflows/*.yaml", LoadOptions.STRICT);
        defs.forEach(registry::register);

        return new WorkflowEngine(registry, transport, new InMemoryStateStore(),
                new InMemoryTimerService(), blob);
    }
}
