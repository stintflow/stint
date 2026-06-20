package io.stintflow.example;

import java.nio.file.Path;

import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
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
        registry.register(BuildReport.definition());

        return new WorkflowEngine(registry, transport, new InMemoryStateStore(),
                new InMemoryTimerService(), blob);
    }
}
