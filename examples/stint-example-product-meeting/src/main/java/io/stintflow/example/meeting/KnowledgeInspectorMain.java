package io.stintflow.example.meeting;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.cloudevents.CloudEvent;
import io.stintflow.example.meeting.Corpus.Meeting;
import io.stintflow.knowledge.inspector.InspectorAccess;
import io.stintflow.knowledge.inspector.InspectorServer;
import io.stintflow.knowledge.inspector.InspectorService;
import io.stintflow.spi.Sensitivity;

/**
 * SDD 3b.6: the inspector over the base the product flow builds from the synthetic corpus, on loopback. Runs every meeting with fake
 * models, ingests the minutes, and serves the page at {@code http://127.0.0.1:<port>/} (first argument; 8085 by default). The
 * operator to type in the page is {@code demo}. A manual run: it stays up until it is stopped. Synthetic data only.
 */
public final class KnowledgeInspectorMain {

    private KnowledgeInspectorMain() {
    }

    public static void main(String... args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 8085;
        List<Meeting> meetings = Corpus.load();
        Path work = Files.createTempDirectory("stint-inspector");
        ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, ProductMeetingPipeline.fakes(meetings),
                new RunMetrics(Map.of(), false), work);
        KnowledgeIngestPipeline ingest = new KnowledgeIngestPipeline(pipeline.blobs,
                new FakeKnowledgeModel(Corpus.loadKnowledge().base().glossary()));
        for (Meeting meeting : meetings) {
            pipeline.run(meeting, Duration.ofSeconds(60));
            CloudEvent published = pipeline.channel.published().stream().filter(e -> e.getType().endsWith("minutes-published.v1"))
                    .filter(e -> meeting.id().equals(e.getSubject())).findFirst().orElseThrow();
            ingest.ingest(published, Duration.ofSeconds(60));
        }
        InspectorService service = new InspectorService(ingest.store,
                InspectorAccess.allowing(Map.of("demo", Set.of(KnowledgeIngestPipeline.SCOPE))),
                new InspectorService.Settings(500, Sensitivity.RESTRICTED, io.stintflow.knowledge.inspector.Metrics.Thresholds.DEFAULT));
        InspectorServer server = new InspectorServer(service, List.of(KnowledgeIngestPipeline.SCOPE),
                InspectorServer.operatorHeader(), port).start();
        System.out.println("KNOWLEDGE-INSPECTOR http://127.0.0.1:" + server.port() + "/  (operator: demo; Ctrl+C to stop)");
        Thread.currentThread().join();
    }
}
