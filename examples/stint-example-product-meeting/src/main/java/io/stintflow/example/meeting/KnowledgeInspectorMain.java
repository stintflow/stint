package io.stintflow.example.meeting;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.cloudevents.CloudEvent;
import io.stintflow.example.meeting.Corpus.Meeting;
import io.stintflow.knowledge.ChangeEntry;
import io.stintflow.knowledge.GraphView;
import io.stintflow.knowledge.KnowledgeInspector;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.ViewQuery;
import io.stintflow.knowledge.inspector.InspectorAccess;
import io.stintflow.knowledge.inspector.InspectorServer;
import io.stintflow.knowledge.inspector.InspectorService;
import io.stintflow.spi.Sensitivity;

/**
 * SDD 3b.6: the inspector over the base the product flow builds from the synthetic corpus, on loopback. Runs every meeting with fake
 * models, ingests the minutes, and serves the page at {@code http://127.0.0.1:<port>/} (first argument; 8085 by default). The
 * operator to type in the page is {@code demo}. A second base, {@code demo-history}, holds two weeks of mock history ({@link DemoBase}). A manual run: it stays up until it is stopped. Synthetic data only.
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
        // a second base with two weeks of mock history, so the page has something to show (see DemoBase)
        io.stintflow.knowledge.inmemory.CompositeKnowledgeStore demo = DemoBase.build(work);
        KnowledgeInspector both = new KnowledgeInspector() {
            @Override
            public java.util.concurrent.CompletionStage<GraphView> view(KnowledgeScope scope, ViewQuery query) {
                return (scope.equals(DemoBase.SCOPE) ? demo : ingest.store).view(scope, query);
            }

            @Override
            public java.util.concurrent.CompletionStage<List<ChangeEntry>> changes(KnowledgeScope scope, long after, int limit) {
                return (scope.equals(DemoBase.SCOPE) ? demo : ingest.store).changes(scope, after, limit);
            }
        };
        InspectorService service = new InspectorService(both,
                InspectorAccess.allowing(Map.of("demo", Set.of(KnowledgeIngestPipeline.SCOPE, DemoBase.SCOPE))),
                new InspectorService.Settings(500, Sensitivity.RESTRICTED, io.stintflow.knowledge.inspector.Metrics.Thresholds.DEFAULT));
        InspectorServer server = new InspectorServer(service, List.of(KnowledgeIngestPipeline.SCOPE, DemoBase.SCOPE),
                InspectorServer.operatorHeader(), port).start();
        System.out.println("KNOWLEDGE-INSPECTOR http://127.0.0.1:" + server.port() + "/  (operator: demo; Ctrl+C to stop)");
        Thread.currentThread().join();
    }
}
