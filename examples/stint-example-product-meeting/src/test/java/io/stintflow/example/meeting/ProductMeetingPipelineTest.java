package io.stintflow.example.meeting;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.stream.Stream;

import org.jboss.logmanager.ExtLogRecord;
import org.jboss.logmanager.LogContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.cloudevents.CloudEvent;
import io.stintflow.example.meeting.Corpus.Meeting;
import io.stintflow.example.meeting.ProductMeetingPipeline.MeetingRun;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.spi.BlobNotFoundException;
import io.stintflow.spi.InboxEntry;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.StateStore;
import io.stintflow.wire.CeWire;
import io.stintflow.worker.transcription.FakeTranscriber;

/**
 * SDD 3a.6, CA1 (local) and extras: every meeting of the corpus follows its key in one chain, and the report is
 * written; no marker leaves the restricted blobs — not in logs, spans, events, snapshots, the inbox, the published
 * pages or the report; the raw audio and transcript expire with their retention; an unavailable transcriber is retried
 * on the same audio; a redelivered recording publishes nothing twice. Fake models, synthetic data.
 */
class ProductMeetingPipelineTest {

    @TempDir
    Path work;

    private final List<Meeting> meetings = Corpus.load();

    @Test
    void ca1_every_meeting_follows_its_key_in_one_chain_and_the_report_is_written() throws Exception {
        RunMetrics metrics = new RunMetrics(Map.of(), false);
        List<MeetingRun> runs = new ArrayList<>();
        Map<String, byte[]> audio = new HashMap<>();
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, ProductMeetingPipeline.fakes(meetings),
                metrics, work)) {
            for (Meeting meeting : meetings) {
                runs.add(pipeline.run(meeting, Duration.ofSeconds(60)));
                audio.put(meeting.id(), ProductMeetingPipeline.syntheticAudio(meeting));
            }
            assertThat(ProductMeetingMain.followsTheKey(runs, meetings)).isTrue();
            assertThat(ProductMeetingMain.published(pipeline, meetings)).isTrue();
        }
        ObjectNode report = Report.build(runs, metrics, audio, "fake");
        Path written = Report.write(report, work.resolve("report"), "ci");

        assertThat(report.path("summary").path("docTypeAccuracy").asDouble()).isEqualTo(1.0);
        assertThat(report.path("summary").path("humanReviewRate").asDouble()).isEqualTo(0.125);
        assertThat(report.path("summary").path("uncertainRate").asDouble()).isEqualTo(0.125);
        assertThat(report.path("summary").path("estimatedCost").decimalValue()).isZero();
        assertThat(report.path("summary").path("meanWer").asDouble()).isZero();   // the fake hears the script
        assertThat(report.path("meetings").get(0).path("stepsMs").size()).isGreaterThanOrEqualTo(6);
        assertThat(report.path("meetings").get(0).path("inputTokens").asLong()).isPositive();
        assertThat(Files.exists(written)).isTrue();
        assertThat(Files.exists(work.resolve("report/ci.md"))).isTrue();
    }

    /** Every early event the engine kept. */
    static final class InboxProbe {
        final List<InboxEntry> kept = new CopyOnWriteArrayList<>();
    }

    @Test
    void no_marker_leaves_the_restricted_blobs() throws Exception {
        RunMetrics metrics = new RunMetrics(Map.of(), false);
        List<String> markers = Corpus.allMarkers(meetings);
        InboxProbe inbox = new InboxProbe();
        List<String> places = new ArrayList<>();
        List<MeetingRun> runs = new ArrayList<>();
        AllLogs logs = new AllLogs();
        try (logs; ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings,
                ProductMeetingPipeline.fakes(meetings), metrics, work, java.time.InstantSource.system(),
                new InMemoryTimerService(), store -> probe(store, inbox))) {
            for (Meeting meeting : meetings) {
                runs.add(pipeline.run(meeting, Duration.ofSeconds(60)));
            }
            for (MeetingRun run : runs) {
                places.add("snapshot " + run.meeting().id() + ": " + pipeline.state.load(run.instanceId())
                        .toCompletableFuture().get(5, TimeUnit.SECONDS));
                places.add("audit: " + pipeline.audit.of(run.instanceId()).toCompletableFuture().get());
            }
            for (CloudEvent event : pipeline.channel.published()) {
                places.add("event: " + new String(CeWire.toJson(event), StandardCharsets.UTF_8));
            }
            pipeline.confluence.pages().forEach(page -> places.add("page: " + page));
            places.add("corrections: " + pipeline.corrections.of(pipeline.definition.ref()).toCompletableFuture()
                    .get());
            assertThat(pipeline.confluence.pages()).hasSize(8)
                    .allSatisfy(page -> assertThat(page.body()).doesNotContain("SYNTH-PII"));
        }
        places.add("logs: " + String.join("\n", logs.lines));
        places.add("spans: " + metrics.spans);
        places.add("inbox: " + inbox.kept);
        ObjectNode report = Report.build(runs, metrics, Map.of(), "fake");
        Path written = Report.write(report, work.resolve("report"), "scan");
        places.add("report: " + Files.readString(written) + Files.readString(work.resolve("report/scan.md")));

        assertThat(places).allSatisfy(place -> {
            for (String marker : markers) {
                assertThat(place).doesNotContain(marker);
            }
        });
        // the markers did flow: they rest, and only, in blobs of a restricted class
        List<String> holding = new ArrayList<>();
        try (Stream<Path> files = Files.list(work.resolve("blobs"))) {
            for (Path file : files.toList()) {
                String text = new String(Files.readAllBytes(file), StandardCharsets.ISO_8859_1);
                if (markers.stream().anyMatch(text::contains)) {
                    holding.add(file.getFileName().toString());
                }
            }
        }
        assertThat(holding).isNotEmpty().allMatch(name -> name.startsWith("retention_restricted_")
                || name.startsWith("retention_raw-transcript_"));
        assertThat(logs.lines).isNotEmpty();
        assertThat(metrics.spans).isNotEmpty();
    }

    @Test
    void the_raw_audio_and_transcript_expire_and_the_minutes_live_on() throws Exception {
        RunMetrics metrics = new RunMetrics(Map.of(), false);
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, ProductMeetingPipeline.fakes(meetings),
                metrics, work)) {
            pipeline.run(meetings.getFirst(), Duration.ofSeconds(60));
        }
        AtomicReference<Instant> later = new AtomicReference<>(Instant.now().plus(Duration.ofDays(8)));
        FilesystemBlobStore eightDaysLater = new FilesystemBlobStore(work.resolve("blobs"), later::get);
        List<Path> files;
        try (Stream<Path> listed = Files.list(work.resolve("blobs"))) {
            files = listed.filter(p -> !p.getFileName().toString().endsWith(".expires")).toList();
        }
        Path audio = files.stream().filter(p -> p.getFileName().toString().startsWith("retention_raw-audio_"))
                .findFirst().orElseThrow();
        Path transcript = files.stream().filter(p -> p.getFileName().toString()
                .startsWith("retention_raw-transcript_")).findFirst().orElseThrow();
        Path minutes = files.stream().filter(p -> p.getFileName().toString().startsWith("retention_minutes_"))
                .findFirst().orElseThrow();

        for (Path gone : List.of(audio, transcript)) {
            assertThatThrownBy(() -> eightDaysLater.get(gone.toUri()).toCompletableFuture().join())
                    .satisfies(e -> assertThat(BlobNotFoundException.in(e)).isTrue());
        }
        assertThat(eightDaysLater.get(minutes.toUri()).toCompletableFuture().join()).isNotEmpty();   // 90 days
    }

    @Test
    void an_unavailable_transcriber_is_retried_on_the_same_audio() throws Exception {
        AtomicReference<Instant> now = new AtomicReference<>(Instant.now());   // the LLM worker keeps the system clock
        InMemoryTimerService timer = new InMemoryTimerService(now::get);
        ProductMeetingPipeline.Stack fakes = ProductMeetingPipeline.fakes(meetings);
        FakeTranscriber flaky = ((FakeTranscriber) fakes.transcriber()).failingFirst(2);
        ProductMeetingPipeline.Stack stack = new ProductMeetingPipeline.Stack(flaky, fakes.docType(), fakes.urgency(),
                fakes.minutes(), fakes.minutesProvider(), fakes.minutesModelName(), false, fakes.audio());
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, stack,
                new RunMetrics(Map.of(), false), work, now::get, timer, s -> s)) {
            CompletableFuture<MeetingRun> run = CompletableFuture.supplyAsync(() -> {
                try {
                    return pipeline.run(meetings.getFirst(), Duration.ofSeconds(60));
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            java.util.Set<String> passed = new java.util.HashSet<>();
            while (!run.isDone() && System.nanoTime() < until) {
                // each new retry wait moves the clock once, past its delay (30 s, then 60 s) — never to a timeout
                String retry = retryWait(pipeline);
                if (retry != null && passed.add(retry)) {
                    now.set(now.get().plusSeconds(61));
                    timer.tick();
                }
                Thread.onSpinWait();
            }
            MeetingRun done = run.get(5, TimeUnit.SECONDS);

            assertThat(done.status()).as("at %s, audit %s", pipeline.state.load(done.instanceId()).toCompletableFuture()
                    .join().map(sn -> sn.position()).orElse("?"), pipeline.audit.of(done.instanceId())
                    .toCompletableFuture().join()).isEqualTo(InstanceStatus.COMPLETED);
            assertThat(flaky.calls()).isEqualTo(3);   // two failures, then the same audio transcribed
            assertThat(done.context().path("pageId").asText()).isNotBlank();
        }
    }

    @Test
    void a_redelivered_recording_publishes_nothing_twice() throws Exception {
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, ProductMeetingPipeline.fakes(meetings),
                new RunMetrics(Map.of(), false), work)) {
            Meeting meeting = meetings.getFirst();
            MeetingRun first = pipeline.run(meeting, Duration.ofSeconds(60));
            CloudEvent again = ProductMeetingPipeline.meetingRecorded(meeting,
                    java.net.URI.create(first.context().path("audioRef").asText("file:///x")), 0);

            String sameId = pipeline.engine.startFromEvent(pipeline.definition.ref(), again).toCompletableFuture()
                    .get(10, TimeUnit.SECONDS);

            assertThat(sameId).isEqualTo(first.instanceId());
            assertThat(pipeline.confluence.pages()).hasSize(1);
            assertThat(pipeline.channel.published().stream()
                    .filter(e -> e.getType().endsWith("minutes-published.v1")).count()).isEqualTo(1);
        }
    }

    private static String retryWait(ProductMeetingPipeline pipeline) {
        String id = pipeline.current;
        return id == null ? null : pipeline.state.load(id).toCompletableFuture().join()
                .map(snapshot -> snapshot.waitingKey()).filter(key -> key.startsWith("retry:")).orElse(null);
    }

    private static StateStore probe(StateStore store, InboxProbe probe) {
        return (StateStore) java.lang.reflect.Proxy.newProxyInstance(StateStore.class.getClassLoader(),
                new Class<?>[] {StateStore.class}, (proxy, method, args) -> {
                    if (method.getName().equals("putInbox")) {
                        probe.kept.add((InboxEntry) args[0]);
                    }
                    try {
                        return method.invoke(store, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    /** Every record of every logger at every level, rendered with parameters, MDC and exception chain. */
    static final class AllLogs extends Handler implements AutoCloseable {
        final List<String> lines = new CopyOnWriteArrayList<>();
        private final java.util.logging.Logger root = LogContext.getLogContext().getLogger("");
        private final java.util.logging.Logger julRoot = java.util.logging.Logger.getLogger("");
        private final Level previous = root.getLevel();
        private final Level julPrevious = julRoot.getLevel();

        AllLogs() {
            setLevel(Level.ALL);
            root.setLevel(Level.ALL);
            root.addHandler(this);
            julRoot.setLevel(Level.ALL);
            julRoot.addHandler(this);
        }

        @Override
        public void publish(LogRecord record) {
            StringBuilder line = new StringBuilder(record.getLevel() + " " + record.getLoggerName() + " ");
            line.append(record instanceof ExtLogRecord ext ? ext.getFormattedMessage() : record.getMessage());
            if (record.getParameters() != null) {
                line.append(" params=").append(Arrays.asList(record.getParameters()));
            }
            if (record instanceof ExtLogRecord ext) {
                line.append(" mdc=").append(ext.getMdcCopy());
            }
            for (Throwable t = record.getThrown(); t != null; t = t.getCause()) {
                line.append(" thrown=").append(t);
            }
            lines.add(line.toString());
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
            root.removeHandler(this);
            root.setLevel(previous);
            julRoot.removeHandler(this);
            julRoot.setLevel(julPrevious);
        }
    }
}
