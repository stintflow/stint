package io.stintflow.example.meeting;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.stintflow.spi.TraceAttributes;
import io.stintflow.worker.decision.DecisionUsageSink;
import io.stintflow.worker.langchain4j.UsageSink;
import io.stintflow.worker.transcription.FakeTranscriber;
import io.stintflow.worker.transcription.Transcriber;

/**
 * What a run measures (SDD 3a.6, 4.7), collected in memory: the spans (ids and durations only), the tokens of every
 * LLM step and decision, the transcripts' text — kept here only to compute the WER, never written anywhere — and,
 * in the manual run, the GPU's memory. The report reads it.
 */
public final class RunMetrics implements SpanExporter {

    /** Tokens and cost of one step of one instance. */
    public record Usage(String instanceId, String taskId, String model, long inputTokens, long outputTokens,
            BigDecimal cost) {
    }

    final List<SpanData> spans = new CopyOnWriteArrayList<>();
    final List<Usage> usage = new CopyOnWriteArrayList<>();
    final Map<String, String> transcripts = new ConcurrentHashMap<>();
    private final AtomicLong gpuPeakMiB = new AtomicLong(-1);
    private final Map<String, BigDecimal> pricePer1kTokens;
    private final boolean gpu;

    /**
     * @param pricePer1kTokens a remote model's price per thousand tokens; a model not listed is local: 0
     * @param gpu              whether to sample the GPU's memory (the manual run)
     */
    public RunMetrics(Map<String, BigDecimal> pricePer1kTokens, boolean gpu) {
        this.pricePer1kTokens = Map.copyOf(pricePer1kTokens);
        this.gpu = gpu;
    }

    // --- spans ---------------------------------------------------------------------------------------------------

    @Override
    public CompletableResultCode export(Collection<SpanData> batch) {
        spans.addAll(batch);
        return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode flush() {
        return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown() {
        return CompletableResultCode.ofSuccess();
    }

    /** How long each task of an instance ran, from its worker spans, by task id (summed over attempts). */
    public Map<String, Duration> stepLatencies(String instanceId) {
        Map<String, Duration> steps = new java.util.LinkedHashMap<>();
        for (SpanData span : spans) {
            if (!"stint.task".equals(span.getName())) {
                continue;
            }
            Map<String, String> attributes = new java.util.HashMap<>();
            span.getAttributes().forEach((k, v) -> attributes.put(k.getKey(), String.valueOf(v)));
            if (!instanceId.equals(attributes.get(TraceAttributes.INSTANCE_ID))) {
                continue;
            }
            String task = attributes.get(TraceAttributes.TASK_ID);
            steps.merge(task, Duration.ofNanos(span.getEndEpochNanos() - span.getStartEpochNanos()), Duration::plus);
        }
        return steps;
    }

    /** The trace ids of an instance's spans: one chain, one trace per activation linked by the chain id. */
    public List<String> chainIds(String instanceId) {
        List<String> chains = new ArrayList<>();
        for (SpanData span : spans) {
            Map<String, String> attributes = new java.util.HashMap<>();
            span.getAttributes().forEach((k, v) -> attributes.put(k.getKey(), String.valueOf(v)));
            if (instanceId.equals(attributes.get(TraceAttributes.INSTANCE_ID))
                    && attributes.get(TraceAttributes.CHAIN_ID) != null
                    && !chains.contains(attributes.get(TraceAttributes.CHAIN_ID))) {
                chains.add(attributes.get(TraceAttributes.CHAIN_ID));
            }
        }
        return chains;
    }

    // --- usage ---------------------------------------------------------------------------------------------------

    public UsageSink llmUsage() {
        return (task, used) -> usage.add(new Usage(task.instanceId(), task.taskId(), used.model(), used.inputTokens(),
                used.outputTokens(), cost(used.model(), used.inputTokens() + used.outputTokens())));
    }

    public DecisionUsageSink decisionUsage() {
        return (task, adapter, model, used) -> usage.add(new Usage(task.instanceId(), task.taskId(), model,
                used.inputTokens(), used.outputTokens(), used.estimatedCost() != null ? used.estimatedCost()
                        : cost(model, used.inputTokens() + used.outputTokens())));
    }

    private BigDecimal cost(String model, long tokens) {
        BigDecimal price = model == null ? null : pricePer1kTokens.get(model);
        return price == null ? BigDecimal.ZERO : price.multiply(BigDecimal.valueOf(tokens))
                .divide(BigDecimal.valueOf(1000));
    }

    // --- transcripts (for the WER only) ---------------------------------------------------------------------------

    /** The transcriber, recording what it heard by the audio's hash. */
    public Transcriber recording(Transcriber transcriber) {
        return new Transcriber() {
            @Override
            public Transcript transcribe(Audio audio, Options options) {
                Transcript transcript = transcriber.transcribe(audio, options);
                transcripts.put(FakeTranscriber.sha256(audio.bytes()), transcript.text());
                return transcript;
            }

            @Override
            public Provenance provenance() {
                return transcriber.provenance();
            }
        };
    }

    /** Word error rate of what was heard against the script: (substitutions + deletions + insertions) / words. */
    public static double wer(String reference, String hypothesis) {
        List<String> ref = words(reference);
        List<String> hyp = words(hypothesis);
        int[][] d = new int[ref.size() + 1][hyp.size() + 1];
        for (int i = 0; i <= ref.size(); i++) {
            d[i][0] = i;
        }
        for (int j = 0; j <= hyp.size(); j++) {
            d[0][j] = j;
        }
        for (int i = 1; i <= ref.size(); i++) {
            for (int j = 1; j <= hyp.size(); j++) {
                int same = ref.get(i - 1).equals(hyp.get(j - 1)) ? 0 : 1;
                d[i][j] = Math.min(Math.min(d[i - 1][j] + 1, d[i][j - 1] + 1), d[i - 1][j - 1] + same);
            }
        }
        return ref.isEmpty() ? 0 : (double) d[ref.size()][hyp.size()] / ref.size();
    }

    private static List<String> words(String text) {
        String normalized = text.toLowerCase(java.util.Locale.ROOT).replaceAll("[^a-z0-9@+\\-' ]", " ");
        return java.util.Arrays.stream(normalized.split("\\s+")).filter(w -> !w.isBlank()).toList();
    }

    // --- GPU (manual run) ----------------------------------------------------------------------------------------

    /** Samples the GPU's used memory with {@code nvidia-smi}; keeps the peak. Absent tool: no sample. */
    public void sampleGpu() {
        if (!gpu) {
            return;
        }
        try {
            Process process = new ProcessBuilder("nvidia-smi", "--query-gpu=memory.used",
                    "--format=csv,noheader,nounits").redirectErrorStream(true).start();
            try (BufferedReader out = new BufferedReader(new InputStreamReader(process.getInputStream(),
                    StandardCharsets.UTF_8))) {
                String line = out.readLine();
                if (line != null && line.strip().matches("\\d+")) {
                    gpuPeakMiB.accumulateAndGet(Long.parseLong(line.strip()), Math::max);
                }
            }
            process.waitFor();
        } catch (Exception e) {
            // no GPU tool here: the report says so
        }
    }

    public Optional<Long> gpuPeakMiB() {
        return gpuPeakMiB.get() < 0 ? Optional.empty() : Optional.of(gpuPeakMiB.get());
    }
}
