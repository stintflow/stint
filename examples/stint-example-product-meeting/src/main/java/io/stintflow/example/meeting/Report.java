package io.stintflow.example.meeting;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.example.meeting.ProductMeetingPipeline.MeetingRun;
import io.stintflow.wire.Json;
import io.stintflow.worker.transcription.FakeTranscriber;

/**
 * The run's metrics report (SDD 3a.6, 4.7): one row per meeting and the run's totals — latency per step and in all,
 * tokens and estimated cost, review and uncertainty rates, accuracy against the key, rejected items, the WER of the
 * transcription and the GPU's peak memory. Ids, numbers, option names, providers and models only: never a word of a
 * meeting.
 */
public final class Report {

    private Report() {
    }

    public static ObjectNode build(List<MeetingRun> runs, RunMetrics metrics, Map<String, byte[]> audioByMeeting,
            String stack) {
        ObjectNode report = Json.obj();
        report.put("sdd", "3a.6");
        report.put("stack", stack);
        ArrayNode rows = report.putArray("meetings");
        int completed = 0;
        int docTypeRight = 0;
        int adapterRight = 0;
        int urgentRight = 0;
        int itemsRight = 0;
        int reviewed = 0;
        int uncertain = 0;
        int rejectedItems = 0;
        long tokensIn = 0;
        long tokensOut = 0;
        BigDecimal cost = BigDecimal.ZERO;
        List<Long> totals = new ArrayList<>();
        List<Double> wers = new ArrayList<>();
        for (MeetingRun run : runs) {
            JsonNode context = run.context() == null ? Json.obj() : run.context();
            ObjectNode row = rows.addObject();
            row.put("meetingId", run.meeting().id());
            row.put("instanceId", run.instanceId());
            row.put("status", run.status().name());
            String docType = context.path("docType").asText(null);
            String byAdapter = run.docTypeDecision() == null ? null : run.docTypeDecision().path("value").asText(null);
            boolean wasUncertain = run.docTypeDecision() != null
                    && run.docTypeDecision().path("uncertain").asBoolean(false);
            boolean wasReviewed = run.reviewsAnswered().contains("choice");
            row.put("docType", docType);
            row.put("docTypeByAdapter", byAdapter);
            row.put("expectedDocType", run.meeting().expected().docType());
            row.put("uncertain", wasUncertain);
            row.put("reviewed", wasReviewed);
            row.put("urgent", context.path("urgent").asBoolean());
            row.put("expectedUrgent", run.meeting().expected().urgent());
            row.put("items", context.path("itemIds").size());
            row.put("expectedItems", run.meeting().expected().items().size());
            row.put("verdict", context.path("verdict").asText(null));
            row.set("rejectedItems", context.path("rejectedItems").isArray() ? context.get("rejectedItems")
                    : Json.MAPPER.createArrayNode());
            row.put("pageId", context.path("pageId").asText(null));
            row.put("totalMs", run.total().toMillis());
            ObjectNode steps = row.putObject("stepsMs");
            run.steps().forEach((task, latency) -> steps.put(task, latency.toMillis()));
            row.set("chains", Json.MAPPER.valueToTree(run.chains()));
            long in = 0;
            long out = 0;
            BigDecimal rowCost = BigDecimal.ZERO;
            for (RunMetrics.Usage usage : metrics.usage) {
                if (usage.instanceId().equals(run.instanceId())) {
                    in += usage.inputTokens();
                    out += usage.outputTokens();
                    rowCost = rowCost.add(usage.cost());
                }
            }
            row.put("inputTokens", in);
            row.put("outputTokens", out);
            row.put("estimatedCost", rowCost);
            String heard = audioByMeeting.containsKey(run.meeting().id())
                    ? metrics.transcripts.get(FakeTranscriber.sha256(audioByMeeting.get(run.meeting().id()))) : null;
            if (heard != null) {
                double wer = RunMetrics.wer(run.meeting().text(), heard);
                row.put("wer", Math.round(wer * 10_000) / 10_000.0);
                wers.add(wer);
            }
            completed += run.status() == io.stintflow.spi.InstanceSnapshot.InstanceStatus.COMPLETED ? 1 : 0;
            docTypeRight += run.meeting().expected().docType().equals(docType) ? 1 : 0;
            adapterRight += run.meeting().expected().docType().equals(byAdapter) && !wasUncertain ? 1 : 0;
            urgentRight += run.meeting().expected().urgent() == context.path("urgent").asBoolean() ? 1 : 0;
            itemsRight += context.path("itemIds").size() == run.meeting().expected().items().size() ? 1 : 0;
            reviewed += wasReviewed ? 1 : 0;
            uncertain += wasUncertain ? 1 : 0;
            rejectedItems += context.path("rejectedItems").size();
            tokensIn += in;
            tokensOut += out;
            cost = cost.add(rowCost);
            totals.add(run.total().toMillis());
        }
        int n = runs.size();
        ObjectNode summary = report.putObject("summary");
        summary.put("meetings", n);
        summary.put("completed", completed);
        summary.put("docTypeAccuracy", ratio(docTypeRight, n));
        summary.put("docTypeAccuracyOfTheAdapterAlone", ratio(adapterRight, n));
        summary.put("urgencyAccuracy", ratio(urgentRight, n));
        summary.put("itemsMatchingTheKey", ratio(itemsRight, n));
        summary.put("humanReviewRate", ratio(reviewed, n));
        summary.put("uncertainRate", ratio(uncertain, n));
        summary.put("rejectedItems", rejectedItems);
        summary.put("inputTokens", tokensIn);
        summary.put("outputTokens", tokensOut);
        summary.put("estimatedCost", cost);
        totals.sort(null);
        summary.put("latencyP50Ms", totals.isEmpty() ? 0 : totals.get(totals.size() / 2));
        summary.put("latencyMaxMs", totals.isEmpty() ? 0 : totals.getLast());
        if (!wers.isEmpty()) {
            summary.put("meanWer", Math.round(wers.stream().mapToDouble(d -> d).average().orElse(0) * 10_000)
                    / 10_000.0);
        }
        metrics.gpuPeakMiB().ifPresent(peak -> summary.put("gpuPeakMiB", peak));
        return report;
    }

    private static double ratio(int part, int whole) {
        return whole == 0 ? 0 : Math.round(1000.0 * part / whole) / 1000.0;
    }

    /** Writes {@code <name>.json} and {@code <name>.md} in {@code dir}; returns the JSON's path. */
    public static Path write(ObjectNode report, Path dir, String name) throws IOException {
        Files.createDirectories(dir);
        Path json = dir.resolve(name + ".json");
        Files.writeString(json, Json.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(report),
                StandardCharsets.UTF_8);
        StringBuilder md = new StringBuilder("# Product meeting run (SDD 3a.6)\n\nStack: ")
                .append(report.path("stack").asText()).append("\n\n| Metric | Value |\n|---|---|\n");
        report.path("summary").properties().forEach(e -> md.append("| ").append(e.getKey()).append(" | ")
                .append(e.getValue().asText()).append(" |\n"));
        md.append("\n| Meeting | Status | Doc type (adapter → final, key) | Reviewed | Urgent (key) | Items (key) | "
                + "Total ms | WER |\n|---|---|---|---|---|---|---|---|\n");
        for (JsonNode row : report.path("meetings")) {
            md.append("| ").append(row.path("meetingId").asText()).append(" | ").append(row.path("status").asText())
                    .append(" | ").append(row.path("docTypeByAdapter").asText()).append(" → ")
                    .append(row.path("docType").asText()).append(" (").append(row.path("expectedDocType").asText())
                    .append(") | ").append(row.path("reviewed").asBoolean()).append(" | ")
                    .append(row.path("urgent").asBoolean()).append(" (").append(row.path("expectedUrgent").asBoolean())
                    .append(") | ").append(row.path("items").asInt()).append(" (")
                    .append(row.path("expectedItems").asInt()).append(") | ").append(row.path("totalMs").asLong())
                    .append(" | ").append(row.has("wer") ? row.path("wer").asText() : "-").append(" |\n");
        }
        Files.writeString(dir.resolve(name + ".md"), md.toString(), StandardCharsets.UTF_8);
        return json;
    }

    static Duration ms(long millis) {
        return Duration.ofMillis(millis);
    }
}
