package io.stintflow.example.meeting;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.knowledge.ViewQuery;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.Sensitivity;
import io.stintflow.wire.Json;

/**
 * The report of the milestone (SDD 3c.6): the product area's rows and totals (the 3a.6 report), the decisions the base holds
 * against the key, the architecture area's pairs against the key — the conflicts found, missed and invented, the pairs a
 * person judged, the origin that did not answer — the quality of the entity resolution, and the cost of the run. Ids, numbers,
 * option names and statuses: never a word of a meeting, a summary or a page. Under fake models the numbers prove the plumbing
 * and the agreement with the key; they say nothing of a model, and the report says so.
 */
public final class ScenarioReport {

    public static final String NOTE = "Metrics of fake models measure the plumbing and the agreement with the answer key, not the "
            + "quality of a model; the value of the POC is measured in the manual run with real local models.";

    private ScenarioReport() {
    }

    /**
     * @param stack  where it ran: {@code local} or {@code floci}
     * @param models {@code fake} or {@code real}
     */
    public static ObjectNode build(ScenarioRun.Result result, ScenarioRun.Env env, String stack, String models) {
        ObjectNode report = Json.obj();
        report.put("sdd", "3c.6");
        report.put("stack", stack);
        report.put("models", models);
        report.put("language", "en");
        report.put("note", NOTE);

        report.set("product", Report.build(result.product(), env.metrics(), Map.of(), stack + " " + models));

        // the decisions the product base holds against the key
        List<Corpus.ProductDecision> key = Corpus.loadProductDecisions();
        var nodes = env.workers().product.view(KnowledgeIngestPipeline.SCOPE, new ViewQuery(Sensitivity.RESTRICTED, 500, null, null))
                .toCompletableFuture().join().nodes();
        long inGraph = key.stream().filter(d -> nodes.stream().anyMatch(n -> n.type().equals("Decision") && d.label().equals(n.label())))
                .count();
        ObjectNode decisions = report.putObject("productDecisions");
        decisions.put("expected", key.size()).put("inTheGraph", inGraph).put("announcedPointers", result.announced().size());

        // the architecture area: pair by pair, and the matrix of conflicts
        ArrayNode rows = report.putArray("architecture");
        int truePositive = 0;
        int falsePositive = 0;
        int falseNegative = 0;
        int trueNegative = 0;
        int reviewed = 0;
        int decidedByMachine = 0;
        int machineRight = 0;
        int unavailable = 0;
        int completed = 0;
        int reviewsAsked = 0;
        List<Long> totals = new ArrayList<>();
        for (ScenarioRun.PairRun run : result.architecture()) {
            ObjectNode row = rows.addObject();
            row.put("meetingId", run.meetingId()).put("draftId", run.draftId()).put("instanceId", run.instanceId());
            row.put("status", run.status().name());
            row.put("expectedVerdict", run.expectedVerdict()).put("verdict", run.verdict());
            row.put("expectedReviewed", run.expectedReviewed()).put("reviewed", run.reviewed());
            row.put("machineVerdict", run.auto());
            row.put("resolution", run.resolution()).put("pageStatus", run.pageStatus());
            row.put("originDown", run.originDown()).put("reviewsAnswered", run.reviewsAnswered());
            row.put("totalMs", run.total().toMillis());
            ObjectNode steps = row.putObject("stepsMs");
            run.steps().forEach((task, latency) -> steps.put(task, latency.toMillis()));
            completed += run.status() == InstanceStatus.COMPLETED ? 1 : 0;
            boolean expectedConflict = "conflicts".equals(run.expectedVerdict());
            boolean gotConflict = "conflicts".equals(run.verdict());
            truePositive += expectedConflict && gotConflict ? 1 : 0;
            falseNegative += expectedConflict && !gotConflict ? 1 : 0;
            falsePositive += !expectedConflict && gotConflict ? 1 : 0;
            trueNegative += !expectedConflict && !gotConflict ? 1 : 0;
            reviewed += run.reviewed() ? 1 : 0;
            reviewsAsked += run.reviewsAnswered();
            unavailable += "unavailable".equals(run.resolution()) ? 1 : 0;
            if (run.auto() != null) {
                decidedByMachine++;
                machineRight += run.auto().equals(run.expectedVerdict()) ? 1 : 0;
            }
            totals.add(run.total().toMillis());
        }
        int pairs = result.architecture().size();
        ObjectNode conflicts = report.putObject("conflicts");
        conflicts.put("pairs", pairs).put("truePositive", truePositive).put("falsePositive", falsePositive)
                .put("falseNegative", falseNegative).put("trueNegative", trueNegative);
        conflicts.put("precision", ratio(truePositive, truePositive + falsePositive));
        conflicts.put("recall", ratio(truePositive, truePositive + falseNegative));
        conflicts.put("reviewedByAPerson", reviewed).put("reviewRate", ratio(reviewed, pairs));
        conflicts.put("decidedByTheMachine", decidedByMachine).put("machineAgreesWithTheKey", ratio(machineRight, decidedByMachine));
        conflicts.put("originUnavailable", unavailable).put("reviewRequestsAnswered", reviewsAsked);

        // the quality of the entity resolution over the evaluation set
        ResolutionEval.Quality quality = ResolutionEval.measure();
        ObjectNode resolution = report.putObject("entityResolution");
        resolution.put("mentions", quality.mentions()).put("resolvable", quality.resolvable())
                .put("resolvedRight", quality.resolvedRight()).put("recall", ratio(quality.resolvedRight(), quality.resolvable()))
                .put("wrongMerges", quality.wrongMerges()).put("newRight", quality.newRight()).put("newExpected", quality.newExpected())
                .put("duplicateCandidatesFlagged", quality.candidatesFound()).put("duplicateCandidatesExpected", quality.candidatesExpected());

        // the cost of the whole run
        long in = 0;
        long out = 0;
        BigDecimal cost = BigDecimal.ZERO;
        for (RunMetrics.Usage usage : env.metrics().usage) {
            in += usage.inputTokens();
            out += usage.outputTokens();
            cost = cost.add(usage.cost());
        }
        totals.sort(null);
        ObjectNode summary = report.putObject("summary");
        summary.put("productMeetings", result.product().size()).put("architectureMeetings", pairs).put("architectureCompleted", completed);
        summary.put("inputTokens", in).put("outputTokens", out).put("estimatedCost", cost);
        summary.put("architectureLatencyP50Ms", totals.isEmpty() ? 0 : totals.get(totals.size() / 2));
        summary.put("architectureLatencyMaxMs", totals.isEmpty() ? 0 : totals.getLast());
        summary.put("scenarioLatencyMs", Duration.ofMillis(result.product().stream().mapToLong(r -> r.total().toMillis()).sum()
                + totals.stream().mapToLong(Long::longValue).sum()).toMillis());
        return report;
    }

    private static double ratio(int part, int whole) {
        return whole == 0 ? 0 : Math.round(1000.0 * part / whole) / 1000.0;
    }
}
