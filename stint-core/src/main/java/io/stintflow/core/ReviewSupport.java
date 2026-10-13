package io.stintflow.core;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.cloudevents.CloudEvent;
import io.stintflow.core.interpreter.Emitted;
import io.stintflow.core.model.ReviewSpec;
import io.stintflow.spi.CorrectionLog.Correction;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.spi.wire.StintEvents;
import io.stintflow.wire.Json;
import io.stintflow.wire.review.ReviewContract;

/**
 * The engine's side of SDD 3a.4: the review record kept in the instance's decisions under the review's pointer,
 * the review events an activation emits (their id, deadline and the request's fields come from the record, so every
 * phase agrees), the checks an answer passes before it may resume an instance, and what the flow and the correction
 * log get from it. Ids, declared names and pointers only.
 */
final class ReviewSupport {

    /** On a generated review {@code emit}: the review's pointer. Removed before the event leaves. */
    static final String REVIEW_MARK = "stintreview";
    /** On a generated review {@code emit}: {@code request}, {@code escalate} or {@code remind}. Removed too. */
    static final String PHASE_MARK = "stintreviewphase";
    static final String REQUEST = "request";
    static final String ESCALATE = "escalate";
    static final String REMIND = "remind";
    /** In a generated request's data: how long this phase waits. Replaced by the {@code deadline} instant. */
    static final String DEADLINE_SECONDS = "deadlineSeconds";

    private ReviewSupport() {
    }

    /** What {@link #prepare} made of an activation's emits: the records, the events, and what to audit. */
    record Prepared(JsonNode decisions, List<Emitted> emitted, List<ObjectNode> audits, String problem) {
    }

    /**
     * Completes the generated review events of an activation from the review's record, and updates the record: a
     * {@code request} opens a new review ({@code n} + 1), an {@code escalate} asks the next role, a {@code remind}
     * only notes it. Any other event passes untouched.
     */
    static Prepared prepare(String instanceId, WorkflowRef ref, JsonNode decisions, List<Emitted> emitted,
            Instant now) {
        ObjectNode records = decisions != null && decisions.isObject() ? ((ObjectNode) decisions).deepCopy()
                : Json.obj();
        List<Emitted> out = new ArrayList<>();
        List<ObjectNode> audits = new ArrayList<>();
        boolean changed = false;
        for (Emitted e : emitted) {
            JsonNode with = e.with();
            if (!with.hasNonNull(REVIEW_MARK)) {
                out.add(e);
                continue;
            }
            changed = true;
            ObjectNode event = ((ObjectNode) with).deepCopy();
            String pointer = event.remove(REVIEW_MARK).asText();
            String phase = event.path(PHASE_MARK).asText();
            event.remove(PHASE_MARK);
            ObjectNode data = event.with("data");
            ObjectNode record = records.get(pointer) instanceof ObjectNode r ? r : null;
            String role = event.path(StintEvents.EXT_ROLE).asText();
            long seconds = data.path(DEADLINE_SECONDS).asLong(0);
            data.remove(DEADLINE_SECONDS);
            if (REQUEST.equals(phase)) {
                JsonNode material = data.get("materialRef");
                if (!ReviewContract.isPointer(material)) {
                    return new Prepared(decisions, emitted, List.of(), "review at " + pointer
                            + ": materialRef must evaluate to a pointer (a URI with a scheme), never the material");
                }
                JsonNode items = data.get("items");
                if (items != null && !items.isNull() && !(items.isArray() && allText(items))) {
                    return new Prepared(decisions, emitted, List.of(), "review at " + pointer
                            + ": items must evaluate to a list of ids");
                }
                int n = record == null ? 1 : record.path("n").asInt(0) + 1;
                record = Json.obj();
                record.put("reviewId", instanceId + ":" + pointer + ":" + n);
                record.put("n", n);
                record.put("kind", data.path("kind").asText());
                record.put("role", role);
                record.putArray("roles").add(role);
                record.put("requestedAt", now.toString());
                record.set("options", data.path("options").deepCopy());
                record.set("items", items == null || items.isNull() ? Json.MAPPER.createArrayNode() : items);
                record.set("materialRef", material);
                if (data.hasNonNull("decisionPointer")) {
                    record.put("decisionPointer", data.get("decisionPointer").asText());
                }
                record.put("escalated", false);
                records.set(pointer, record);
            } else if (record == null) {
                return new Prepared(decisions, emitted, List.of(), "review at " + pointer + ": " + phase
                        + " without a request");
            } else if (ESCALATE.equals(phase)) {
                data.put("escalatedFrom", record.path("role").asText());
                record.put("role", role);
                ((ArrayNode) record.withArray("roles")).add(role);
                record.put("escalated", true);
                record.put("escalatedAt", now.toString());
            } else {
                record.put("remindedAt", now.toString());
                role = record.path("role").asText();
                event.put(StintEvents.EXT_ROLE, role);
            }
            String reviewId = record.path("reviewId").asText();
            if (seconds > 0) {
                record.put("deadline", now.plusSeconds(seconds).toString());
            }
            event.put("subject", reviewId);
            data.put("reviewId", reviewId);
            data.put("role", role);
            data.put("deadline", record.path("deadline").asText(null));
            if (!REMIND.equals(phase)) {
                data.put("kind", record.path("kind").asText());
                data.set("options", record.path("options").deepCopy());
                data.set("items", record.path("items").deepCopy());
                data.set("materialRef", record.path("materialRef").deepCopy());
                data.put("definition", ref.canonical());
                data.put("pointer", pointer);
                data.remove("decisionPointer");
            }
            out.add(new Emitted(e.pointer(), e.occurrence(), event));
            audits.add(audit(pointer, REQUEST.equals(phase) ? "requested" : ESCALATE.equals(phase) ? "escalated"
                    : "reminded", reviewId, role));
        }
        return new Prepared(changed ? records : decisions, out, audits, null);
    }

    /** The current id of the review at {@code pointer}, or {@code null} if it was never asked. */
    static String reviewId(JsonNode decisions, String pointer) {
        JsonNode record = decisions == null ? null : decisions.get(pointer);
        return record == null ? null : record.path("reviewId").asText(null);
    }

    /**
     * Why an arriving answer may not resume the instance — {@code untrusted-source}, {@code late} (a role asked
     * before an escalation), {@code wrong-role} or {@code out-of-contract} — or {@code null} when it may.
     */
    static String refusal(ReviewSpec spec, CloudEvent event, JsonNode record) {
        if (!spec.sources().contains(String.valueOf(event.getSource()))) {
            return "untrusted-source";
        }
        Object role = event.getExtension(StintEvents.EXT_ROLE);
        if (role == null || !spec.role().equals(role.toString())) {
            return role != null && spec.earlierRoles().contains(role.toString()) ? "late" : "wrong-role";
        }
        if (!ReviewContract.violations(data(event), spec.kind(), spec.options(), texts(record.path("items")))
                .isEmpty()) {
            return "out-of-contract";
        }
        return null;
    }

    /** What the flow continues with after a valid answer. */
    static ObjectNode result(ReviewSpec spec, CloudEvent event, JsonNode record, JsonNode decision, Instant now) {
        JsonNode data = data(event);
        String role = String.valueOf(event.getExtension(StintEvents.EXT_ROLE));
        String reviewer = data.path(ReviewContract.REVIEWER).asText();
        ObjectNode result = Json.obj();
        if (spec.decisionPointer() != null) {
            // the shape of a decision's result: the author's output.as reads it alike (8b)
            result.set("value", data.get(ReviewContract.VALUE));
            result.putNull("confidence");
            result.put("uncertain", false);
            result.set("provenance", decision == null ? Json.MAPPER.nullNode()
                    : decision.path("provenance").deepCopy());
            ObjectNode reviewed = result.putObject("reviewed");
            reviewed.put("reviewId", record.path("reviewId").asText());
            reviewed.put("role", role);
            reviewed.put("reviewer", reviewer);
            reviewed.put("reviewedAt", now.toString());
            reviewed.set("originalValue", decision == null ? Json.MAPPER.nullNode() : decision.path("value"));
            copyIfSet(data, reviewed, ReviewContract.NOTE_REF);
            return result;
        }
        if (ReviewContract.APPROVAL.equals(spec.kind())) {
            result.put("verdict", ReviewContract.verdictOf(data));
            ArrayNode rejected = result.putArray("rejectedItems");
            ReviewContract.rejectedItems(data).forEach(rejected::add);
            result.set("items", data.path(ReviewContract.ITEMS).isArray() ? data.get(ReviewContract.ITEMS)
                    : Json.MAPPER.createArrayNode());
        } else {
            result.set("value", data.get(ReviewContract.VALUE));
        }
        copyIfSet(data, result, ReviewContract.NOTE_REF);
        result.put("role", role);
        result.put("reviewer", reviewer);
        result.put("reviewedAt", now.toString());
        result.put("reviewId", record.path("reviewId").asText());
        return result;
    }

    /** The decisions with the review's record marked answered. */
    static JsonNode answered(JsonNode decisions, String pointer, ObjectNode human, String role, String reviewer,
            Instant now) {
        ObjectNode records = ((ObjectNode) decisions).deepCopy();
        ObjectNode record = (ObjectNode) records.get(pointer);
        record.set("answer", human);
        record.put("answeredBy", role);
        record.put("reviewer", reviewer);
        record.put("answeredAt", now.toString());
        return records;
    }

    /** The human part of an answer: the value, or the verdict and rejected items, and the note's pointer. */
    static ObjectNode human(ReviewSpec spec, CloudEvent event) {
        JsonNode data = data(event);
        ObjectNode human = Json.obj();
        if (ReviewContract.APPROVAL.equals(spec.kind())) {
            human.put("verdict", ReviewContract.verdictOf(data));
            ArrayNode rejected = human.putArray("rejectedItems");
            ReviewContract.rejectedItems(data).forEach(rejected::add);
        } else {
            human.set("value", data.get(ReviewContract.VALUE));
        }
        copyIfSet(data, human, ReviewContract.NOTE_REF);
        return human;
    }

    static Correction correction(WorkflowRef ref, String instanceId, ReviewSpec spec, JsonNode record,
            JsonNode decision, ObjectNode human, CloudEvent event, Sensitivity level, Instant now) {
        ObjectNode decided = null;
        if (decision != null) {
            decided = Json.obj();
            for (String field : List.of("value", "confidence", "uncertain", "provenance")) {
                decided.set(field, decision.path(field).deepCopy());
            }
        }
        return new Correction(ref, instanceId, spec.reviewPointer(), record.path("reviewId").asText(), spec.kind(),
                level, record.path("materialRef").asText(null), decided, human,
                String.valueOf(event.getExtension(StintEvents.EXT_ROLE)),
                data(event).path(ReviewContract.REVIEWER).asText(), Instant.parse(record.path("requestedAt").asText()),
                now, record.path("escalated").asBoolean(false));
    }

    static ObjectNode audit(String pointer, String outcome, String reviewId, String role) {
        ObjectNode audit = Json.obj();
        audit.put("pointer", pointer);
        audit.put("outcome", outcome);
        audit.put("reviewId", reviewId);
        audit.put("role", role);
        return audit;
    }

    static JsonNode data(CloudEvent event) {
        if (event.getData() == null) {
            return Json.MAPPER.nullNode();
        }
        try {
            return Json.read(event.getData().toBytes());
        } catch (RuntimeException notJson) {
            return Json.MAPPER.nullNode();
        }
    }

    private static List<String> texts(JsonNode array) {
        List<String> texts = new ArrayList<>();
        array.forEach(n -> texts.add(n.asText()));
        return texts;
    }

    private static boolean allText(JsonNode array) {
        for (JsonNode n : array) {
            if (!n.isTextual()) {
                return false;
            }
        }
        return true;
    }

    private static void copyIfSet(JsonNode from, ObjectNode to, String field) {
        if (from.hasNonNull(field)) {
            to.set(field, from.get(field));
        }
    }
}
