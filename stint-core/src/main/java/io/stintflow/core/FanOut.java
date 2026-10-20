package io.stintflow.core;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.core.model.ForRunNode;
import io.stintflow.wire.Json;

/**
 * SDD 3a.8: the bookkeeping of a {@code for} over {@code run: workflow}, kept in the parent's
 * {@code InstanceSnapshot.composition} under {@code for}, and the link a child keeps to its parent under {@code parent}.
 * <pre>
 * { "parent": { "id", "pointer", "index" },                       // on a child, for its whole life
 *   "for":    { "pointer", "total", "concurrency", "join", "started", "settled",
 *               "children": [ { "id", "status", "input"?, "output"?, "error"? }, ... ] } }   // on the parent, while the for runs
 * </pre>
 * The join logic is here, pure, over the JSON: {@link #open} and {@link #settle} change the state they are given and say
 * what the engine must do — start children, cancel the others, finish the {@code for} with an output, or fail it.
 */
final class FanOut {

    static final String PENDING = "PENDING";
    static final String RUNNING = "RUNNING";
    static final String COMPLETED = "COMPLETED";
    static final String FAILED = "FAILED";
    static final String CANCELLED = "CANCELLED";

    static final String PARENT = "parent";
    static final String FOR = "for";
    private static final String CHILD_WAIT_PREFIX = "child:";

    private FanOut() {
    }

    enum Action {
        /** Nothing to decide yet: wait for the running children (after starting {@link Step#start}). */
        WAIT,
        /** The {@code for} is done: {@link Step#output} is its output. */
        COMPLETE,
        /** The {@code for} fails with {@link Step#message}. */
        FAIL
    }

    /** What to do after a change: children to start (index, id, input), children to cancel (ids), and the verdict. */
    record Step(Action action, JsonNode output, String message, List<Start> start, List<String> cancel) {
    }

    record Start(int index, String id, JsonNode input) {
    }

    // --- naming and links ----------------------------------------------------------------------------------------

    /** The deterministic id of the child for item {@code index}: a redelivered start finds the same instance. */
    static String childId(String parentId, String pointer, int index) {
        return parentId + ":" + pointer + ":" + index;
    }

    static String childWait(String childId) {
        return CHILD_WAIT_PREFIX + childId;
    }

    static boolean isChildWait(String waitKey) {
        return waitKey != null && waitKey.startsWith(CHILD_WAIT_PREFIX);
    }

    static String childIdOf(String waitKey) {
        return waitKey.substring(CHILD_WAIT_PREFIX.length());
    }

    static ObjectNode parentOf(JsonNode composition) {
        return composition != null && composition.get(PARENT) instanceof ObjectNode parent ? parent : null;
    }

    static ObjectNode forOf(JsonNode composition) {
        return composition != null && composition.get(FOR) instanceof ObjectNode state ? state : null;
    }

    static JsonNode withParent(String parentId, String pointer, int index) {
        ObjectNode composition = Json.obj();
        composition.putObject(PARENT).put("id", parentId).put("pointer", pointer).put("index", index);
        return composition;
    }

    static JsonNode withFor(JsonNode composition, ObjectNode state) {
        ObjectNode out = composition == null ? Json.obj() : (ObjectNode) composition.deepCopy();
        out.set(FOR, state);
        return out;
    }

    /** The composition once the {@code for} is over: the parent link only, or {@code null} when there is none. */
    static JsonNode withoutFor(JsonNode composition) {
        if (composition == null) {
            return null;
        }
        ObjectNode out = (ObjectNode) composition.deepCopy();
        out.remove(FOR);
        return out.isEmpty() ? null : out;
    }

    /** The wait keys of the children still running, in order. */
    static List<String> runningWaits(JsonNode composition) {
        ObjectNode state = forOf(composition);
        List<String> keys = new ArrayList<>();
        if (state != null) {
            for (JsonNode child : state.get("children")) {
                if (RUNNING.equals(child.path("status").asText())) {
                    keys.add(childWait(child.path("id").asText()));
                }
            }
        }
        return keys;
    }

    /** The ids of the children still running. */
    static List<String> runningIds(JsonNode composition) {
        return runningWaits(composition).stream().map(FanOut::childIdOf).toList();
    }

    // --- the join ------------------------------------------------------------------------------------------------

    /** The state of a {@code for} that has not started a child: every child {@code PENDING}, holding its input. */
    static ObjectNode initial(ForRunNode node, String parentId, List<JsonNode> inputs) {
        ObjectNode state = Json.obj();
        state.put("pointer", node.pointer());
        state.put("total", inputs.size());
        state.put("concurrency", node.concurrency());
        state.put("join", node.join().name());
        state.put("started", 0);
        state.put("settled", 0);
        ArrayNode children = state.putArray("children");
        for (int i = 0; i < inputs.size(); i++) {
            ObjectNode child = children.addObject();
            child.put("id", childId(parentId, node.pointer(), i));
            child.put("status", PENDING);
            child.set("input", inputs.get(i));
        }
        return state;
    }

    /** Starts the first window. A {@code for} over no items completes (or, for {@code any}, fails) at once. */
    static Step open(ObjectNode state) {
        if (state.path("total").asInt() == 0) {
            return "ANY".equals(state.path("join").asText())
                    ? new Step(Action.FAIL, null, "for ... any over no items has no child to succeed", List.of(), List.of())
                    : new Step(Action.COMPLETE, Json.MAPPER.createArrayNode(), null, List.of(), List.of());
        }
        return new Step(Action.WAIT, null, null, startWindow(state), List.of());
    }

    /**
     * Records the end of child {@code index} ({@code status} COMPLETED, FAILED or CANCELLED) and decides what follows,
     * by the join: {@code all} fails on the first failure and cancels the rest; {@code any} ends on the first success
     * (and fails only if every child failed); {@code collect} waits for all. Running children are never started past
     * the concurrency, and a decided {@code for} starts nothing more.
     */
    static Step settle(ObjectNode state, int index, String status, JsonNode output, String error) {
        ArrayNode children = (ArrayNode) state.get("children");
        ObjectNode child = (ObjectNode) children.get(index);
        if (!RUNNING.equals(child.path("status").asText())) {
            return new Step(Action.WAIT, null, null, List.of(), List.of());   // already settled: a redelivery
        }
        child.put("status", status);
        if (output != null) {
            child.set("output", output);
        }
        if (error != null) {
            child.put("error", error);
        }
        state.put("settled", state.path("settled").asInt() + 1);
        String join = state.path("join").asText();
        boolean failed = !COMPLETED.equals(status);
        int total = state.path("total").asInt();
        boolean allSettled = state.path("settled").asInt() == total;

        switch (join) {
            case "ALL" -> {
                if (failed) {
                    return new Step(Action.FAIL, null, "child " + index + " failed: " + describe(child),
                            List.of(), cancelRunning(state));
                }
                if (allSettled) {
                    ArrayNode outputs = Json.MAPPER.createArrayNode();
                    children.forEach(c -> outputs.add(c.has("output") ? c.get("output") : Json.MAPPER.nullNode()));
                    return new Step(Action.COMPLETE, outputs, null, List.of(), List.of());
                }
            }
            case "ANY" -> {
                if (!failed) {
                    ObjectNode winner = Json.obj();
                    winner.put("index", index);
                    winner.set("output", child.has("output") ? child.get("output") : Json.MAPPER.nullNode());
                    return new Step(Action.COMPLETE, winner, null, List.of(), cancelRunning(state));
                }
                if (allSettled) {
                    return new Step(Action.FAIL, null, "all " + total + " children failed; the last: "
                            + describe(child), List.of(), List.of());
                }
            }
            default -> {
                if (allSettled) {
                    ArrayNode results = Json.MAPPER.createArrayNode();
                    for (int i = 0; i < children.size(); i++) {
                        ObjectNode c = (ObjectNode) children.get(i);
                        ObjectNode entry = results.addObject();
                        entry.put("index", i);
                        entry.put("status", c.path("status").asText());
                        if (c.has("output")) {
                            entry.set("output", c.get("output"));
                        }
                        if (c.has("error")) {
                            entry.put("error", c.get("error").asText());
                        }
                    }
                    return new Step(Action.COMPLETE, results, null, List.of(), List.of());
                }
            }
        }
        return new Step(Action.WAIT, null, null, startWindow(state), List.of());
    }

    /** Marks the children to start now RUNNING (dropping their stored input) and returns them. */
    private static List<Start> startWindow(ObjectNode state) {
        ArrayNode children = (ArrayNode) state.get("children");
        int total = state.path("total").asInt();
        int concurrency = state.path("concurrency").asInt();
        int running = 0;
        for (JsonNode child : children) {
            if (RUNNING.equals(child.path("status").asText())) {
                running++;
            }
        }
        List<Start> starts = new ArrayList<>();
        int next = state.path("started").asInt();
        while (running < concurrency && next < total) {
            ObjectNode child = (ObjectNode) children.get(next);
            starts.add(new Start(next, child.path("id").asText(), child.get("input")));
            child.put("status", RUNNING);
            child.remove("input");
            running++;
            next++;
        }
        state.put("started", next);
        return starts;
    }

    /** Marks the running children CANCELLED and returns their ids (the engine cancels them durably). */
    private static List<String> cancelRunning(ObjectNode state) {
        List<String> ids = new ArrayList<>();
        for (JsonNode child : state.get("children")) {
            if (RUNNING.equals(child.path("status").asText())) {
                ids.add(child.path("id").asText());
                ((ObjectNode) child).put("status", CANCELLED);
            }
        }
        return ids;
    }

    private static String describe(JsonNode child) {
        return child.path("status").asText() + (child.hasNonNull("error") ? " (" + child.get("error").asText() + ")" : "");
    }
}
