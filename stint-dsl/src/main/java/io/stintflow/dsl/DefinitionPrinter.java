package io.stintflow.dsl;

import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.CallRemoteNode;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.model.DoNode;
import io.stintflow.core.model.EmitNode;
import io.stintflow.core.model.ForRunNode;
import io.stintflow.core.model.FlowDirective;
import io.stintflow.core.model.ListenNode;
import io.stintflow.core.model.SetNode;
import io.stintflow.core.model.SwitchNode;
import io.stintflow.core.model.TaskNode;
import io.stintflow.core.model.TryNode;

/**
 * SDD 3a.1, RF8: prints a compiled definition back as DSL 1.0 YAML (with the Stint extension {@code call: remote}),
 * so what a {@code call: decision} expands to can be read while debugging. Expressions print as {@code ${ jq }};
 * a {@code listen} prints its strategy and event types only. The output is for reading, not for loading again:
 * an output contract (a decision's) shows as a comment-like {@code x-stint-output-contract} entry.
 */
public final class DefinitionPrinter {

    private static final YAMLMapper YAML = YAMLMapper.builder()
            .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
            .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES)
            .disable(YAMLGenerator.Feature.SPLIT_LINES)
            .build();

    private DefinitionPrinter() {
    }

    public static String print(WorkflowDefinition definition) {
        ObjectNode root = YAML.createObjectNode();
        ObjectNode document = root.putObject("document");
        document.put("dsl", "1.0.0");
        document.put("namespace", definition.ref().namespace());
        document.put("name", definition.ref().name());
        document.put("version", definition.ref().version());
        root.set("do", tasks(definition.root().tasks()));
        return write(root);
    }

    /** {@code tasks} as a {@code do} list. */
    public static String print(List<TaskNode> tasks) {
        return write(tasks(tasks));
    }

    private static ArrayNode tasks(List<TaskNode> tasks) {
        ArrayNode list = YAML.createArrayNode();
        for (TaskNode task : tasks) {
            list.addObject().set(task.name(), task(task));
        }
        return list;
    }

    private static ObjectNode task(TaskNode node) {
        ObjectNode body = YAML.createObjectNode();
        switch (node) {
            case CallRemoteNode call -> {
                body.put("call", "remote");
                ObjectNode with = body.putObject("with");
                with.put("task", call.routingKey());
                if (call.withInput() != null) {
                    with.put("input", expr(call.withInput()));
                }
                if (call.timeout() != null) {
                    body.putObject("timeout").put("after", duration(call.timeout()));
                }
                if (call.outputContract() != null) {
                    body.put("x-stint-output-contract", call.outputContract().title());
                }
            }
            case SwitchNode sw -> {
                ArrayNode cases = body.putArray("switch");
                for (SwitchNode.Case c : sw.cases()) {
                    ObjectNode caseBody = cases.addObject().putObject(c.name());
                    if (c.when() != null) {
                        caseBody.put("when", expr(c.when()));
                    }
                    caseBody.put("then", directive(c.then()));
                }
            }
            case SetNode set -> body.put("set", set.set() == null ? "${ . }" : expr(set.set()));
            case TryNode tryNode -> {
                ArrayNode tryList = body.putArray("try");
                tryList.addObject().set(tryNode.task().name(), task(tryNode.task()));
                ObjectNode catchBody = body.putObject("catch");
                TryNode.Catch c = tryNode.catchClause();
                if (c.errorFilter() != null) {
                    catchBody.put("x-stint-errors-filter", expr(c.errorFilter()));
                }
                if (c.when() != null) {
                    catchBody.put("when", expr(c.when()));
                }
                if (c.exceptWhen() != null) {
                    catchBody.put("exceptWhen", expr(c.exceptWhen()));
                }
                if (c.retry() != null) {
                    ObjectNode retry = catchBody.putObject("retry");
                    retry.put("delay", duration(c.retry().delay()));
                    retry.putObject("backoff").putObject(c.retry().backoff().name().toLowerCase());
                    retry.putObject("limit").putObject("attempt").put("count", c.retry().maxAttempts());
                }
                if (c.then() != null) {
                    catchBody.put("then", directive(c.then()));
                }
            }
            case EmitNode emit -> body.putObject("emit").putObject("event").put("with", expr(emit.event()));
            case ListenNode listen -> {
                ObjectNode to = body.putObject("listen").putObject("to");
                ArrayNode filters = to.putArray(listen.strategy().name().toLowerCase());
                for (ListenNode.Filter filter : listen.filters()) {
                    filters.addObject().putObject("with").put("type", filter.type());
                }
            }
            case ForRunNode forRun -> {
                ObjectNode spec = body.putObject("for");
                spec.put("each", forRun.each());
                if (forRun.at() != null) {
                    spec.put("at", forRun.at());
                }
                spec.put("in", expr(forRun.in()));
                ObjectNode workflow = body.putArray("do").addObject().putObject("run").putObject("workflow");
                workflow.put("namespace", forRun.run().namespace());
                workflow.put("name", forRun.run().name());
                workflow.put("version", forRun.run().version());
                if (forRun.run().input() != null) {
                    workflow.put("input", expr(forRun.run().input()));
                }
                ObjectNode metadata = body.putObject("metadata");
                metadata.put("x-stint-concurrency", forRun.concurrency());
                metadata.put("x-stint-join", forRun.join().name().toLowerCase());
                metadata.put("x-stint-max-iterations", forRun.maxIterations());
            }
            case DoNode doNode -> body.set("do", tasks(doNode.tasks()));
        }
        dataFlow(body, node.dataFlow());
        if (!(node instanceof SwitchNode) && !(node instanceof DoNode) && node.then() != FlowDirective.CONTINUE) {
            body.put("then", directive(node.then()));
        }
        return body;
    }

    private static void dataFlow(ObjectNode body, DataFlow flow) {
        if (flow.inputFrom() != null) {
            body.putObject("input").put("from", expr(flow.inputFrom()));
        }
        if (flow.outputAs() != null) {
            body.putObject("output").put("as", expr(flow.outputAs()));
        }
        if (flow.exportAs() != null) {
            body.putObject("export").put("as", expr(flow.exportAs()));
        }
    }

    private static String expr(Expr expr) {
        return expr instanceof Expr.Jq jq ? "${ " + jq.source() + " }" : "<java lambda>";
    }

    private static String directive(FlowDirective directive) {
        return switch (directive) {
            case FlowDirective.Continue c -> "continue";
            case FlowDirective.End e -> "end";
            case FlowDirective.Exit e -> "exit";
            case FlowDirective.GoTo go -> go.taskName();
        };
    }

    private static String duration(Duration duration) {
        return duration.toString();
    }

    private static String write(JsonNode node) {
        try {
            return YAML.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }
}
