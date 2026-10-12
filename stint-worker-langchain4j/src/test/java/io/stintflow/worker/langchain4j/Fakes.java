package io.stintflow.worker.langchain4j;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import org.jboss.logmanager.ExtLogRecord;
import org.jboss.logmanager.LogContext;

import com.fasterxml.jackson.databind.JsonNode;

import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;
import dev.langchain4j.model.output.FinishReason;
import dev.langchain4j.model.output.TokenUsage;
import dev.langchain4j.service.SystemMessage;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;

/** Test doubles for SDD 3a.3: a scripted model, a clock that moves only when told, synthetic tasks, captured logs. */
final class Fakes {

    static final Instant T0 = Instant.parse("2026-10-12T12:00:00Z");

    private Fakes() {
    }

    interface Summarizer {
        Summary summarize(@dev.langchain4j.service.UserMessage String text);
    }

    record Summary(String topic, int items) {
    }

    interface Chat {
        String reply(@dev.langchain4j.service.UserMessage String text);
    }

    interface ChatWithSystem {
        @SystemMessage("You are a synthetic test assistant.")
        String reply(@dev.langchain4j.service.UserMessage String text);
    }

    /** A model that answers by a rule and remembers every request. Nothing goes over the network. */
    static final class ScriptedModel implements ChatModel {
        final List<ChatRequest> requests = new CopyOnWriteArrayList<>();
        private final Function<ChatRequest, ChatResponse> rule;

        ScriptedModel(Function<ChatRequest, ChatResponse> rule) {
            this.rule = rule;
        }

        static ScriptedModel replying(String text) {
            return new ScriptedModel(request -> answer(text));
        }

        /** Answers with how many messages it saw and the last user text, e.g. {@code 3:hello}. */
        static ScriptedModel echoing() {
            return new ScriptedModel(request -> answer(request.messages().size() + ":" + lastUserText(request)));
        }

        @Override
        public ChatResponse doChat(ChatRequest request) {
            requests.add(request);
            return rule.apply(request);
        }

        List<ChatMessage> lastMessages() {
            return requests.getLast().messages();
        }
    }

    static ChatResponse answer(String text) {
        return ChatResponse.builder().aiMessage(AiMessage.from(text)).modelName("scripted-model")
                .tokenUsage(new TokenUsage(11, 7)).finishReason(FinishReason.STOP).build();
    }

    static String lastUserText(ChatRequest request) {
        for (ChatMessage message : request.messages().reversed()) {
            if (message instanceof UserMessage user) {
                return user.singleText();
            }
        }
        return "";
    }

    /** A clock that moves only when the test (or a fake) says so. */
    static final class Clock implements InstantSource {
        final AtomicReference<Instant> now = new AtomicReference<>(T0);

        @Override
        public Instant instant() {
            return now.get();
        }

        void advance(Duration by) {
            now.updateAndGet(t -> t.plus(by));
        }
    }

    static TaskContext task(String instanceId, String taskId, String correlationId, int attempt, JsonNode input) {
        return new TaskContext(taskId, instanceId, correlationId, attempt, input, new WorkflowRef("test", "ai", "1.0.0"),
                "chn-" + instanceId, Lineage.NONE);
    }

    static TaskContext task(String instanceId, String taskId, String correlationId) {
        return task(instanceId, taskId, correlationId, 1, Json.obj().put("text", "synthetic"));
    }

    static JsonNode run(TaskHandler handler, TaskContext ctx) {
        return handler.execute(ctx).toCompletableFuture().join();
    }

    static TaskFailure failure(TaskHandler handler, TaskContext ctx) {
        try {
            handler.execute(ctx).toCompletableFuture().join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof TaskFailure failure) {
                return failure;
            }
            throw e;
        }
        throw new AssertionError("the task did not fail");
    }

    /**
     * Every record logged anywhere (the root logger of the JBoss log context the SLF4J binding writes to), rendered
     * with its parameters, its MDC and its exception — everything a JSON log line would carry.
     */
    static final class Logs extends Handler implements AutoCloseable {
        final List<String> lines = new CopyOnWriteArrayList<>();
        final BlockingQueue<String> arrived = new LinkedBlockingQueue<>();
        private final org.jboss.logmanager.Logger root = LogContext.getLogContext().getLogger("");
        private final Level previous;

        Logs() {
            previous = root.getLevel();
            root.setLevel(Level.ALL);
            setLevel(Level.ALL);
            root.addHandler(this);
        }

        @Override
        public void publish(LogRecord record) {
            String message = record instanceof ExtLogRecord ext ? ext.getFormattedMessage() : record.getMessage();
            StringBuilder line = new StringBuilder(record.getLevel() + " " + record.getLoggerName() + " " + message);
            if (record instanceof ExtLogRecord ext) {
                line.append(" mdc=").append(ext.getMdcCopy());
            }
            for (Throwable t = record.getThrown(); t != null; t = t.getCause()) {
                line.append(" thrown=").append(t);
            }
            lines.add(line.toString());
            arrived.add(line.toString());
        }

        String joined() {
            return String.join("\n", lines);
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
            root.removeHandler(this);
            root.setLevel(previous);
        }
    }
}
