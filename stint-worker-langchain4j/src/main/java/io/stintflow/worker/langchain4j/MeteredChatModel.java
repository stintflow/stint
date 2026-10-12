package io.stintflow.worker.langchain4j;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.Set;

import dev.langchain4j.model.ModelProvider;
import dev.langchain4j.model.chat.Capability;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.listener.ChatModelListener;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.request.ChatRequestParameters;
import dev.langchain4j.model.chat.response.ChatResponse;
import dev.langchain4j.model.output.TokenUsage;
import io.stintflow.spi.ExecutionSpan;
import io.stintflow.spi.ExecutionTracer;
import io.stintflow.worker.TaskContext;

/**
 * The worker's {@link ChatModel}, measured for one execution (SDD 3a.3, 5.6): each call is a {@code stint.llm} span,
 * child of the task's span, with the task's identifiers only; calls, tokens, latency and the model's name are counted
 * for the {@link LlmUsage}. Nothing of the request or the response is recorded — a failure is recorded on the span by
 * its exception's class, never its message, which may quote the prompt.
 */
final class MeteredChatModel implements ChatModel {

    static final String SPAN_NAME = "stint.llm";

    private final ChatModel delegate;
    private final ExecutionTracer tracer;
    private final TaskContext task;
    private final InstantSource clock;

    private int calls;
    private long inputTokens;
    private long outputTokens;
    private Duration latency = Duration.ZERO;
    private String model;
    private String finishReason;

    MeteredChatModel(ChatModel delegate, ExecutionTracer tracer, TaskContext task, InstantSource clock) {
        this.delegate = delegate;
        this.tracer = tracer;
        this.task = task;
        this.clock = clock;
    }

    @Override
    public ChatResponse chat(ChatRequest request) {
        ExecutionSpan span = tracer.start(SPAN_NAME, TaskIdentifiers.of(task),
                task.lineage() != null && task.lineage().hasTrace() ? task.lineage() : null, List.of());
        Instant start = clock.instant();
        try {
            ChatResponse response = delegate.chat(request);
            record(response);
            return response;
        } catch (RuntimeException e) {
            span.recordFailure(new IllegalStateException(e.getClass().getName()));
            throw e;
        } finally {
            synchronized (this) {
                calls++;
                latency = latency.plus(Duration.between(start, clock.instant()));
            }
            span.close();
        }
    }

    private synchronized void record(ChatResponse response) {
        TokenUsage usage = response.tokenUsage();
        if (usage != null) {
            inputTokens += usage.inputTokenCount() == null ? 0 : usage.inputTokenCount();
            outputTokens += usage.outputTokenCount() == null ? 0 : usage.outputTokenCount();
        }
        if (response.modelName() != null) {
            model = response.modelName();
        }
        finishReason = response.finishReason() == null ? null : response.finishReason().name();
    }

    synchronized LlmUsage usage(int toolCalls) {
        return new LlmUsage(model, calls, inputTokens, outputTokens, toolCalls, latency, finishReason);
    }

    @Override
    public ChatRequestParameters defaultRequestParameters() {
        return delegate.defaultRequestParameters();
    }

    /** The delegate's own listeners run inside its {@code chat}; this wrapper has none of its own. */
    @Override
    public List<ChatModelListener> listeners() {
        return List.of();
    }

    @Override
    public ModelProvider provider() {
        return delegate.provider();
    }

    @Override
    public Set<Capability> supportedCapabilities() {
        return delegate.supportedCapabilities();
    }
}
