package io.stintflow.worker.langchain4j;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;

import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.ChatMessageDeserializer;
import dev.langchain4j.data.message.ChatMessageSerializer;
import dev.langchain4j.memory.chat.MessageWindowChatMemory;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.service.AiServices;
import dev.langchain4j.service.Result;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.ErrorInfo.StandardType;
import io.stintflow.spi.ExecutionTracer;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;
import io.stintflow.worker.langchain4j.memory.BlobOffloadingConversationStore;
import io.stintflow.worker.langchain4j.memory.CommitOutcome;
import io.stintflow.worker.langchain4j.memory.ConversationStore;
import io.stintflow.worker.langchain4j.memory.Exchange;
import io.stintflow.worker.langchain4j.memory.MemoryKey;

/**
 * Builds {@link TaskHandler}s that run LLM work with LangChain4j AI Services (SDD 3a.3). A handler is an ordinary
 * worker task: the engine dispatches it, retries it and routes its errors like any other, and never sees a model.
 * <pre>{@code
 * LangChain4jWorker worker = LangChain4jWorker.builder()
 *         .chatModel(model)                    // the provider, built by the worker
 *         .conversations(store, blobs)         // optional: per-instance memory
 *         .build();
 * registry.register("summarize-meeting", worker.handler(Summarizer.class,
 *         (ai, task) -> ai.summarize(task.input().path("text").asText())));
 * }</pre>
 * Each execution:
 * <ol>
 *   <li>loads the conversation committed for its {@link MemoryKey} — and if an exchange of this very correlationId is
 *       among it (an at-least-once redelivery), returns that exchange's output without calling the model;</li>
 *   <li>builds the AI Service over a memory that keeps this execution's messages apart from the committed ones;</li>
 *   <li>runs the author's {@link AiCall}; a failure becomes a standard error ({@link ProviderErrors}) and commits
 *       nothing;</li>
 *   <li>checks the output ({@link OutputValidator}) and commits the exchange in one act — the first commit of a
 *       correlationId wins, and a losing concurrent delivery returns the winner's output;</li>
 *   <li>reports its {@link LlmUsage} — never content, unless the {@link ContentPolicy} names a sink.</li>
 * </ol>
 * The AI Service runs synchronously on the thread that executes the handler: give the worker enough threads
 * ({@code WorkerPool} uses virtual threads).
 */
public final class LangChain4jWorker {

    private static final Logger LOG = LoggerFactory.getLogger(LangChain4jWorker.class);
    /** SDD 3a.3b, 8b: a clock this far behind the conversation is worth a warning — workers' clocks disagree. */
    static final Duration CLOCK_LAG_WARNING = Duration.ofSeconds(1);
    /** SDD 3a.3c, 8b: the default {@link Builder#commitMargin}. */
    static final Duration DEFAULT_COMMIT_MARGIN = Duration.ofSeconds(5);

    private final ChatModel chatModel;
    private final ConversationStore conversations;
    private final int memoryWindow;
    private final Duration retention;
    private final UsageSink usage;
    private final ContentPolicy contentPolicy;
    private final ExecutionTracer tracer;
    private final InstantSource clock;
    private final Duration toolCallLimit;
    private final Function<Throwable, Optional<Duration>> retryAfter;
    private final Duration rateLimitRetryAfter;
    private final Duration commitMargin;

    private LangChain4jWorker(Builder b) {
        this.chatModel = Objects.requireNonNull(b.chatModel, "chatModel is required");
        this.conversations = b.conversations;
        this.memoryWindow = b.memoryWindow;
        this.retention = b.retention;
        this.usage = b.usage;
        this.contentPolicy = b.contentPolicy;
        this.tracer = b.tracer;
        this.clock = b.clock;
        this.toolCallLimit = b.toolCallLimit;
        this.retryAfter = b.retryAfter;
        this.rateLimitRetryAfter = b.rateLimitRetryAfter;
        this.commitMargin = b.commitMargin;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** A handler over {@code service}, with the instance's conversation as memory (when the worker has a store). */
    public <S> TaskHandler handler(Class<S> service, AiCall<S> call) {
        return service(service).handler(call);
    }

    /** Starts a handler with more options than {@link #handler(Class, AiCall)}. */
    public <S> HandlerBuilder<S> service(Class<S> service) {
        return new HandlerBuilder<>(service);
    }

    /** Deletes everything an instance remembers, in every scope, blobs included (the erasure SDD 3b.5 needs). */
    public CompletionStage<Void> forget(String instanceId) {
        if (conversations == null) {
            return CompletableFuture.completedFuture(null);
        }
        return conversations.deleteInstance(instanceId);
    }

    /** The options of one handler. */
    public final class HandlerBuilder<S> {
        private final Class<S> service;
        private MemoryScope scope = MemoryScope.INSTANCE;
        private Consumer<AiServices<S>> configure = b -> { };
        private OutputValidator validator = OutputValidator.NONE;

        private HandlerBuilder(Class<S> service) {
            this.service = Objects.requireNonNull(service, "service");
        }

        /** Which conversation the executions share; {@link MemoryScope#INSTANCE} by default. */
        public HandlerBuilder<S> memory(MemoryScope scope) {
            this.scope = Objects.requireNonNull(scope, "scope");
            return this;
        }

        /**
         * Further AI Service configuration: tools, a system message provider, and so on. The chat model and the memory
         * are the module's; setting them here replaces what the module measures and commits.
         */
        public HandlerBuilder<S> configure(Consumer<AiServices<S>> configure) {
            this.configure = Objects.requireNonNull(configure, "configure");
            return this;
        }

        /** Checks the output beyond its type; a violation fails the task with a {@code validation} error. */
        public HandlerBuilder<S> validate(OutputValidator validator) {
            this.validator = Objects.requireNonNull(validator, "validator");
            return this;
        }

        public TaskHandler handler(AiCall<S> call) {
            Objects.requireNonNull(call, "call");
            MemoryScope memory = conversations == null ? MemoryScope.NONE : scope;
            return ctx -> execute(ctx, service, memory, configure, validator, call);
        }
    }

    private <S> CompletionStage<JsonNode> execute(TaskContext ctx, Class<S> service, MemoryScope scope,
            Consumer<AiServices<S>> configure, OutputValidator validator, AiCall<S> call) {
        try (TaskIdentifiers ids = TaskIdentifiers.inMdc(ctx)) {
            return CompletableFuture.completedFuture(run(ctx, service, scope, configure, validator, call));
        } catch (TaskFailure failure) {
            return CompletableFuture.failedFuture(failure);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(failure(ctx, e));
        }
    }

    private TaskFailure failure(TaskContext ctx, Throwable thrown) {
        TaskFailure failure = ProviderErrors.toTaskFailure(thrown, retryAfter, rateLimitRetryAfter);
        if (ProviderErrors.unrecognized(failure)) {
            LOG.warn("Task {} of instance {} failed with an error that is not a known provider error: {} (its message "
                    + "is not logged: it may quote the prompt)", ctx.taskId(), ctx.instanceId(),
                    ProviderErrors.describe(thrown));
        }
        return failure;
    }

    private <S> JsonNode run(TaskContext ctx, Class<S> service, MemoryScope scope, Consumer<AiServices<S>> configure,
            OutputValidator validator, AiCall<S> call) {
        MemoryKey key = switch (scope) {
            case INSTANCE -> MemoryKey.instance(ctx.instanceId());
            case TASK -> MemoryKey.task(ctx.instanceId(), ctx.taskId());
            case NONE -> null;
        };
        List<Exchange> history = key == null ? List.of() : join(conversations.committed(key));
        Optional<JsonNode> done = outputOf(history, ctx.correlationId());
        if (done.isPresent()) {
            LOG.info("Task {} of instance {} was delivered again (correlation {}): returning its committed output "
                    + "without calling the model", ctx.taskId(), ctx.instanceId(), ctx.correlationId());
            return done.get();
        }
        Instant deadline = ctx.deadline();
        if (deadline != null && !clock.instant().isBefore(deadline)) {
            LOG.warn("Task {} of instance {} (correlation {}) arrived {} ms past its deadline: the engine no longer "
                    + "waits for it, so the model is not called", ctx.taskId(), ctx.instanceId(), ctx.correlationId(),
                    Duration.between(deadline, clock.instant()).toMillis());
            throw new TaskFailure(StandardType.TIMEOUT.uri(), 408,
                    "The task's deadline passed before its execution started", null);
        }

        ExecutionMemoryStore memory = new ExecutionMemoryStore(
                messagesOf(withoutSupersededAttempts(history, ctx.taskId(), ctx.attempt())));
        MeteredChatModel model = new MeteredChatModel(chatModel, tracer, ctx, clock);
        ToolCallTimer tools = new ToolCallTimer(clock, toolCallLimit);
        AiServices<S> builder = AiServices.builder(service).chatModel(model)
                .beforeToolExecution(tools::before)
                .afterToolExecution(tools::after);
        if (key != null) {
            // System messages are never committed, so the memory adds the service's on every execution: first.
            builder.chatMemory(MessageWindowChatMemory.builder().id(key).maxMessages(memoryWindow)
                    .alwaysKeepSystemMessageFirst(true).chatMemoryStore(memory).build());
        }
        configure.accept(builder);
        S ai = builder.build();

        JsonNode output;
        try {
            output = toJson(call.call(ai, new AiTaskContext(ctx, key)));
        } catch (Exception e) {
            throw failure(ctx, e);
        } finally {
            usage.record(ctx, model.usage(tools.calls()));
        }

        List<String> violations = validator.violations(output);
        if (!violations.isEmpty()) {
            throw ProviderErrors.validation("The LLM output is out of its contract: " + String.join("; ", violations));
        }

        JsonNode messages = Json.read(ChatMessageSerializer.messagesToJson(memory.added())
                .getBytes(StandardCharsets.UTF_8));
        if (key != null) {
            if (deadline != null && clock.instant().isAfter(deadline.minus(commitMargin))) {
                LOG.warn("Task {} of instance {} (correlation {}) ended {} ms before its deadline, inside the commit "
                        + "margin of {} ms: the engine may discard its result, so its exchange is not committed",
                        ctx.taskId(), ctx.instanceId(), ctx.correlationId(),
                        Duration.between(clock.instant(), deadline).toMillis(), commitMargin.toMillis());
                throw new TaskFailure(StandardType.TIMEOUT.uri(), 408,
                        "The execution ended past its deadline; its exchange was not committed", null);
            }
            Instant now = commitTime(ctx, history);
            Exchange exchange = new Exchange(key, ctx.correlationId(), ctx.taskId(), ctx.attempt(), now, messages,
                    output, now.plus(retention));
            if (join(conversations.commit(exchange)) == CommitOutcome.ALREADY_COMMITTED) {
                LOG.info("Task {} of instance {} was committed by a concurrent delivery (correlation {}): returning "
                        + "that delivery's output", ctx.taskId(), ctx.instanceId(), ctx.correlationId());
                output = outputOf(join(conversations.committed(key)), ctx.correlationId()).orElseThrow(() ->
                        new IllegalStateException("Exchange " + ctx.correlationId() + " is committed but not readable"));
            }
        }
        JsonNode recorded = output;
        contentPolicy.sinkFor(ctx).ifPresent(sink -> sink.record(ctx, messages, recorded));
        return output;
    }

    /**
     * SDD 3a.3b: the clock, but strictly after every exchange this execution read — one millisecond, the resolution a
     * store may keep — so the memory's order (committedAt, then correlationId) is the causal one even when the clock
     * ties (a fast step, a fixed test clock) or lags (another worker's clock). A lag over {@link #CLOCK_LAG_WARNING} is
     * logged with the ids only.
     */
    private Instant commitTime(TaskContext ctx, List<Exchange> history) {
        Instant now = clock.instant();
        Instant last = history.stream().map(Exchange::committedAt).max(Instant::compareTo).orElse(null);
        if (last == null || !now.isBefore(last.plusMillis(1))) {
            return now;
        }
        Duration lag = Duration.between(now, last);
        if (lag.compareTo(CLOCK_LAG_WARNING) > 0) {
            LOG.warn("Task {} of instance {}: the worker's clock is {} ms behind the conversation it continues; "
                    + "its exchange is committed right after it", ctx.taskId(), ctx.instanceId(), lag.toMillis());
        }
        return last.plusMillis(1);
    }

    /**
     * SDD 3a.3c, 4.4: the history without the exchanges of attempts a later attempt of the same pass through the same
     * task superseded — the engine applied at most the later one. A pass ends where the task's attempt starts again
     * at 1. The execution reading the history ({@code taskId}, {@code attempt}) counts as the latest exchange: a retry
     * doesn't read the attempts it replaces. Superseded exchanges stay stored (and are deleted with the instance); the
     * model just never sees them.
     */
    static List<Exchange> withoutSupersededAttempts(List<Exchange> history, String taskId, int attempt) {
        List<Exchange> kept = new ArrayList<>(history.size());
        for (int i = 0; i < history.size(); i++) {
            Exchange exchange = history.get(i);
            Boolean superseded = null;
            for (int j = i + 1; j < history.size() && superseded == null; j++) {
                superseded = supersedes(exchange, history.get(j).taskId(), history.get(j).attempt());
            }
            if (superseded == null) {
                superseded = supersedes(exchange, taskId, attempt);   // the reader itself, last
            }
            if (!Boolean.TRUE.equals(superseded)) {
                kept.add(exchange);
            }
        }
        return kept;
    }

    /** Whether a later {@code taskId}/{@code attempt} decides about {@code exchange}: true, false, or null to go on. */
    private static Boolean supersedes(Exchange exchange, String taskId, int attempt) {
        if (taskId == null || !taskId.equals(exchange.taskId())) {
            return null;
        }
        if (attempt == 1) {
            return false;   // a new pass through the task
        }
        return attempt > exchange.attempt() ? Boolean.TRUE : null;
    }

    private static Optional<JsonNode> outputOf(List<Exchange> history, String correlationId) {
        return history.stream().filter(e -> e.correlationId().equals(correlationId)).findFirst()
                .map(e -> e.output() == null ? NullNode.getInstance() : e.output());
    }

    private static List<ChatMessage> messagesOf(List<Exchange> history) {
        List<ChatMessage> messages = new ArrayList<>();
        for (Exchange exchange : history) {
            messages.addAll(ChatMessageDeserializer.messagesFromJson(exchange.messages().toString()));
        }
        return messages;
    }

    private static JsonNode toJson(Object result) {
        Object content = result instanceof Result<?> r ? r.content() : result;
        if (content == null) {
            return NullNode.getInstance();
        }
        if (content instanceof JsonNode node) {
            return node;
        }
        if (content instanceof String text) {
            return Json.MAPPER.getNodeFactory().textNode(text);
        }
        return Json.MAPPER.valueToTree(content);
    }

    private static <T> T join(CompletionStage<T> stage) {
        try {
            return stage.toCompletableFuture().join();
        } catch (CompletionException e) {
            throw e.getCause() instanceof RuntimeException r ? r : e;
        }
    }

    public static final class Builder {
        private ChatModel chatModel;
        private ConversationStore conversations;
        private int memoryWindow = 20;
        private Duration retention = Duration.ofDays(30);
        private Duration commitMargin = DEFAULT_COMMIT_MARGIN;
        private UsageSink usage = UsageSink.LOG;
        private ContentPolicy contentPolicy = ContentPolicy.NONE;
        private ExecutionTracer tracer = ExecutionTracer.NOOP;
        private InstantSource clock = InstantSource.system();
        private Duration toolCallLimit = Duration.ofSeconds(5);
        private Function<Throwable, Optional<Duration>> retryAfter = t -> Optional.empty();
        private Duration rateLimitRetryAfter;

        private Builder() {
        }

        /**
         * The model, built by the worker. Leave the provider's own retries at 0 ({@code maxRetries(0)}): the workflow's
         * {@code retry} owns them, with its delays and limits, and a fresh correlationId per attempt.
         */
        public Builder chatModel(ChatModel chatModel) {
            this.chatModel = chatModel;
            return this;
        }

        /** Conversation memory; without it, handlers have none. */
        public Builder conversations(ConversationStore store) {
            this.conversations = store;
            return this;
        }

        /** Conversation memory whose big exchanges go to {@code blobs} by pointer. */
        public Builder conversations(ConversationStore store, BlobStore blobs) {
            this.conversations = new BlobOffloadingConversationStore(store, blobs);
            return this;
        }

        /**
         * Messages of the conversation the model sees, before the execution's own question (the store keeps them all
         * until they expire). Default 20.
         */
        public Builder memoryWindow(int messages) {
            if (messages < 1) {
                throw new IllegalArgumentException("memoryWindow must be at least 1");
            }
            this.memoryWindow = messages;
            return this;
        }

        /**
         * SDD 3a.3c: an execution that ends less than this before its task's deadline doesn't commit its exchange —
         * the engine may discard its result. Covers the result's way back to the engine and the clocks' difference.
         * Default 5 s.
         */
        public Builder commitMargin(Duration margin) {
            if (margin == null || margin.isNegative()) {
                throw new IllegalArgumentException("commitMargin must be zero or positive");
            }
            this.commitMargin = margin;
            return this;
        }

        /** How long an exchange is kept after its commit. Default 30 days. */
        public Builder retention(Duration retention) {
            this.retention = Objects.requireNonNull(retention, "retention");
            return this;
        }

        public Builder usage(UsageSink usage) {
            this.usage = Objects.requireNonNull(usage, "usage");
            return this;
        }

        /** The sensitivity policy's hook (SDD 3a.5). Default {@link ContentPolicy#NONE}: no content recorded anywhere. */
        public Builder contentPolicy(ContentPolicy contentPolicy) {
            this.contentPolicy = Objects.requireNonNull(contentPolicy, "contentPolicy");
            return this;
        }

        /** Each model call becomes a {@code stint.llm} span, child of the task's. Default: no spans. */
        public Builder tracer(ExecutionTracer tracer) {
            this.tracer = Objects.requireNonNull(tracer, "tracer");
            return this;
        }

        public Builder clock(InstantSource clock) {
            this.clock = Objects.requireNonNull(clock, "clock");
            return this;
        }

        /** A tool call longer than this is reported (SDD 3a.3, 5.8). Default 5 s. */
        public Builder toolCallLimit(Duration limit) {
            this.toolCallLimit = Objects.requireNonNull(limit, "limit");
            return this;
        }

        /**
         * Reads a rate limit's {@code Retry-After} from what the provider threw — LangChain4j's
         * {@code RateLimitException} doesn't carry it. Empty by default.
         */
        public Builder retryAfter(Function<Throwable, Optional<Duration>> extractor) {
            this.retryAfter = Objects.requireNonNull(extractor, "extractor");
            return this;
        }

        /** The {@code retryAfter} of a rate limit when the extractor finds none. None by default. */
        public Builder rateLimitRetryAfter(Duration retryAfter) {
            this.rateLimitRetryAfter = retryAfter;
            return this;
        }

        public LangChain4jWorker build() {
            return new LangChain4jWorker(this);
        }
    }
}
