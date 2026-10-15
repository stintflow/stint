package io.stintflow.worker.langchain4j;

/**
 * The author's part of an LLM task: call the AI Service with what the task brings, and return the result — a record
 * or POJO (an object in the task's output), a {@code String}, a {@code JsonNode}, or a LangChain4j {@code Result} of
 * one of those.
 */
@FunctionalInterface
public interface AiCall<S> {
    Object call(S ai, AiTaskContext task) throws Exception;
}
