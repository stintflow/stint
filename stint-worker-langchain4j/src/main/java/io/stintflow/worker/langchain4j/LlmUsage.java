package io.stintflow.worker.langchain4j;

import java.time.Duration;

/**
 * What one execution used of the model (SDD 3a.3, 5.6) — numbers and the model's name, never content: there is no
 * field a prompt or a response could go into.
 *
 * @param model        the model that answered (the provider's name for it), or {@code null} if none answered
 * @param calls        calls to the model, failed ones included
 * @param inputTokens  input tokens the provider reported (0 if it reported none)
 * @param outputTokens output tokens the provider reported (0 if it reported none)
 * @param toolCalls    tool calls the AI Service executed
 * @param latency      time spent waiting for the model, summed over the calls
 * @param finishReason why the last answer ended, as LangChain4j names it, or {@code null}
 */
public record LlmUsage(String model, int calls, long inputTokens, long outputTokens, int toolCalls, Duration latency,
        String finishReason) {
}
