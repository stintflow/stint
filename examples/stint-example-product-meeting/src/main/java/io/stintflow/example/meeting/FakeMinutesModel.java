package io.stintflow.example.meeting;

import java.util.List;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;
import dev.langchain4j.model.output.FinishReason;
import dev.langchain4j.model.output.TokenUsage;
import io.stintflow.wire.Json;

/**
 * A deterministic stand-in for the minutes generator (SDD 3a.6, CI): no network. It reads the transcript out of the
 * prompt and answers, by rules, the JSON the {@link MinutesWriter} expects — the first line as the summary, lines with
 * {@code "decided"} as decisions, and every line with {@code "I will"} as an action item, in order. It counts tokens as
 * characters / 4.
 */
public final class FakeMinutesModel implements ChatModel {

    public static final String MODEL_NAME = "fake-minutes-rules";

    @Override
    public ChatResponse doChat(ChatRequest request) {
        String prompt = lastUserText(request.messages());
        String transcript = prompt.contains("Transcript:\n") ? prompt.substring(prompt.indexOf("Transcript:\n") + 12)
                : prompt;
        List<String> lines = transcript.lines().map(String::strip).filter(l -> !l.isEmpty()).toList();
        ObjectNode answer = Json.obj();
        answer.put("summary", lines.isEmpty() ? "" : lines.getFirst());
        answer.putArray("requirements");
        ArrayNode decisions = answer.putArray("decisions");
        lines.stream().filter(l -> l.toLowerCase(java.util.Locale.ROOT).contains("decided")).forEach(decisions::add);
        answer.putArray("constraints");
        answer.putArray("openQuestions");
        ArrayNode items = answer.putArray("items");
        int n = 0;
        for (String line : lines) {
            int at = line.indexOf("I will");
            if (at >= 0) {
                items.addObject().put("id", "item-" + (++n)).put("text", line.substring(at));
            }
        }
        String text = answer.toString();
        return ChatResponse.builder().aiMessage(AiMessage.from(text)).modelName(MODEL_NAME)
                .tokenUsage(new TokenUsage(prompt.length() / 4, text.length() / 4)).finishReason(FinishReason.STOP)
                .build();
    }

    private static String lastUserText(List<ChatMessage> messages) {
        for (int i = messages.size() - 1; i >= 0; i--) {
            if (messages.get(i) instanceof UserMessage user && user.hasSingleText()) {
                return user.singleText();
            }
        }
        return "";
    }
}
