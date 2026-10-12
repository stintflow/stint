package io.stintflow.example.ai;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

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
 * A deterministic stand-in for an LLM provider — no network, synthetic data only. It answers by rules over the
 * request's messages:
 * <ul>
 *   <li>a request to summarize: the transcript's {@code Topic:} line and the number of distinct {@code Speaker X:};</li>
 *   <li>a request for action items: every transcript line with {@code " will "} found in <em>any</em> user message of
 *       the request — the transcript only reaches it through the conversation memory — and how many messages it saw.</li>
 * </ul>
 */
public final class FakeMeetingModel implements ChatModel {

    public static final String MODEL_NAME = "fake-meeting-rules";

    @Override
    public ChatResponse doChat(ChatRequest request) {
        String last = lastUserText(request.messages());
        ObjectNode answer = Json.obj();
        if (last.startsWith("Summarize")) {
            Set<String> speakers = new LinkedHashSet<>();
            for (String line : last.split("\n")) {
                if (line.startsWith("Topic:")) {
                    answer.put("topic", line.substring("Topic:".length()).trim());
                } else if (line.startsWith("Speaker ") && line.contains(":")) {
                    speakers.add(line.substring(0, line.indexOf(':')).trim());
                }
            }
            answer.put("speakers", speakers.size());
        } else {
            ArrayNode items = answer.putArray("items");
            for (ChatMessage message : request.messages()) {
                if (message instanceof UserMessage user && user.hasSingleText()) {
                    for (String line : user.singleText().split("\n")) {
                        if (line.contains(" will ")) {
                            items.add(line.trim());
                        }
                    }
                }
            }
            answer.put("contextMessages", request.messages().size());
        }
        String text = answer.toString();
        return ChatResponse.builder()
                .aiMessage(AiMessage.from(text))
                .modelName(MODEL_NAME)
                .tokenUsage(new TokenUsage(characters(request.messages()) / 4, text.length() / 4))
                .finishReason(FinishReason.STOP)
                .build();
    }

    private static String lastUserText(List<ChatMessage> messages) {
        List<ChatMessage> reversed = new ArrayList<>(messages);
        Collections.reverse(reversed);
        for (ChatMessage message : reversed) {
            if (message instanceof UserMessage user && user.hasSingleText()) {
                return user.singleText();
            }
        }
        return "";
    }

    private static int characters(List<ChatMessage> messages) {
        int total = 0;
        for (ChatMessage message : messages) {
            if (message instanceof UserMessage user && user.hasSingleText()) {
                total += user.singleText().length();
            }
        }
        return total;
    }
}
