package io.stintflow.example.meeting;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * A deterministic stand-in for the extraction model (SDD 3b.3, CI): no network. It reads the numbered passages out of the prompt
 * and answers, by rules, the structured answer of the knowledge schema: every system or term of the glossary the passages name
 * (by name or alias), every person pseudonym ({@code PERSON_<hex>}) as a participant of the meeting. It never invents a name.
 */
public final class FakeKnowledgeModel implements ChatModel {

    public static final String MODEL_NAME = "fake-knowledge-rules";

    private static final Pattern PASSAGE = Pattern.compile("^\\[(\\d+)\\] (.*)$");
    private static final Pattern PSEUDONYM = Pattern.compile("PERSON_[0-9a-f]+");

    private final List<Corpus.Term> glossary;

    public FakeKnowledgeModel(List<Corpus.Term> glossary) {
        this.glossary = List.copyOf(glossary);
    }

    @Override
    public ChatResponse doChat(ChatRequest request) {
        String prompt = lastUserText(request.messages());
        Map<String, List<Integer>> systems = new LinkedHashMap<>();
        Map<String, List<Integer>> persons = new LinkedHashMap<>();
        // a passage may span several lines (the minutes are markdown): a line that opens no passage continues the last one
        Map<Integer, StringBuilder> passages = new LinkedHashMap<>();
        int current = -1;
        for (String line : prompt.split("\n")) {
            Matcher passage = PASSAGE.matcher(line);
            if (passage.matches()) {
                current = Integer.parseInt(passage.group(1));
                passages.put(current, new StringBuilder(passage.group(2)));
            } else if (current >= 0) {
                passages.get(current).append('\n').append(line);
            }
        }
        for (Map.Entry<Integer, StringBuilder> entry : passages.entrySet()) {
            int ordinal = entry.getKey();
            String raw = entry.getValue().toString();
            String text = raw.toLowerCase(Locale.ROOT);
            for (Corpus.Term term : glossary) {
                if (text.contains(term.name().toLowerCase(Locale.ROOT)) || term.aliases().stream()
                        .anyMatch(a -> text.contains(a.toLowerCase(Locale.ROOT)))) {
                    systems.computeIfAbsent(term.type() + "|" + term.name(), k -> new ArrayList<>()).add(ordinal);
                }
            }
            Matcher person = PSEUDONYM.matcher(raw);
            while (person.find()) {
                persons.computeIfAbsent(person.group(), k -> new ArrayList<>()).add(ordinal);
            }
        }
        ObjectNode answer = Json.obj();
        ArrayNode entities = answer.putArray("entities");
        ArrayNode relations = answer.putArray("relations");
        int n = 0;
        for (Map.Entry<String, List<Integer>> e : systems.entrySet()) {
            String[] typeAndName = e.getKey().split("\\|", 2);
            ObjectNode entity = entities.addObject();
            entity.put("ref", "e" + (++n)).put("type", typeAndName[0]).put("mention", typeAndName[1]);
            ArrayNode ordinals = entity.putArray("passages");
            e.getValue().stream().distinct().forEach(ordinals::add);
        }
        for (Map.Entry<String, List<Integer>> e : persons.entrySet()) {
            String ref = "e" + (++n);
            ObjectNode entity = entities.addObject();
            entity.put("ref", ref).put("type", "Person").put("mention", e.getKey());
            ArrayNode ordinals = entity.putArray("passages");
            e.getValue().stream().distinct().forEach(ordinals::add);
            relations.addObject().put("type", "participated").put("from", ref).put("to", "meeting");
        }
        String text = answer.toString();
        return ChatResponse.builder().aiMessage(AiMessage.from(text)).modelName(MODEL_NAME)
                .tokenUsage(new TokenUsage(prompt.length() / 4, text.length() / 4)).finishReason(FinishReason.STOP).build();
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
