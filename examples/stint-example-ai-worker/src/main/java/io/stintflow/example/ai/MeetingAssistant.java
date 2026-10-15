package io.stintflow.example.ai;

import java.util.List;

import dev.langchain4j.service.UserMessage;
import io.quarkus.runtime.annotations.RegisterForProxy;
import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * The example's AI Service. In a native image, LangChain4j implements it with a JDK proxy and reads its methods
 * reflectively, so it is registered for both (SDD 3a.3, ADR 3a.3-A) — like the records it returns.
 */
@RegisterForProxy
@RegisterForReflection(methods = true)
public interface MeetingAssistant {

    Summary summarize(@UserMessage String request);

    @UserMessage("List the action items agreed in the meeting we discussed.")
    ActionItems actionItems();

    @RegisterForReflection
    record Summary(String topic, int speakers) {
    }

    /** {@code contextMessages}: how many messages the model saw — the proof that the memory reached it. */
    @RegisterForReflection
    record ActionItems(List<String> items, int contextMessages) {
    }
}
