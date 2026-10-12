package io.stintflow.worker.langchain4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.memory.chat.MessageWindowChatMemory;

/**
 * SDD 3a.3, 5.4: pins how {@link ExecutionMemoryStore} works under LangChain4j's {@link MessageWindowChatMemory} — if
 * an upgrade changes how the memory writes back, this fails before a conversation is committed wrong.
 */
class ExecutionMemoryStoreTest {

    private final List<ChatMessage> committed = List.of(UserMessage.from("earlier question"), AiMessage.from("earlier answer"),
            UserMessage.from("older question"), AiMessage.from("older answer"));

    @Test
    void only_the_execution_s_own_messages_are_added_and_the_window_is_what_the_model_sees() {
        ExecutionMemoryStore store = new ExecutionMemoryStore(committed);
        MessageWindowChatMemory memory = MessageWindowChatMemory.builder().id("m").maxMessages(3)
                .chatMemoryStore(store).build();

        memory.add(SystemMessage.from("synthetic system"));
        memory.add(UserMessage.from("new question"));
        memory.add(AiMessage.from("new answer"));

        assertThat(store.added()).extracting(ChatMessage::type).containsExactly(
                dev.langchain4j.data.message.ChatMessageType.USER, dev.langchain4j.data.message.ChatMessageType.AI);
        assertThat(memory.messages()).hasSize(3).first().isInstanceOf(SystemMessage.class);
        assertThat(memory.messages().getLast()).isEqualTo(AiMessage.from("new answer"));
    }

    @Test
    void clear_forgets_what_the_execution_added() {
        ExecutionMemoryStore store = new ExecutionMemoryStore(committed);
        MessageWindowChatMemory memory = MessageWindowChatMemory.builder().id("m").maxMessages(10)
                .chatMemoryStore(store).build();
        memory.add(UserMessage.from("new question"));

        memory.clear();

        assertThat(store.added()).isEmpty();
        assertThat(memory.messages()).isEmpty();
    }
}
