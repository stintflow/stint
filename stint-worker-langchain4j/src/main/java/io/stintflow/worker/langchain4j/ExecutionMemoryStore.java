package io.stintflow.worker.langchain4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.store.memory.chat.ChatMemoryStore;

/**
 * The {@link ChatMemoryStore} one execution's AI Service sees (SDD 3a.3, 5.4): it starts from the conversation
 * committed before the execution, and keeps what the execution adds apart, so the execution commits only its own
 * messages, in one act, and only if it succeeds.
 * <p>
 * The {@code ChatMemory} (a window) writes back its whole, possibly evicted, list on every update: that list is what
 * the model sees next, so it is what {@link #getMessages} returns. A message is this execution's when it is not one of
 * the instances this store already held. System messages are not part of the conversation — the AI Service adds its
 * own on every call — so they are never committed.
 */
final class ExecutionMemoryStore implements ChatMemoryStore {

    private final Set<ChatMessage> known = Collections.newSetFromMap(new IdentityHashMap<>());
    private final List<ChatMessage> added = new ArrayList<>();
    private List<ChatMessage> view;

    ExecutionMemoryStore(List<ChatMessage> committed) {
        this.view = new ArrayList<>(committed);
        known.addAll(committed);
    }

    @Override
    public synchronized List<ChatMessage> getMessages(Object memoryId) {
        return new ArrayList<>(view);
    }

    @Override
    public synchronized void updateMessages(Object memoryId, List<ChatMessage> messages) {
        for (ChatMessage message : messages) {
            if (known.add(message) && !(message instanceof SystemMessage)) {
                added.add(message);
            }
        }
        view = new ArrayList<>(messages);
    }

    /** {@code ChatMemory.clear()}: empties the execution's view and forgets what it added; the store is untouched. */
    @Override
    public synchronized void deleteMessages(Object memoryId) {
        view = new ArrayList<>();
        added.clear();
    }

    synchronized List<ChatMessage> added() {
        return List.copyOf(added);
    }
}
