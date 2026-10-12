package io.stintflow.worker.langchain4j.memory;

/** The result of {@link ConversationStore#commit}: the first commit of a correlationId wins. */
public enum CommitOutcome {
    COMMITTED,
    /** An exchange with the same instanceId and correlationId was already committed; nothing was written. */
    ALREADY_COMMITTED
}
