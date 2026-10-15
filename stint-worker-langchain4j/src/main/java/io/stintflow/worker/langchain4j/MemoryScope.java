package io.stintflow.worker.langchain4j;

/** Which conversation a handler's executions share (SDD 3a.3, 5.3). */
public enum MemoryScope {
    /** The default: every task of the instance with this scope sees the same conversation. */
    INSTANCE,
    /** A conversation of the task's own (instance + taskId): two steps of one instance that must not see each other. */
    TASK,
    /** No memory: each execution starts from nothing and commits nothing. */
    NONE
}
