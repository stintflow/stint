package io.stintflow.knowledge;

import io.stintflow.spi.WorkflowRef;

/**
 * Where a batch comes from (SDD 3b.1, 4.5): what the change log records about it. Ids only.
 *
 * @param instanceId the workflow instance that wrote
 * @param chainId    its business chain (SDD 2.5), or {@code null}
 * @param definition the definition (with its version) the instance runs, or {@code null}
 */
public record Source(String instanceId, String chainId, WorkflowRef definition) {

    /** A batch written by hand, with nothing of a workflow behind it. */
    public static final Source NONE = new Source(null, null, null);
}
