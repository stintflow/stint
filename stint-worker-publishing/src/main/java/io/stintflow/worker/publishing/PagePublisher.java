package io.stintflow.worker.publishing;

import java.net.URI;
import java.util.List;
import java.util.Objects;

import io.stintflow.spi.Sensitivity;

/**
 * SDD 3a.6, RF4: the publishing port, on the worker side only. Publishes a page — already redacted — into a space; a
 * second publish with the same {@code key} returns the page it made, unchanged ({@code created = false}). An
 * implementation fails with a {@link io.stintflow.worker.TaskFailure} from {@link PublishFailures}, never quoting the
 * server.
 */
public interface PagePublisher {

    PublishedPage publish(Page page);

    /**
     * A page to publish.
     *
     * @param space       the space's key; one allowed space only ({@link PublishHandler})
     * @param key         the idempotency key (e.g. the meeting id): one page per key and space
     * @param title       the title, built from ids only — never text of the data
     * @param body        the page in Confluence storage format (XHTML)
     * @param labels      labels to put on the page
     * @param sensitivity the data's level, carried as a label ({@code stint-<level>})
     */
    record Page(String space, String key, String title, String body, List<String> labels, Sensitivity sensitivity) {
        public Page {
            Objects.requireNonNull(space, "space");
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(title, "title");
            Objects.requireNonNull(body, "body");
            labels = labels == null ? List.of() : List.copyOf(labels);
        }
    }

    /** @param created {@code false} when a page with this key already existed and was returned as it was */
    record PublishedPage(String id, URI url, int version, boolean created) {
    }

    /** The label that makes a page findable by its key. */
    static String keyLabel(String key) {
        return "stint-key-" + key.toLowerCase(java.util.Locale.ROOT).replaceAll("[^a-z0-9-]", "-");
    }

    static String levelLabel(Sensitivity level) {
        return "stint-" + (level == null ? Sensitivity.RESTRICTED : level).wireName();
    }
}
