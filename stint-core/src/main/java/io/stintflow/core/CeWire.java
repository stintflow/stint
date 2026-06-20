package io.stintflow.core;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.jackson.JsonFormat;

/**
 * Serializes a {@link CloudEvent} to/from its canonical JSON form for transports that carry a text
 * body (SQS, SNS, EventBridge, HTTP, ...). Cloud-neutral: only depends on the CNCF CloudEvents libs.
 */
public final class CeWire {

    private static final EventFormat FORMAT = new JsonFormat();

    private CeWire() {
    }

    public static byte[] toJson(CloudEvent event) {
        return FORMAT.serialize(event);
    }

    public static CloudEvent fromJson(byte[] bytes) {
        return FORMAT.deserialize(bytes);
    }
}
