package io.stintflow.spi;

import java.net.URI;
import java.util.concurrent.CompletionStage;

/**
 * Port: large-payload offload (the claim-check pattern).
 * <p>
 * Two uses: (1) the user's task code may deliberately stage a domain artifact and pass a pointer
 * (e.g. an S3 document); (2) the core transparently offloads any payload exceeding the transport's
 * {@link AdapterCapabilities#maxPayloadBytes()} and passes a pointer instead, rehydrating on arrival.
 */
public interface BlobStore {

    CompletionStage<URI> put(byte[] data, String key);

    /**
     * SDD 3a.5, RF7: puts {@code data} as a blob of retention class {@code cls}, under its prefix
     * ({@code retention/<class>/<key>}). An adapter that can expire or encrypt by class overrides this; the default
     * only prefixes the key, so a bucket lifecycle rule by prefix still applies and older adapters stay valid.
     */
    default CompletionStage<URI> put(byte[] data, String key, RetentionClass cls) {
        return put(data, cls.prefixed(key));
    }

    CompletionStage<byte[]> get(URI ref);

    CompletionStage<Void> delete(URI ref);
}
