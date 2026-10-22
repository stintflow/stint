package io.stintflow.worker.knowledge;

import java.util.Objects;

/**
 * SDD 3c.5, 4.2: what the deployment declares about the channel the invitations of this origin arrive through — its name and
 * whether it authenticates who publishes on it (IAM, mutual TLS). It is a declaration of configuration, <b>not a proof</b>: the
 * code cannot see the transport. What it buys is that the origin fails closed — with an attestation that says the channel is not
 * authenticated, it answers nothing — and that each resolution records which channel it was answered through.
 */
public record ChannelAttestation(String channelId, boolean authenticated) {

    public ChannelAttestation {
        Objects.requireNonNull(channelId, "channelId");
        if (channelId.isBlank() || channelId.length() > 100) {
            throw new IllegalArgumentException("A channel has a name of up to 100 characters");
        }
    }
}
