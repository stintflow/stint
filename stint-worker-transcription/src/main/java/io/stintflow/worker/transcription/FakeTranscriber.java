package io.stintflow.worker.transcription;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * A transcriber for tests and the CI (SDD 3a.6, 4.2): it hears nothing, it looks the audio up — by the SHA-256 of its
 * bytes — and returns the transcript it was given for it. The first {@code failures} calls fail as an unavailable
 * server, to exercise the workflow's retry. Never calls anything.
 */
public final class FakeTranscriber implements Transcriber {

    private final Function<byte[], Transcript> lookup;
    private final AtomicInteger failuresLeft;
    private final AtomicInteger calls = new AtomicInteger();

    private FakeTranscriber(Function<byte[], Transcript> lookup, int failures) {
        this.lookup = lookup;
        this.failuresLeft = new AtomicInteger(failures);
    }

    /** Transcripts by the SHA-256 (hex) of the audio they stand for. */
    public static FakeTranscriber scripted(Map<String, Transcript> bySha256) {
        Map<String, Transcript> copy = Map.copyOf(bySha256);
        return new FakeTranscriber(bytes -> copy.get(sha256(bytes)), 0);
    }

    /** This one, whose first {@code failures} calls fail as an unavailable server. */
    public FakeTranscriber failingFirst(int failures) {
        return new FakeTranscriber(lookup, failures);
    }

    public int calls() {
        return calls.get();
    }

    @Override
    public Transcript transcribe(Audio audio, Options options) {
        calls.incrementAndGet();
        if (failuresLeft.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
            throw TranscriptionFailures.unavailable(503, "The transcription server fake answered HTTP 503", null);
        }
        Transcript transcript = lookup.apply(audio.bytes());
        if (transcript == null) {
            throw TranscriptionFailures.validation("The fake transcriber does not know this audio");
        }
        return transcript;
    }

    @Override
    public Provenance provenance() {
        return new Provenance("fake-transcriber", "corpus-script", false);
    }

    public static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
