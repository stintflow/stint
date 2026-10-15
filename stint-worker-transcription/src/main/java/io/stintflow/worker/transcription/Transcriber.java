package io.stintflow.worker.transcription;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * SDD 3a.6, RF1: the transcription port, on the worker side only — the engine never sees it. Audio in, a transcript
 * with its language, duration and timed segments out. An implementation fails with a {@link io.stintflow.worker.TaskFailure}
 * from {@link TranscriptionFailures}: a standard error the workflow can catch or retry, never quoting the server's
 * message (it may quote the audio).
 */
public interface Transcriber {

    Transcript transcribe(Audio audio, Options options);

    /** Who transcribes: the engine (e.g. {@code speaches}), its model, and whether the audio leaves the process. */
    Provenance provenance();

    /** The audio, as read from its blob. */
    record Audio(byte[] bytes, String mediaType, String fileName) {
        public Audio {
            Objects.requireNonNull(bytes, "bytes");
        }
    }

    /** @param language the spoken language (ISO 639-1, e.g. {@code en}), or {@code null} to detect it */
    record Options(String language) {
    }

    /** A transcript: what the transcriber heard, in timed segments. */
    record Transcript(String language, Duration duration, List<Segment> segments) {
        public Transcript {
            segments = List.copyOf(segments);
        }

        /** The segments' text, in order, separated by single spaces. */
        public String text() {
            return String.join(" ", segments.stream().map(Segment::text).map(String::strip).toList());
        }
    }

    record Segment(Duration start, Duration end, String text) {
    }

    record Provenance(String engine, String model, boolean remote) {
    }
}
