#!/usr/bin/env python3
"""SDD 3a.6, 8b: a minimal OpenAI-compatible transcription server over faster-whisper (MIT), for the manual run when
the speaches container is not available. Same engine as speaches (CTranslate2), same route and answer:
POST /v1/audio/transcriptions (multipart: file, model, language, response_format=verbose_json) ->
{text, language, duration, segments: [{id, start, end, text}]}.

Runs outside the Maven build (WSL2, Python 3.11+), on loopback only:
    pip install faster-whisper==1.1.1 requests
    python scripts/whisper-server.py --model Systran/faster-distil-whisper-small.en --device cpu --port 8001

No authentication, no persistence: the audio is held in memory for the request and dropped. Never expose it.
"""
import argparse
import io
import json
import time
import wave

import numpy as np
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from faster_whisper import WhisperModel
from email import message_from_bytes
from email.policy import HTTP


def parse_multipart(body: bytes, content_type: str) -> dict:
    """The form's fields, by name, as bytes (the standard library's MIME parser)."""
    message = message_from_bytes(b"Content-Type: " + content_type.encode() + b"\r\n\r\n" + body, policy=HTTP)
    return {part.get_param("name", header="content-disposition"): part.get_payload(decode=True)
            for part in message.iter_parts()}


def samples(audio: bytes):
    """A 16 kHz mono 16-bit WAV (what generate-audio.py writes) as floats, without PyAV; anything else as is."""
    if audio[:4] != b"RIFF":
        return io.BytesIO(audio)
    with wave.open(io.BytesIO(audio)) as wav:
        if wav.getframerate() != 16000 or wav.getnchannels() != 1 or wav.getsampwidth() != 2:
            return io.BytesIO(audio)
        return np.frombuffer(wav.readframes(wav.getnframes()), dtype=np.int16).astype(np.float32) / 32768.0


def main() -> None:
    args = argparse.ArgumentParser()
    args.add_argument("--model", default="Systran/faster-distil-whisper-small.en")
    args.add_argument("--device", default="cpu")
    args.add_argument("--compute-type", default="int8")
    args.add_argument("--threads", type=int, default=6)
    args.add_argument("--port", type=int, default=8001)
    opts = args.parse_args()
    started = time.time()
    model = WhisperModel(opts.model, device=opts.device, compute_type=opts.compute_type, cpu_threads=opts.threads)
    print(f"loaded {opts.model} on {opts.device}/{opts.compute_type} in {time.time() - started:.1f} s", flush=True)

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            if self.path != "/v1/audio/transcriptions":
                self.send_error(404)
                return
            body = self.rfile.read(int(self.headers["Content-Length"]))
            fields = parse_multipart(body, self.headers["Content-Type"])
            language = fields.get("language", b"").decode() or None
            begun = time.time()
            segments, info = model.transcribe(samples(fields["file"]), language=language, beam_size=5,
                                              vad_filter=False)
            out = [{"id": i, "start": round(s.start, 2), "end": round(s.end, 2), "text": s.text}
                   for i, s in enumerate(segments)]
            answer = {"text": "".join(s["text"] for s in out).strip(), "language": info.language,
                      "duration": round(info.duration, 2), "segments": out}
            print(f"transcribed {info.duration:.1f} s of audio in {time.time() - begun:.1f} s", flush=True)
            payload = json.dumps(answer).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, fmt, *a):   # no request lines: nothing of the audio is logged anyway
            pass

    ThreadingHTTPServer(("127.0.0.1", opts.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
