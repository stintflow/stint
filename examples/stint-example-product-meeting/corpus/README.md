# Synthetic meeting corpus (SDD 3a.6, format v1)

English meetings for the Phase 3a milestone, and the seed of the sandbox corpus (S.5). **Synthetic only**: no real
person, meeting or recording. Every piece of personal data is a planted marker (`SYNTH-PII-<nnnn>`), so scanners can
prove it never leaks.

## Layout
- `meetings/<id>.yaml`: one meeting (format below).
- `manifest.json`: the SHA-256 of every script and, once generated, of every audio file, with the TTS engine, its
  version, the model and the voices.
- `audio/`: generated audio, git-ignored. `scripts/generate-audio.py` regenerates it (Kokoro-82M, Apache-2.0, through
  kokoro-onnx, MIT, on CPU) and checks each script against the manifest first.

Scripts are stored with LF line endings (`.gitattributes`), so their hashes are the same on every checkout.

## Meeting format
```yaml
id: m-001                       # unique, also the meetingId of meeting-recorded.v1
language: en
label: confidential             # the sensitivity label the recorder puts on the event
speakers: [ { id: s1, voice: af_heart } ]       # synthetic speakers and their Kokoro voices
script:                         # in order; one turn per line
  - { speaker: s1, text: "..." }
expected:                       # the answer key
  docType: ata                  # ata | adr | runbook
  urgent: false
  items: [ "I will ..." ]       # the action items the minutes must hold, in order
  review: false                 # true: the fake decision is uncertain and the review branch runs
pii: [ SYNTH-PII-0101 ]         # every marker planted in the script
```

## Rules
- Every marker in `pii` appears in the script, and every `SYNTH-PII-` in the script is listed.
- Personal data takes forms the reference redactor recognizes: an e-mail (`SYNTH-PII-0102@example.com`), a phone
  (`+55 11 98765-4321`), or a name that is a marker (`SYNTH-PII-0101 Rivera`, redacted by the term list).
- Changing a script means updating its hash in the manifest; `CorpusTest` fails otherwise.
