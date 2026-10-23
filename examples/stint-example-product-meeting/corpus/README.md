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
label: restricted               # the level the recorder puts on the event: a raw recording is restricted
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

## The scenario of the Phase 3c milestone (SDD 3c.6, format v1.2)
Additive: nothing that reads a script for the earlier milestones looks at it.
- A product script may carry `kb.decisions`: `surface` (the words as the script has them), `label` (the decision's name in the
  graph), `summary` (what it says; sensitive) and `status`. The extraction model of the build cites a decision where a passage of
  the minutes holds its surface. `m-001`, `m-002` and `m-004` take one decision each.
- `architecture` in the manifest lists the meetings of the architecture area (`a-001` to `a-004`), text only (no audio: the
  transcription was proved in 3a.6), with `expected.drafts`: the decision's `id` (its title as a slug), its `title`, the
  product decisions it answers (`motivatedBy`, by label), the final `verdict` (`conflicts | compatible`), `how` (`auto | reviewed`:
  whether a person judged it) and `originDown` (the product area does not answer during that meeting).
- A sentence that opens with `We decided to` is a decision of the draft; `It answers the product decision "…"` names the product
  decision it answers. The model of the build is a rule over these cues: it proves the plumbing, not the quality of a model.

## The entity-resolution set (SDD 3b.2, format v1.1)
A script may carry an optional `kb` section: what a model would cite in the meeting, and what each mention should resolve to
(`kb.mentions`: `type`, `surface` as the script has it, `expect` = an entity id or `new`, `how` = `glossary | alias | existing`,
and for a `new` one the `candidates` a duplicate check must flag beside it). It is additive: nothing that reads a script for
the milestone looks at it.
- `kb/base.yaml` is the base every mention is resolved against: the `glossary`, the `existing` entities from earlier meetings
  and the `mustNotMerge` pairs (homonyms that must never become one). Its hash is `kbBase.sha256` in the manifest.
- The eight milestone meetings carry a few mentions each. `m-009` to `m-012` are the meetings written for the evaluation
  (aliases, abbreviations, case, typos, homonyms, the same thing in several meetings); they are listed under `knowledge` in the
  manifest, **not** under `meetings`, so the milestone does not run them and has no audio for them.
- `ResolutionEvalTest` resolves every mention with `EntityResolver` and checks the thresholds of the SDD: no wrong merge, at
  least 95 % of the resolvable ones right, at least 90 % of the expected duplicate candidates flagged.
