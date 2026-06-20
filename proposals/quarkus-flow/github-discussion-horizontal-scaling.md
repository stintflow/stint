# GitHub Discussion draft — Quarkus Flow

> Conversational pitch for the maintainers' thread. The ADR
> (`2026-06-20-horizontal-scaling-design.md`) is the formal attachment; this is what gets pasted
> into a Discussion to spark the conversation. Replace `@maintainer` and the ADR link before posting.

---

**Title:** Proposal: optional horizontal scale-out via an async activity-worker mode

Following up on a chat with @maintainer about horizontal scaling — here's a concrete shape so the team can react.

### The gap

Quarkus Flow runs a workflow **in-process**. We already ship a lot of what matters: durable persistence (4 backends), a CloudEvents bridge (`messaging`), suspend/resume, and lease-based HA (`durable-kubernetes`).

What we don't have is horizontal scale-out of **execution throughput**. The lease gives *failover* — one leader — not *work distribution*. A single orchestrating JVM is the ceiling as instance volume or per-task cost grows.

### The idea (opt-in, additive)

A pluggable `TaskExecutor` with two modes:

- **`inline`** — today's behaviour. Default. Unchanged.
- **`async`** — instead of running a task in-process, publish a CloudEvent, checkpoint the instance, suspend, and resume when the result event arrives.

Because state lives in persistence and results arrive as events, **any node can resume any suspended instance** — that's what turns single-leader HA into scale-out. A worker can be another pod, a Knative service, or a FaaS function.

The key point: this **reuses what we already ship** (persistence + CloudEvents + resume + scheduler). It is not a rewrite, and it does **not** touch the lightweight in-process default — you only pay for it if you turn it on.

### The honest hard part

The cleanest place to intercept "about to execute task X" may need a hook into the `io.serverlessworkflow.impl` pipeline (we depend on impl). Whether that's a wrapper at the Quarkus layer or a small upstream SDK extension point is the main thing I'd want the team's read on.

### What I'm asking

1. Is there appetite for this as an **optional** mode (explicitly not the default)?
2. Preference on the hook point — wrap at the Quarkus layer, or propose an extension point upstream in the Serverless Workflow SDK?
3. Worth an ADR? (Drafted — happy to open a PR.)

For context: I've built a standalone CNCF-spec engine that runs exactly this topology end-to-end (SQS/DynamoDB/S3 connectors + an in-memory local path), so I can point at a working reference for any of the mechanics.

Full ADR draft: _[link]_
