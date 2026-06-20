# Horizontal Scalability via an Optional Async Activity-Worker Execution Mode

**Date:** 2026-06-20
**Status:** Proposed (pending maintainer discussion)
**Context:** Maintainer interest expressed in horizontal scaling; needs broader maintainer review before adoption.

> **Draft note:** authored to be submitted to `quarkus-flow/adr/2026-06-20-horizontal-scaling-design.md`.
> A working reference implementation of the topology described here exists as a standalone, CNCF-spec-compatible
> engine ("Stint") — referenced below as proof the model is viable. This ADR proposes adopting the *pattern* as an
> **optional** mode in Quarkus Flow, not importing that engine.

## Context

Quarkus Flow executes a workflow **in-process**: the orchestration loop and every task run inside one JVM (HTTP/OpenAPI/function/agent tasks only *call out*). This is the project's deliberate identity — lightweight, low-dependency, CDI-first, native-friendly.

Durability and high-availability already exist:

- **`persistence/`** — checkpoints instance state across 4 backends (jpa, redis, infinispan, mvstore) via the `PersistenceInstanceWriter`/`reader` SPI; `FlowPersistenceRestore` rehydrates instances on `WorkflowApplicationReady`.
- **`messaging/`** — a CloudEvents bridge over SmallRye Reactive Messaging (`flow-in`/`flow-out`), enabling event-driven `listen`/`emit`.
- **`durable-kubernetes/`** — Kubernetes `Lease`-based **leader election** (`PoolLeaderController`/`PoolMemberController`, `MemberLeaseCoordinator`).
- **suspend / resume / abort** — already demonstrated (`examples/suspend-resume-abort`).

What is **missing** is *horizontal scale-out of execution throughput*. `durable-kubernetes` gives **HA via a single leader** — failover, not work distribution. There is today no way to spread the execution of a workflow's tasks across many nodes/workers. As instance volume or per-task cost grows, a single orchestrating JVM is the ceiling.

The building blocks to close this gap are **already present** (persistence checkpointing, CloudEvents transport, event-driven resume). What is absent is a seam to *dispatch a task to a remote worker and resume on its result* instead of running it inline.

## Goals

1. **Optional horizontal scale-out**: distribute task execution across workers/nodes for throughput, not just failover.
2. **Zero change to the lightweight default**: the in-process path stays byte-for-byte the default behaviour.
3. **Reuse existing infrastructure**: persistence (checkpoint), messaging (CloudEvents), event-driven resume, fault tolerance — no new storage or transport stack.
4. **Transport/deployment agnosticism**: workers may run as another pod, a Knative service, or a FaaS function; the engine must not hard-depend on any one.
5. **Native-friendly**: all decisions made at build time where possible; no reflection-heavy runtime wiring.

## Non-Goals

- **Not** changing or deprecating the in-process execution path (it remains the default).
- **Not** mandating Kubernetes (this is independent of `durable-kubernetes`; it complements it).
- **Not** introducing a new persistence or transport layer (reuse the existing SPIs/connectors).
- **Not** per-task deployment tooling (how a worker is packaged/deployed is IaC/operator concern, not runtime).
- **Not** a distributed orchestrator rewrite — the orchestration loop stays where it is; only *task execution* becomes optionally remote.

## Decision

Introduce a pluggable **`TaskExecutor` SPI** with two strategies, selectable per task (or per workflow) via configuration/metadata:

- **`inline`** — the current behaviour (execute the task in-process). **Default. Unchanged.**
- **`async`** — instead of invoking the task in-process, **publish a CloudEvent**, **checkpoint** the instance as waiting, **suspend**, and **resume** when the correlated result event arrives.

This is the classic **activity-worker** pattern (à la Temporal / Step Functions activities), expressed entirely on top of machinery Quarkus Flow already ships.

### Flow (async mode)

```
Orchestrator reaches an async task
  → build invocation event (CloudEvent: type=task.invoke, correlationid, instanceid, taskid, attempt)
  → persist instance state as WAITING(correlationid)        [reuse persistence SPI]
  → publish to transport                                    [reuse messaging / CloudEvents bridge]
  → arm timeout                                             [reuse scheduler]
  → SUSPEND  (nothing running on this node)

Worker (any node / pod / Knative / FaaS) consumes invoke
  → runs the task (a CDI bean method / the existing task handler)
  → emits result CloudEvent (type=task.result, same correlationid)

Orchestrator (any node) consumes result
  → look up instance WAITING on correlationid               [persistence index]
  → load checkpoint, merge output, advance / dispatch next  [existing resume path]
```

The orchestrator side is **node-agnostic**: because state lives in persistence and results arrive as events, *any* instance of the application can resume *any* suspended instance. That is what turns single-leader HA into horizontal scale-out.

## Architecture

### 1. `TaskExecutor` SPI (the one new seam)

```java
public interface TaskExecutor {
    /** Either run the task now (inline) or dispatch it and signal suspension (async). */
    CompletionStage<TaskOutcome> execute(TaskInvocation invocation);
    ExecutionMode mode(); // INLINE | ASYNC
}
```

- `InlineTaskExecutor` wraps the current execution path (default; ships in `core`).
- `AsyncTaskExecutor` lives in a new optional module (e.g. `core` + an opt-in extension) and delegates dispatch to the **messaging** module and checkpointing to **persistence**.

The hard part — and the main thing for maintainers to weigh — is **where the SDK exposes this hook**. Quarkus Flow depends on `io.serverlessworkflow.impl.*` for task execution; cleanly intercepting "about to execute task X" may require either a wrapper at the Quarkus layer or a small upstream extension point in the Serverless Workflow SDK. This coordination cost should be assessed early.

### 2. Wire contract (reuse CloudEvents)

Reuse the existing CloudEvents bridge. Define three event types and correlation extensions (`correlationid`, `workflowinstanceid`, `taskid`, `attempt`). Workers in any language interoperate by honouring these.

### 3. Correlation & idempotency

- Persistence gains a **correlation index** (waiting-correlation → instance id). DynamoDB-GSI / SQL-index shaped; trivial to add to the existing backends.
- At-least-once transports → dedup by `correlationid + attempt`.

### 4. Claim-check (large payloads)

Add a small **`BlobStore` SPI**; when a payload exceeds a transport's declared limit, offload and pass a pointer (the engine rehydrates on the other side). Honest **capability metadata** per transport (delivery guarantee, max payload, native delay, dedup) lets the core adapt rather than assume uniformity.

### 5. Worker SDK

A thin `TaskHandler` programming model so a task author writes logic once; bindings (in-pool runner, Knative HTTP receiver, FaaS handler) are thin shells. Workers reuse the same CloudEvents codec as the engine.

### 6. Timeouts

Arm a timer on dispatch (reuse `scheduler/`); cancel on result; on fire, retry (increment `attempt`) or fail/compensate. Prevents an unanswered worker from suspending an instance forever.

## Consequences

### Positive
- True **horizontal scale-out** of throughput, additive to the existing lease-based HA.
- **Lightweight default preserved** — opt-in only; no cost for users who don't enable it.
- **Maximal reuse** — persistence, messaging, resume, fault tolerance already exist.
- **Deployment-agnostic** — workers anywhere (pod, Knative, FaaS); no K8s mandate.
- Establishes a clean **`TaskExecutor` seam** useful beyond this feature (e.g. testing, instrumentation).

### Negative
- Requires a hook into the task-execution pipeline, possibly touching the upstream Serverless Workflow SDK (coordination cost).
- New surface: correlation index, idempotency, claim-check, capability metadata, worker SDK.
- Async mode introduces distributed-systems concerns (exactly-once-ish, ordering, orphaned workers) that must be documented honestly per transport.

### Neutral
- Independent of `durable-kubernetes` but complementary (lease for HA + async for scale-out compose well).
- Adjacent to the (approved-but-unbuilt) Runner REST API ADR (#52), which assumes a `WorkflowRegistry` that does not yet exist; a registry would also benefit this work.

## Alternatives Considered

1. **Rely on `durable-kubernetes` lease only** — gives HA/failover, *not* throughput scale-out. Rejected as insufficient for the goal.
2. **Full distributed-orchestrator rewrite** — contradicts the project's lightweight identity; high risk. Rejected.
3. **Pure choreography (no orchestrator)** — loses the declarative CNCF workflow model. Rejected.
4. **Make distribution the default** — breaks the lightweight identity and the "low-dependency" promise. Rejected in favour of opt-in.

## References

- CNCF Serverless Workflow Specification — https://serverlessworkflow.io/
- Existing Quarkus Flow modules: `persistence/`, `messaging/`, `durable-kubernetes/`, `scheduler/`
- Runner REST API ADR — `adr/2026-05-05-workflow-runner-rest-api-design.md` (Issue #52)
- Build-time agentic workflow ADR — `adr/2026-05-15-buildtime-agentic-workflow-generation.md`
- Reference implementation (standalone, CNCF-compatible, FaaS-agnostic): "Stint" — the activity-worker topology described here, running end-to-end with SQS/DynamoDB/S3 connectors and an in-memory local path.
