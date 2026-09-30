# Stint

[![CI](https://github.com/stintflow/stint/actions/workflows/ci.yml/badge.svg)](https://github.com/stintflow/stint/actions/workflows/ci.yml)
[![Integration (floci)](https://github.com/stintflow/stint/actions/workflows/it.yml/badge.svg)](https://github.com/stintflow/stint/actions/workflows/it.yml)

> Cloud-agnostic, distributed, serverless execution engine for **CNCF Serverless Workflows**.
> AWS-first in practice, cloud-blind by design.

A *stint* is the run a driver does between pit stops — a bounded, ephemeral segment of work. That is exactly
a worker here: it spins up, runs its leg, emits a result, and disappears. Workflows are the relay; tasks are
the legs; the correlation id is the baton.

- **GitHub org:** `stintflow` (github.com/stintflow)
- **Maven groupId:** `io.github.stintflow`
- **Java package:** `io.stintflow`

## The empty quadrant

| Engine | Topology | Cloud coupling |
|---|---|---|
| Quarkus Flow | in-process orchestrator | agnostic, but single-JVM |
| SonataFlow / Kogito | decomposed services | K8s / Knative committed |
| **Stint (this)** | **distributed, per-task workers** | **transport/cloud-agnostic** |

Same CNCF DSL as the others; opposite runtime. The orchestrator dispatches each task over a transport and
**suspends**; an ephemeral worker (Lambda / Knative / pool) runs it and emits a result that resumes the
instance. `stint-spi` and `stint-core` never import a cloud SDK or an AI library — that invariant is
enforced by a single consolidated ArchUnit suite (`stint-architecture-tests`).

## Module map

```
stint-spi               ports + domain                                    (ZERO cloud/AI deps)
stint-wire              the CloudEvents wire contract: Json, CeWire,
                         DefaultCloudEventCodec, ClaimCheck                (ZERO cloud/AI deps)
stint-core               the cloud-blind orchestrator: dispatch / suspend / resume
stint-dsl                YAML/JSON front-end: compiles the CNCF DSL 1.0 into stint-core's tree model
stint-worker-sdk         uniform TaskHandler programming model (depends on stint-wire, not stint-core)
stint-otel               OpenTelemetry implementation of the tracing port (the only module using OTel)
connectors/
  stint-inmemory         single-JVM transport/state/timer/blob/domain-event bus — the local dev+test path
  stint-aws              S3 (blob) · DynamoDB (state) · SQS/SNS/EventBridge (transport) · SQS-delay (timer)
                         · SQS inbound queue for domain events (fed directly or by an EventBridge rule)
bundles/
  stint-bundle-local     core + in-memory
  stint-bundle-aws       core + AWS
examples/
  stint-example-build-report   "query → stage S3 pointer → format", loaded from build-report.yaml
  stint-example-send-report    schedule + emit + call:remote — strict-vs-permissive loading; emits report-sent
stint-architecture-tests   the single consolidated ArchUnit suite for the whole reactor (test-only)
stint-it                 floci (local AWS emulator) integration tests
```

Package boundaries, in dependency order: `stint-spi ← stint-wire ← stint-core ← stint-dsl`, with
`stint-worker-sdk` and every connector depending on `stint-wire` (never on `stint-core`).

## Toolchain

- **Java 25** (`JAVA_HOME` must point at a JDK 25)
- **Quarkus 3.33.2** (first line supporting JDK 25), **native-ready** from the start
- Maven 3.8+, Docker (for floci ITs)

## Build & run

```bash
# unit tests, incl. the end-to-end local run of build-report (no cloud, no Docker)
mvn test

# run the example app (in-memory bundle), JVM mode — the workflow is loaded from build-report.yaml
mvn -pl examples/stint-example-build-report quarkus:dev
#   POST http://localhost:8080/reports  {"reportQuery":"SELECT * FROM orders","outputFormat":"xlsx"}

# native build of the example
mvn -pl examples/stint-example-build-report -Pnative package

# floci integration tests (needs Docker)
docker compose -f stint-it/docker-compose.floci.yml up -d
mvn -pl stint-it verify
```

The local `BuildReportLocalTest`/`BuildReportGoldenTest` prove the full distributed loop — two remote
tasks, two dispatch→suspend→result cycles, an S3-style pointer threaded between them — in a single JVM,
and that the YAML definition and the equivalent Java model produce identical runtime behaviour.

## Wire contract (the real interop surface)

Workers in any language interoperate by honouring two CloudEvent types and a handful of extensions, all
defined in `stint-spi`'s `StintEvents` and implemented once in `stint-wire`'s `DefaultCloudEventCodec` —
reused by the orchestrator, `stint-worker-sdk` and every connector, so they always agree on the exact envelope:

```
type: io.stintflow.task.invoke.v1   (engine → worker)
type: io.stintflow.task.result.v1   (worker → engine)
extensions: correlationid · workflowinstanceid · taskid · attempt · definition
            chainid · causationid · traceparent · tracestate      (SDD 2.5, optional)
```

- `correlationid` is the correlation of an **attempt**: one dispatch of one task. It matches a result to
  its invoke; it is not a business correlation.
- `chainid` is the business chain: the same value on every event of every workflow along the path (a
  meeting → its minutes → an ADR). An event without one starts a new chain.
- `causationid` is the id of the event that directly caused this one (for work resumed by a timer, the
  timer's id — timer fires travel in the timer connector's own format, not as CloudEvents).
- `traceparent`/`tracestate` are the CloudEvents Distributed Tracing extension (W3C Trace Context).

All four are optional on read: an event in the older format is accepted.

`definition` carries the workflow's `namespace:name:version` — `TaskContext.definition()` on the worker
side is the exact version the running instance started with, not necessarily the latest registered one.

## Tracing and structured logs (SDD 2.5)

- **Spans:** every engine activation and every task execution is one short span that ends before the
  instance suspends — no span stays open across a wait. Engine → worker → engine is parent-child (one
  trace); a resume after a wait (timer, `listen`) or a start by another workflow's fact is a new trace with
  a span *link*. Spans carry identifiers only (instance, chain, cause, workflow, task, attempt, correlation),
  never inputs, outputs or payloads.
- **OpenTelemetry** lives in `stint-otel` only (`spi`, `wire`, `core` and the worker SDK are OTel-free,
  enforced by ArchUnit). Pass `new OpenTelemetryExecutionTracer(openTelemetry)` to the `WorkflowEngine`
  and `WorkerRuntime` constructors; the default is a no-op that forwards the incoming trace context.
- **Logs:** the engine and the worker put `stint.chainId`, `stint.causationId`, `stint.instanceId`,
  `stint.workflow`, `stint.taskId`, `stint.correlationId`, `stint.attempt`, `traceId` and `spanId` in the
  SLF4J MDC. The Quarkus bundles and examples log JSON (`quarkus-logging-json`), which includes the MDC.
  Outside Quarkus, add any SLF4J 2 binding whose MDC works (Logback, Log4j 2, or `slf4j-jdk14`) —
  `slf4j-simple`'s MDC is a no-op.

## Starting workflows from domain events (SDD 2.1)

Workflows don't call each other: they publish facts as CloudEvents and other workflows start when a
fact they're bound to arrives. One consumer per bus (`DomainEventSource`) feeds a `DomainEventRouter`;
a `TriggerBindings` registry maps event filters to definitions (one event may start several):

```java
TriggerBindings bindings = new TriggerBindings(registry);
bindings.bind(new TriggerBinding("invoice-on-order",
        EventFilter.ofType("io.acme.order.placed.v1"), new WorkflowRef("billing", "invoice-order", "1.0.0")));
new DomainEventRouter(List.of(new StartReaction(bindings, engine)))
        .subscribeTo(new SqsDomainEventSource(sqs, inboundQueueUrl)); // or an InMemoryDomainEventBus
```

- **Exactly one instance per (event, definition):** the instance id is derived from the definition and
  the event's `source` + `id`, so creating the instance *is* the idempotency check — a redelivered or
  concurrent duplicate is a no-op.
- **Input:** as in the DSL 1.0 spec, the workflow input is the array of triggering events;
  `$workflow.input[0]` is the event (with every attribute and extension), alongside `$workflow.id` and
  `$workflow.startedAt`.
- **Acknowledgement:** a message is acked only after every effect is recorded. Events matching no
  binding are acked and dropped; bodies that aren't CloudEvents and events bound to a definition this
  engine doesn't know are never acked and end in the queue's DLQ. See `docs/connectors.md` for the
  SQS/EventBridge setup.

## Publishing domain facts with `emit` (SDD 2.2)

`emit` (DSL 1.0) publishes a fact on the **domain channel** — an `EventPublisher`, never the task
transport, so consumers can't couple to the engine's internal protocol:

```yaml
- reportSent:
    emit:
      event:
        with:
          source: https://stintflow.io/reports/send-report
          type: io.stintflow.reports.report-sent.v1
          data: ${ $context }
```

- **Never lost, never out of sync with state:** a fact is written to an outbox in the *same* save as
  the state that produced it, published right after, and removed only once the broker accepted it. A
  periodic sweep (`engine.outboxRelay().start(Duration.ofSeconds(30))`) republishes anything a crash left
  behind.
- **Stable ids:** the author's `id` if declared; otherwise the engine derives one from the instance, the
  state version the step started from, the node and its occurrence — a re-run after a crash emits the
  same id. Delivery is at-least-once: consumers deduplicate by (`source`, `id`), as SDD 2.1 triggers do.
- **Lean events:** a fact larger than the channel allows goes out with the standard CloudEvents
  `dataref` extension pointing into a dedicated facts store; the engine never deletes it.
- `io.stintflow.task.*` and `io.stintflow.timer.*` are reserved: rejected when loading, registering or
  running an `emit`.

## Waiting for correlated events with `listen` (SDD 2.3)

A `listen` task (DSL 1.0 *Listen*) suspends the instance until correlated domain events arrive:

```yaml
- awaitReview:
    try:
      - wait:
          listen:
            to:
              all:                     # also: one (a single filter) / any
                - with: { type: io.stintflow.review.completed.v1 }
                  correlate:
                    instance: { from: '${ .data.instanceId }', expect: '${ $workflow.id }' }
                - with: { type: io.stintflow.billing.approved.v1 }
                  correlate:
                    instance: { from: '${ .data.instanceId }', expect: '${ $workflow.id }' }
            read: data                 # data (default) | envelope | raw
          timeout: { after: { minutes: 10 } }
    catch:
      errors: { with: { type: 'stint://errors/timeout' } }
```

- **Correlation:** each `expect` is evaluated when the instance suspends and each `from` on the arriving
  event; the instance waits on the key `event:<type>:<shape>:<hash of the values>`, so several `correlate`
  entries are an AND. The registry indexes every `listen`'s correlation shape by event type, so an arriving
  event costs one key lookup per shape. A key belongs to one instance: a second instance waiting on the
  same key fails with `correlation-conflict`.
- **Output:** an array of the consumed events, in the order they were consumed; `$workflow` is unchanged.
  The resume's cause is the event (the timer on a timeout); the instance keeps its own chain.
- **`all`/`any`:** `all` records each event as it arrives (concurrent arrivals are serialized by the
  versioned save) and resumes on the last; `any`/`one` resume on the first and end the other waits.
- **Early events:** an `emit` followed by a `listen` saves the wait with the request, and the request is
  published only after — even an instant answer finds the wait. Any other event that arrives before its
  wait is kept in an inbox for `ResumeReaction`'s window (default 5 min, max 15) and consumed when the
  instance suspends.
- **Timeout:** `timeout.after` must fit the timer (SQS: 15 min) until SDD 2.4; it raises
  `stint://errors/timeout`, catchable by `try/catch` (no `retry` around a `listen`).
- **Not yet:** `until`, `foreach`, `any: []`, expressions/regex in `with`, a filter without `correlate` or a
  correlation without `expect`, one event resuming several instances.

Wire it next to `StartReaction`: `new DomainEventRouter(List.of(new StartReaction(bindings, engine),
new ResumeReaction(engine)))`.

## Honest MVP cuts (deliberate, documented)

1. **DSL Fase 2 constructs**: `schedule`, `fork`, `wait`, `for` and call types other
   than the `remote` Stint extension are valid CNCF DSL 1.0 but not implemented yet (event-started
   workflows exist programmatically via `TriggerBindings`; reading `schedule.on` from YAML is SDD 2.4;
   `emit` is implemented since SDD 2.2, `listen` — the subset above — since SDD 2.3) — `stint-dsl` fails
   the load citing the construct and its JSON Pointer, unless loaded permissively (see
   `stint-example-send-report`).
2. **AWS transports**: SQS is the default (native + floci-testable). SNS and EventBridge are included
   as `@Alternative` connectors (opt-in). EventBridge uses the raw AWS SDK → **native caveat**: it
   needs reflection registration for native image; the SQS/S3/DynamoDB path is fully native via
   Quarkus extensions.
3. **EventBridge Scheduler timer** (delays > 15 min) and the **Lambda/Knative worker bindings** are
   mapped in `docs/connectors.md` but not yet coded.
4. **`WorkflowEngine.awaitCompletion`** polls the `StateStore` — fine for tests and one-off synchronous
   calls, not designed for high-volume production use. It also can't report *why* an instance failed,
   only that it did: `InstanceSnapshot` doesn't persist a failure reason, only the terminal status.

## floci

[floci](https://github.com/floci-io/floci) is a lightweight local AWS emulator (≈90 MB, ~24 ms start)
exposing AWS-shaped services on `http://localhost:4566`. Connectors point at it via
`stint.aws.endpoint-override` (raw clients) and the standard `quarkus.<svc>.endpoint-override` (Quarkus
extensions). It replaces LocalStack for CI and local dev.
