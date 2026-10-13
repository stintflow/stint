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
  stint-example-send-report    schedule.cron + emit + call:remote — publishes report-sent (SDD 2.6 milestone)
  stint-example-archive-report schedule.on report-sent → archive record; independent of send-report (SDD 2.6)
stint-architecture-tests   the single consolidated ArchUnit suite for the whole reactor (test-only)
stint-it                 floci (local AWS emulator) integration tests
```

Package boundaries, in dependency order: `stint-spi ← stint-wire ← stint-core ← stint-dsl`, with
`stint-worker-sdk` and every connector depending on `stint-wire` (never on `stint-core`).

## Toolchain

- **Java 25** (`JAVA_HOME` must point at a JDK 25)
- **Quarkus 3.33.2** (first line supporting JDK 25), **native-ready** from the start
  (jq expressions, builtin functions included, need no native configuration in the consuming app: `stint-core`
  carries the metadata, SDD 2.10)
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

### Event types at a glance

| Type | Channel | Who emits it | Notes |
|---|---|---|---|
| `io.stintflow.task.invoke.v1` / `io.stintflow.task.result.v1` | task transport | engine / worker | internal protocol |
| timer fire `{timerId, workflowInstanceId}` | timer queue | SQS delay or EventBridge Scheduler | **not** a CloudEvent (SDD 1.3/2.4) |
| `io.stintflow.schedule.fired.v1` | none (in-process) | the schedule consumer | synthetic start event of a cron occurrence; id `<schedule>@<scheduled instant>` (SDD 2.4) |
| any other type, e.g. `io.stintflow.reports.report-sent.v1` | domain channel (EventBridge domain bus / SNS) | an `emit` task | a **domain fact**; workflows react through `schedule.on` or `listen` |

The prefixes `io.stintflow.task.`, `io.stintflow.timer.` and `io.stintflow.schedule.` are reserved: an `emit`
using them is rejected. Domain facts carry the same lineage extensions (`chainid`, `causationid`,
`traceparent`, `tracestate`) plus `dataref` when their data went to the facts store. A `listen` correlates on
values it extracts from the fact (`correlate.from`) — no Stint-specific correlation extension is needed on the
fact itself.

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
      errors: { with: { type: 'https://serverlessworkflow.io/spec/1.0.0/errors/timeout' } }
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
  the DSL 1.0 standard `https://serverlessworkflow.io/spec/1.0.0/errors/timeout` (status 408), catchable by
  `try/catch` (no `retry` around a `listen`).
- **Not yet:** `until`, `foreach`, `any: []`, expressions/regex in `with`, a filter without `correlate` or a
  correlation without `expect`, one event resuming several instances.

Wire it next to `StartReaction`: `new DomainEventRouter(List.of(new StartReaction(bindings, engine),
new ResumeReaction(engine)))`.

## Schedules and long timers (SDD 2.4)

```yaml
document:
  metadata:
    stint.schedule.timezone: America/Sao_Paulo   # Stint extension; UTC by default
schedule:
  cron: '0 7 * * 1-5'          # or every: { minutes: 15 }, or on: { one: { with: { type: ... } } }
```

- `schedule.cron`/`every` become one EventBridge Scheduler schedule per definition (in-memory: `tick()` on a
  controllable clock). Every replica reconciles them create-only at startup; `prune()` is an explicit deploy
  step.
- An occurrence starts the definition through the idempotent start of SDD 2.1, with a synthetic
  `io.stintflow.schedule.fired.v1` event whose id is `<schedule>@<scheduled instant>`. A duplicated delivery
  creates nothing, and each run starts a new chain.
- `schedule.on` (`one`/`any`, exact `with`) binds through `TriggerBindings.bindDeclaredSchedules()`.
- Timers beyond SQS's 15 minutes (a 48 h `listen`, a long retry) go through `DelayRoutingTimerService` to
  Scheduler one-time schedules. These are deleted after firing and deliver the same timer JSON to the same
  queue.
- **SDD 2.9:** SQS delays are rounded up to whole seconds. A timer that fires before the save of its wait is
  delivered again instead of being dropped; the timer JSON carries `armedAt`, and the engine logs every fire that
  finds no wait as `early`, `obsolete` or `obsolete-after-window` (`docs/connectors.md`, "Fires that arrive before
  their wait").
- **Not yet:** `schedule.after`, `schedule.on` with `all`/`until`/`correlate`, input for scheduled runs, catching
  up missed occurrences.

## Milestone: send-report → report-sent → archive-report (SDD 2.6)

Two workflows that only know each other through a fact:

- `examples/stint-example-send-report` — `send-report.yaml` runs on `schedule.cron: '0 9 * * MON'` (UTC).
  Its `prepare` task falls back to default recipients on a cron run, which has no input of its own. It mails
  through `MailGateway` and emits `io.stintflow.reports.report-sent.v1`.
- `examples/stint-example-archive-report` — `archive-report.yaml` starts on `schedule.on` for that type and
  stores an archive record (with the chain id) in the `BlobStore`. Its build bans any dependency on
  send-report, and a test fails if its sources mention it.

The archive instance runs on the chain of the report that caused it. If an engine dies between the checkpoint
and the publication, the outbox sweep of another engine still delivers the fact.

- **Locally:** `SendReportMilestoneLocalTest`, with a controllable clock and in-memory everything.
- **On floci:** `FlociSendReportMilestoneIT`, with real SQS, EventBridge, EventBridge Scheduler, DynamoDB and
  S3, and real SMTP captured by Mailpit. Timers stay in-memory there, as in every IT.

## Errors (SDD 2.8)

Errors raised by Stint follow DSL 1.0:
- a **standard type** (`https://serverlessworkflow.io/spec/1.0.0/errors/<timeout|runtime|communication|…>`);
- its **status** (timeout 408, runtime 500, …);
- the failing component's JSON Pointer as **instance**.

Workers get the same from `stint-worker-sdk`; a worker's own `status`/`instance` is kept. The `stint://errors/*`
types of earlier versions are still accepted as **aliases**: on the wire, from older workers, and in `catch`
filters written in YAML. That compatibility is temporary.

## Decisions with `call: decision` (SDD 3a.1)

A decision (classify, score, yes/no) is a **remote task** answered by a decision worker; the engine never talks to
a model provider. `call: decision` is a Stint extension, compiled to a `call: remote` followed by a `switch`:

```yaml
- classifyItem:
    call: decision
    with:
      task: decide-minute-item           # routing key of the decision worker
      kind: choice                       # choice | score | boolean
      input: ${ { text: .redactedText } }
      options:
        - approve: { description: "Approved in the meeting", then: recordApproval }
        - reject:  { description: "Rejected in the meeting" }   # no then: the task's then
    onUncertain: humanReview             # optional; without it, an uncertain decision follows its value
    timeout: { after: { seconds: 60 } }
```

- The contract (`io.stintflow.wire.decision`): the worker receives `{kind, options, input}` and returns
  `{value, confidence, uncertain, provenance: {adapter, provider, model, version}}`.
- An output out of contract is the standard `validation` error (400), raised at the task and catchable in a `try`.
- The provenance of the first result applied is kept in the snapshot (`InstanceSnapshot.decisions`, by pointer);
  a duplicate or late result never replaces it. Nothing of the decision's input goes to logs, spans or the record.
- The expansion is logged at DEBUG by `DslLoader`; `DefinitionPrinter` prints any compiled definition as YAML.

## Decision adapters (SDD 3a.2)

A `call: decision` names a routing key (`with.task`); the worker's configuration says which **adapter** answers it, with
which question and threshold. The workflow never names a provider, a threshold or a credential.

```java
DecisionHandlers.builder(DecisionConfig.fromProperties(props))   // fails on any invalid key, at startup
        .adapter(LlmDecisionAdapter.builder().id("llm").chatModel(model).build())
        .adapter(JevDecisionAdapter.builder().id("jev").apiKey(secrets::jevKey).build())
        .build()
        .registerAll(registry);
```

```properties
stint.decision.routes.classify-meeting.adapter=llm
stint.decision.routes.classify-meeting.instructions=Which document does this meeting produce?
stint.decision.adapters.llm.threshold=0.8              # default 0.7; also threshold.<choice|score|boolean>
stint.decision.adapters.llm.max-input-tokens=20000     # optional ceiling, checked before the call
stint.decision.adapters.llm.price.input-per-million=0.15   # optional, for the estimated cost in the usage line
```

- **`uncertain`** is computed by the handler, never by the adapter: no confidence, a tie at the top of the adapter's
  distribution, or a (calibrated) confidence below the threshold. Equal to the threshold is certain. A
  `ConfidenceCalibration` per adapter maps the raw confidence before the threshold.
- **Errors**: a malformed answer — not JSON, the wrong type, a confidence outside [0, 1], or a value that is not one of
  the options — is a `validation` error (400) a `catch` can handle. A low confidence is never an error. Provider
  failures map as for every LLM worker (429 with `retryAfter`, `timeout`, `communication`, `authentication`).
- **Adapters**:
  - `FakeDecisionAdapter` (module `stint-worker-decision`): rules for tests and CI;
  - `LlmDecisionAdapter` (`stint-worker-langchain4j`): one call, the model's own confidence (not calibrated, valid only
    for that adapter, model and prompt version, which goes into the provenance), a JSON Schema with the options as an
    enum when the model supports it;
  - `JevDecisionAdapter` (`stint-worker-decision-jev`): TypeSafe AI's Jev over its HTTP API with the JDK's
    `HttpClient`; `Retry-After` of a 429 is honoured. **Tested only against a fake server.**
- **Usage**: one `Decision usage` line per decision (adapter, provider, model, tokens, latency, estimated cost), with
  the task's ids and no content. A `DecisionPolicy` (SDD 3a.5) is consulted before every call; it allows all by default.
- **No real providers in tests**: every test JVM runs with `-Djdk.net.hosts.file=build/test-hosts`, which resolves only
  `localhost`; `NoRealProvidersTest` fails if a provider key is set.

### Local decisions with LitJev (SDD 3a.2b)

The same `JevDecisionAdapter` serves a local [LitJev](https://github.com/zhengxuyu/litjev) server — an independent,
Apache-2.0 reproduction of Jev's API over Qwen checkpoints — so a decision's input never leaves the machine:

```java
JevDecisionAdapter.builder().server(JevDecisionAdapter.Server.LITJEV)   // http://127.0.0.1:8000, id and model "litjev"
        .model("Qwen/Qwen3.5-2B").serverVersion("2cd915a").readout("content")   // optional, for the provenance
        .build();
```

```properties
stint.decision.adapters.litjev.threshold=0.7                 # its own threshold: never the hosted Jev's
stint.decision.adapters.litjev.calibration.temperature=1.5   # optional temperature scaling, any adapter
```

- **Profile**: no key and no `Authorization`; on a loopback address the descriptor is `remote = false` and
  `provider = litjev`, for the sensitivity policy (SDD 3a.5). A non-loopback address fails at build time unless
  `allowRemote(true)`, which logs a warning and makes the adapter remote. The default stays the hosted Jev, with a key.
- **Confidence**: `ConfidenceSource.MAX_PROBABILITY` by default for LitJev — the chance of the chosen option — rather
  than its `confidence`, a Gini concentration that depends on the number of options; `SERVER` takes the latter (and is
  the hosted Jev's default). A boolean is always `max(p, 1 − p)`. LitJev is not calibrated: tune its threshold, or a
  temperature, on its own data.
- **No input**: `null`, blank text, `{}` or `[]` is a `validation` error before any call, for both servers.
- **Booleans**: without declared options, the local profile asks a plain `Yes`/`No` (LitJev needs both descriptions);
  declared `true`/`false` options send their own descriptions.
- **Provenance**: `provider` `litjev`, the checkpoint the server reports as `model`, and a version like
  `jev-adapter/1;server=litjev@2cd915a;readout=content`.
- **Manual run**: [`docs/manual/local-decision.md`](docs/manual/local-decision.md) runs the example's `triage` mode
  against a real LitJev with 15 synthetic pt-BR meetings. The build never needs a LitJev or a model.

## LLM workers with `stint-worker-langchain4j` (SDD 3a.3)

An LLM task is an ordinary worker task: the engine dispatches it, retries it and routes its errors like any other,
and never sees a model. `stint-worker-langchain4j` builds the `TaskHandler` from a LangChain4j AI Service (plain
LangChain4j, version from the platform's `quarkus-langchain4j-bom`: 1.11.7 on Quarkus 3.33.2):

```java
LangChain4jWorker ai = LangChain4jWorker.builder()
        .chatModel(model)                       // the provider, built by the worker — with maxRetries(0)
        .conversations(store, blobs)            // optional: the instance's conversation memory
        .build();
registry.register("summarize-meeting", ai.handler(MeetingAssistant.class,
        (assistant, task) -> assistant.summarize(task.input().path("transcript").asText())));
```

- **Output**: a record or POJO becomes an object in the task's output; a `String`, a string. An answer that
  doesn't fit the type, or fails `.validate(...)`, is a `validation` error (400) a `catch` can handle.
- **Memory**: every task of an instance sees the instance's conversation (`MemoryScope.TASK` gives a task its own,
  `NONE` none). Each execution commits its messages once, when it succeeds, keyed by its correlationId: a
  redelivered message gets the committed output back without calling the model, and a retry sees only what
  earlier executions committed. The port is `ConversationStore`: `InMemoryConversationStore` for tests, and
  `DynamoDbConversationStore` (module `connectors/stint-aws-conversations`; table with `expiresAt` as TTL, 30 days by
  default). Exchanges over 64 KB go to the `BlobStore` by pointer. `ai.forget(instanceId)` deletes everything an
  instance remembers.
- **Order and abandoned attempts** (SDD 3a.3b, 3a.3c): an exchange is committed strictly after everything its
  execution read, so the conversation keeps the steps' order whatever the clocks say. Each dispatch carries its
  attempt's deadline (the `stintdeadline` extension, `TaskContext.deadline()`): a delivery past it doesn't call the
  model, and an execution ending inside `commitMargin` (5 s) of it doesn't commit — the engine discards that result
  anyway. A step never reads an earlier attempt of the same pass through a task, if a later one exists.
- **Provider errors** become standard errors, with a fixed `detail` that never quotes the provider's message:
  429 is `communication`/429 (with `retryAfter` from `.retryAfter(...)` or `.rateLimitRetryAfter(...)` — LangChain4j
  doesn't expose the provider's header), timeouts are `timeout`/408, 5xx and connection failures are
  `communication`, bad credentials are `authentication`/401, a bad request is `configuration`. Retrying is the
  workflow's `catch.retry`: catch `communication` and `timeout`, not `authentication`.
- **No content in telemetry**: each execution logs one `LLM usage` line (model, tokens, calls, tool calls, latency)
  with the task's ids, and each model call is a `stint.llm` span with ids only. Prompts and answers are never logged
  or traced; a `ContentPolicy` (SDD 3a.5) is the only way to hand them to a sink. Keep the providers'
  `logRequests`/`logResponses` off.
- **Tools**: a tool that may take long (over `toolCallLimit`, 5 s by default, logged as a WARN), needs approval or has
  external effects with their own retries is a task of the workflow, not a tool call.
- **Native**: annotate each AI Service interface with `@RegisterForProxy` and `@RegisterForReflection(methods = true)`
  and its output records with `@RegisterForReflection`; the module registers LangChain4j's message types itself.
  `examples/stint-example-ai-worker` runs two steps sharing the memory, as a jar and as a native image.
- **Concurrency**: `WorkerPool` (in `stint-worker-sdk`) runs up to N deliveries at once on virtual threads and
  acknowledges each one only after its result was shipped, so a slow task doesn't hold the others.

## Honest MVP cuts (deliberate, documented)

1. **DSL Fase 2 constructs**: `fork`, `wait`, `for` and call types other than the `remote` Stint
   extension are valid CNCF DSL 1.0 but not implemented yet (`emit` is implemented since SDD 2.2,
   `listen` — the subset above — since SDD 2.3, `schedule` — cron/every/on — since SDD 2.4) — `stint-dsl` fails
   the load citing the construct and its JSON Pointer, unless loaded permissively (see
   `stint-example-send-report`). Since SDD 2.9 this holds for **every declared key**: a key the compiler doesn't
   use fails a `STRICT` load with its path, so nothing in a definition is ignored in silence. Examples are `if`,
   `use`, schemas, `timeout` on a `set`, `then` on a `try`'s body, `catch.as` and `retry.jitter`. `PERMISSIVE`
   warns instead. The informative DSL keys (`title`, `summary`, `tags`, `metadata`) are accepted.
   A `call: remote` sends its worker `with.input` evaluated over the task's input after `input.from` (DSL 1.0:
   input transformation, then the call's `with`). This holds inside a `try` too, and each retry sends exactly what
   attempt 1 sent.
2. **AWS transports**: SQS is the default (native + floci-testable). SNS and EventBridge are included
   as `@Alternative` connectors (opt-in). Every AWS client — S3, DynamoDB, SQS, SNS, EventBridge and
   EventBridge Scheduler — comes from its Quarkus extension (SDD 2.7). The `stint-example-aws-smoke` app runs
   them end to end over floci, as a JVM jar in `it.yml` and as a native image in the on-demand `native.yml`.
3. The **Lambda/Knative worker bindings** are mapped in `docs/connectors.md` but not yet coded (the
   EventBridge Scheduler timer exists since SDD 2.4).
4. **`WorkflowEngine.awaitCompletion`** polls the `StateStore` — fine for tests and one-off synchronous
   calls, not designed for high-volume production use. It also can't report *why* an instance failed,
   only that it did: `InstanceSnapshot` doesn't persist a failure reason, only the terminal status.

## floci

[floci](https://github.com/floci-io/floci) is a lightweight local AWS emulator (≈90 MB, ~24 ms start)
exposing AWS-shaped services on `http://localhost:4566`. Connectors point at it via the standard
`quarkus.<svc>.endpoint-override` of each Quarkus extension (`sqs`, `dynamodb`, `s3`, `sns`, `eventbridge`,
`eventbridge-scheduler` — not `scheduler`, which is Quarkus's own scheduling extension). It replaces
LocalStack for CI and local dev.
