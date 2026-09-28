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
connectors/
  stint-inmemory         single-JVM transport/state/timer/blob — the local dev+test path
  stint-aws              S3 (blob) · DynamoDB (state) · SQS/SNS/EventBridge (transport) · SQS-delay (timer)
bundles/
  stint-bundle-local     core + in-memory
  stint-bundle-aws       core + AWS
examples/
  stint-example-build-report   "query → stage S3 pointer → format", loaded from build-report.yaml
  stint-example-send-report    schedule + emit + call:remote — proves strict-vs-permissive DSL loading
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

Workers in any language interoperate by honouring three CloudEvent types and six extensions, all defined
in `stint-spi`'s `StintEvents` and implemented once in `stint-wire`'s `DefaultCloudEventCodec` — reused by
the orchestrator, `stint-worker-sdk` and every connector, so they always agree on the exact envelope:

```
type: io.stintflow.task.invoke.v1   (engine → worker)
type: io.stintflow.task.result.v1   (worker → engine)
type: io.stintflow.timer.fire.v1    (timer → engine)
extensions: correlationid · workflowinstanceid · taskid · attempt · definition · timerid
```

`definition` carries the workflow's `namespace:name:version` — `TaskContext.definition()` on the worker
side is the exact version the running instance started with, not necessarily the latest registered one.

## Honest MVP cuts (deliberate, documented)

1. **DSL Fase 2 constructs**: `schedule`, `emit`, `listen`, `fork`, `wait`, `for` and call types other
   than the `remote` Stint extension are valid CNCF DSL 1.0 but not implemented yet — `stint-dsl` fails
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
