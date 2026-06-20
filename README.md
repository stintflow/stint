# Stint

> Cloud-agnostic, distributed, serverless execution engine for **CNCF Serverless Workflows**.
> AWS-first in practice, cloud-blind by design.

A *stint* is the run a driver does between pit stops — a bounded, ephemeral segment of work. That is exactly
a worker here: it spins up, runs its leg, emits a result, and disappears. Workflows are the relay; tasks are
the legs; the correlation id is the baton.

- **GitHub org:** `stintflow` (github.com/stintflow)
- **Maven groupId:** `io.github.stintflow`
- **Java package:** `io.stintflow`

> The repo is not git-initialised yet.

## The empty quadrant

| Engine | Topology | Cloud coupling |
|---|---|---|
| Quarkus Flow | in-process orchestrator | agnostic, but single-JVM |
| SonataFlow / Kogito | decomposed services | K8s / Knative committed |
| **Stint (this)** | **distributed, per-task workers** | **transport/cloud-agnostic** |

Same CNCF DSL as the others; opposite runtime. The orchestrator dispatches each task over a transport and
**suspends**; an ephemeral worker (Lambda / Knative / pool) runs it and emits a result that resumes the
instance. The engine never imports a cloud SDK — that invariant is enforced by an ArchUnit test.

## Module map

```
stint-spi          ports + domain + CloudEvents wire contract   (ZERO cloud deps)
stint-core         the cloud-blind orchestrator: dispatch / suspend / resume / claim-check
stint-worker-sdk   uniform TaskHandler programming model
connectors/
  stint-inmemory   single-JVM transport/state/timer/blob — the local dev+test path
  stint-aws        S3 (blob) · DynamoDB (state) · SQS/SNS/EventBridge (transport) · SQS-delay (timer)
bundles/
  stint-bundle-local   core + in-memory
  stint-bundle-aws     core + AWS
examples/
  stint-example-build-report   the recurring "query → stage S3 pointer → format" workflow
stint-it           floci (local AWS emulator) integration tests
```

## Toolchain

- **Java 25** (`JAVA_HOME` must point at a JDK 25)
- **Quarkus 3.33.2** (first line supporting JDK 25), **native-ready** from the start
- Maven 3.8+, Docker (for floci ITs)

## Build & run

```bash
# unit tests, incl. the end-to-end local run of build-report (no cloud, no Docker)
mvn test

# run the example app (in-memory bundle), JVM mode
mvn -pl examples/stint-example-build-report quarkus:dev
#   POST http://localhost:8080/reports  {"reportQuery":"SELECT * FROM orders","outputFormat":"xlsx"}

# native build of the example
mvn -pl examples/stint-example-build-report -Pnative package

# floci smoke test (needs Docker): remove @Disabled in FlociSmokeIT, then
docker compose -f stint-it/docker-compose.floci.yml up -d
mvn -pl stint-it test
```

The local `BuildReportLocalTest` proves the full distributed loop — two remote tasks, two
dispatch→suspend→result cycles, an S3-style pointer threaded between them — in a single JVM.

## Wire contract (the real interop surface)

Workers in any language interoperate by honouring three CloudEvent types and four extensions:

```
type: io.stintflow.task.invoke.v1   (engine → worker)
type: io.stintflow.task.result.v1   (worker → engine)
type: io.stintflow.timer.fire.v1    (timer → engine)
extensions: correlationid · workflowinstanceid · taskid · attempt
```

## Honest MVP cuts (deliberate, documented)

1. **DSL front-end**: the engine runs an internal Java model (`BuildReport.java`); parsing the CNCF
   YAML (`build-report.yaml`) into it is a planned increment. The wire contract is unaffected.
2. **Timeout fire→retry**: timers are armed on dispatch and cancelled on result, but the *fire* path
   (retry/compensate) needs a `TimerService.onFire` port — next increment.
3. **AWS transports**: SQS is the default (native + floci-testable). SNS and EventBridge are included
   as `@Alternative` connectors (opt-in). EventBridge uses the raw AWS SDK → **native caveat**: it
   needs reflection registration for native image; the SQS/S3/DynamoDB path is fully native via
   Quarkus extensions.
4. **EventBridge Scheduler timer** (delays > 15 min) and the **Lambda/Knative worker bindings** are
   mapped in `docs/connectors.md` but not yet coded.

## floci

[floci](https://github.com/floci-io/floci) is a lightweight local AWS emulator (≈90 MB, ~24 ms start)
exposing AWS-shaped services on `http://localhost:4566`. Connectors point at it via
`stint.aws.endpoint-override` (raw clients) and the standard `quarkus.<svc>.endpoint-override` (Quarkus
extensions). It replaces LocalStack for CI and local dev.
