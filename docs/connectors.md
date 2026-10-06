# Connectors

Each **port** (abstraction) is a socket; each **connector** is a plug. The core only knows the socket.
🟢 = implemented in this MVP · ⚪ = SPI-ready, roadmap.

## TaskTransport — dispatch invoke + receive result (CloudEvents)
| Ecosystem | Connector | MVP |
|---|---|---|
| Local | in-memory (same JVM) | 🟢 |
| AWS | SQS (default) | 🟢 |
| AWS | SNS (fan-out, `@Alternative`) | 🟢 |
| AWS | EventBridge (`@Alternative`, `quarkus-amazon-eventbridge`) | 🟢 |
| Generic | Kafka · NATS · RabbitMQ | ⚪ |
| K8s | Knative Eventing | ⚪ |
| Azure / GCP | Service Bus · Pub/Sub | ⚪ |

## StateStore — checkpoint + correlation index
| Ecosystem | Connector | MVP |
|---|---|---|
| Local | in-memory | 🟢 |
| AWS | DynamoDB (`stint-instances` + `stint-waits` + `stint-outbox` + `stint-inbox`) | 🟢 |
| Generic | Postgres · Redis · Mongo | ⚪ |
| Azure / GCP | Cosmos · Firestore | ⚪ |

## TimerService — timeouts / delays
| Ecosystem | Connector | MVP |
|---|---|---|
| Local | in-memory | 🟢 |
| AWS | SQS DelaySeconds (≤ 15 min) | 🟢 |
| AWS | EventBridge Scheduler one-time schedules (long delays, SDD 2.4) | 🟢 |

Compose them with `DelayRoutingTimerService(sqsTimers, schedulerTimers, clock)` (in `stint-core`): up to
15 min goes to SQS, longer to the Scheduler; its `maxDelay()` is the Scheduler's (`stint.aws.scheduler.timer-max-delay`,
default 365 days), so register definitions against it. A long timer is an `at(...)` schedule in UTC, **rounded up
to the minute** (never early; fires within ~1 min after `fireAt`), `ActionAfterCompletion=DELETE`, in the group
`stint.aws.scheduler.timer-group` (default `stint-timers`). Its target is the **same timer queue**
(`stint.aws.scheduler.timer-target-arn` = the ARN of `stint.aws.sqs.timer-queue-url`) with the **same JSON**
`{timerId, workflowInstanceId}`, so `SqsDelayTimerService`'s poller fires both. `EventBridgeSchedulerTimerService`
is injectable by its own type only (`@Typed`): `TimerService` injection points keep resolving to SQS.

## ScheduleService — recurring schedules (`schedule.cron` / `every`, SDD 2.4)
| Ecosystem | Connector | MVP |
|---|---|---|
| Local | in-memory (controllable clock, `tick()`) | 🟢 |
| AWS | EventBridge Scheduler (`cron(...)` + `ScheduleExpressionTimezone`) | 🟢 |

- **Reconcile at startup on every replica:** `new ScheduleReconciler(registry, schedules).reconcile()`. It is
  create-only and idempotent: an existing name is a no-op, so concurrent replicas converge. Nothing is ever
  removed automatically.
- **Prune explicitly** as a deploy/CLI step, with the deployed version's definitions:
  `reconciler.prune()` deletes the schedules in the group that no registered definition wants.
- **One group per application:** `stint.aws.scheduler.group` (default `stint-schedules`). Schedule names are
  content-addressed (`stint-` + 40 hex of definition | cron | zone), so changing a cron creates a new schedule
  and leaves the old one for `prune()`.
- **Target:** the schedule queue `stint.aws.sqs.schedule-queue-url`, whose ARN goes in
  `stint.aws.scheduler.schedule-target-arn`. The role in `stint.aws.scheduler.role-arn` must allow
  `sqs:SendMessage` on both queues. Wire `schedules.onFire(new ScheduledStart(registry, engine))`.
- **Each occurrence** starts the definition through the SDD 2.1 idempotent start, with a synthetic CloudEvent:
  `type io.stintflow.schedule.fired.v1`, `id <schedule>@<scheduled instant>`. A duplicated delivery is therefore
  a no-op, and every occurrence starts a new chain.
  - AWS substitutes `<aws.scheduler.scheduled-time>` in the input.
  - A delivery without it is a misconfigured `Input`: it is not acked and ends up in the DLQ (SDD 2.7).
  - Only for emulators that don't substitute it (floci), set `stint.aws.scheduler.sent-timestamp-fallback=true`.
    The engine then takes the last occurrence at or before the message's `SentTimestamp`, and rejects a delivery
    half a period or more after it.
- **Cron:** five POSIX fields, translated to AWS's six (`0 7 * * 1-5` → `cron(0 7 ? * 2-6 *)`). The loader rejects
  what has no equivalent: day-of-month and day-of-week both restricted, steps on the day of the week, wrapping
  ranges, macros, `? L W #`. `every` must divide an hour or a day.
- **Time zone:** UTC unless `document.metadata["stint.schedule.timezone"]` (Stint extension, IANA id) says
  otherwise. Daylight saving follows `java.time`: a skipped time fires at the shifted instant, a repeated one
  fires once. AWS documents DST adjustment but not the repeated hour, so verify that case on AWS if it matters.
- **Clients (SDD 2.7):** `quarkus-amazon-eventbridge` and `quarkus-amazon-scheduler` produce the clients, configured
  through `quarkus.eventbridge.*` and `quarkus.eventbridge-scheduler.*` (endpoint override, region, credentials,
  `sync-client.type=url`). Mind the prefix: `quarkus.scheduler.*` belongs to Quarkus's own scheduler and is
  ignored here. A client whose configuration is ignored falls back to dev services, or to the default AWS
  credentials chain.
- **Native:** proven by `stint-example-aws-smoke`. Run
  `mvn -Pnative verify -Dquarkus.native.container-build=true -pl examples/stint-example-aws-smoke -am`; it needs
  Docker, no local GraalVM.
| Generic | DB-poller · Redis timer wheel · Quartz | ⚪ |

## BlobStore — claim-check / large payload offload
| Ecosystem | Connector | MVP |
|---|---|---|
| Local | filesystem | 🟢 |
| AWS | S3 | 🟢 |
| Generic | MinIO (S3-API) | 🟢 (via S3 connector) |
| Azure / GCP | Blob Storage · Cloud Storage | ⚪ |

## EventPublisher — the domain channel (SDD 2.2)
| Ecosystem | Connector | MVP |
|---|---|---|
| Local | in-memory (`InMemoryEventPublisher`) | 🟢 |
| AWS | EventBridge, dedicated bus (`EventBridgeEventPublisher`, default) | 🟢 |
| AWS | SNS, dedicated topic (`SnsEventPublisher`, `@Alternative`) | 🟢 |
| Generic | Kafka · NATS | ⚪ |

Facts (`emit`) never share a destination with engine traffic:

- **EventBridge:** `stint.aws.eventbridge.domain-bus=<bus>` — must differ from
  `stint.aws.eventbridge.bus` (the task bus); the same value fails at startup. `Source` = the fact's
  `source`, `DetailType` = its `type`, `Detail` = the CloudEvent (structured JSON). Consumers: a rule
  with an SQS target and `InputPath: "$.detail"` (the SDD 2.1 setup below).
- **SNS:** `stint.aws.sns.domain-topic-arn=<arn>` — must differ from `stint.aws.sns.topic-arn`. The
  message is the CloudEvent; a `type` message attribute enables `FilterPolicy`. SQS subscriptions should
  set `RawMessageDelivery=true` so the queue gets the bare CloudEvent.

### Outbox (DynamoDB `stint-outbox`)
Facts are written in the same `TransactWriteItems` as the state that produced them, published right
after, and deleted once the broker accepted them. Nothing pending is ever expired (no TTL).

- Table `stint.aws.dynamodb.outbox-table` (default `stint-outbox`): PK `eventId` (S).
- GSI `pending-by-age`: PK `shard` (S), SK `createdAt` (N), projection ALL. Shards:
  `stint.aws.dynamodb.outbox-shards` (default 4). The sweep queries each shard — never a scan — and only
  pending facts are in the table.
- Run the sweep in production: `engine.outboxRelay().start(Duration.ofSeconds(30))`. A fact pending for
  more than 15 minutes is logged at ERROR on every sweep.
- At most 25 facts per workflow step (DynamoDB transactions hold 100 items).

### Early-event inbox (DynamoDB `stint-inbox`, SDD 2.3)
A domain event that arrives before the `listen` waiting for it is kept here for a short window and consumed
when the instance suspends; the save that consumes it deletes it in the same transaction.

- Table `stint.aws.dynamodb.inbox-table` (default `stint-inbox`): PK `waitKey` (S), SK `eventId` (S).
- `expiresAt` (N, epoch seconds) can be enabled as the table's TTL attribute to clean up; reads ignore
  expired entries on their own (DynamoDB deletes lazily). Reads are strongly consistent.
- The window is `ResumeReaction`'s (default 5 min, at most 15 min).

### Facts store (large payloads → `dataref`)
When a fact is larger than the channel allows (≈256 KB), its `data` goes to a dedicated bucket and the
event carries the standard CloudEvents `dataref` extension (`s3://<facts-bucket>/facts/<ns>/<name>/<id>`).

- `stint.aws.s3.facts-bucket=<bucket>` — separate from `stint.aws.s3.bucket` (internal claim-check).
- The engine never deletes facts: add a lifecycle rule (≥ 30 days recommended — longer than any
  consumer's replay window, e.g. 14-day queue + 14-day DLQ).
- Grant the engine `s3:PutObject` on `facts/*`; grant consumers `s3:GetObject` through the bucket policy.

## DomainEventSource — inbound domain events (SDD 2.1; replaces `TriggerSource`)
| Ecosystem | Connector | MVP |
|---|---|---|
| Local | in-memory bus (`InMemoryDomainEventBus`) | 🟢 |
| AWS | SQS inbound queue (`SqsDomainEventSource`) | 🟢 |
| AWS | EventBridge rule → SQS inbound queue (infrastructure, see below) | 🟢 |
| AWS | API Gateway | ⚪ |
| K8s | Knative Service / source | ⚪ |

Programmatic starts (e.g. the example's HTTP resource calling `engine.start`) don't go through this
port and aren't idempotent.

### Contract
- One consumer per bus: the engine's `DomainEventRouter` starts every definition bound to the event
  (`TriggerBindings`) and, from SDD 2.3, resumes instances waiting in `listen` — never a second
  subscriber on the same queue.
- **Ack only after every effect is recorded** (the handler's stage completed normally). Otherwise the
  message stays for redelivery and, past `maxReceiveCount`, the queue's own redrive moves it to the DLQ.

| Case | What happens | Acked? |
|---|---|---|
| Body isn't a CloudEvent (bad JSON, missing `id`/`source`/`type`/`specversion`) | logged at ERROR | no → DLQ |
| Valid event, no binding matches and no `listen` waits on its type | logged at INFO, dropped | yes |
| Valid event for a `listen` shape, no instance waiting yet | kept in the early-event inbox | yes |
| Binding matches a definition this engine doesn't know (deploy skew) | logged | no → DLQ (redrive back after the deploy) |
| Transient store/transport/timer failure | logged | no → redelivered |
| Instance created, then its local part fails (e.g. bad `input.from`) | instance persisted as `FAILED` | yes |

- **Idempotency:** the instance id is `evt-` + a name-based UUID of (definition, event `source`, event
  `id`); creating it is the conditional first save, so duplicates are no-ops. The dedup window is the
  instance's lifetime — if you ever add instance retention/archival, keep it longer than the inbound
  queue's `MessageRetentionPeriod` (≤ 14 days) and EventBridge's retry window (≤ 24 h). A redelivery
  after the instance was deleted starts a new execution.

### SQS setup (Stint doesn't create queues)
- Inbound queue: `RedrivePolicy {"deadLetterTargetArn": "<dlq-arn>", "maxReceiveCount": "5"}`,
  `VisibilityTimeout` 30 s, `MessageRetentionPeriod` 4 days (default).
- DLQ: `MessageRetentionPeriod` 14 days. Move messages back with `StartMessageMoveTask` once fixed.
- Config: `stint.aws.sqs.domain-events-queue-url=<inbound queue url>`.
- Each message body is one CloudEvent in **structured JSON mode**.

### EventBridge rule → SQS
Point a rule at the inbound queue and set the target's `InputPath` to `$.detail`, so the queue gets the
bare CloudEvent, not the EventBridge envelope (without it the body has no `specversion` and goes to the
DLQ). Publishers put the CloudEvent JSON in `Detail`:

```bash
aws events put-rule --name order-placed \
  --event-pattern '{"source":["com.acme.orders"],"detail-type":["io.acme.order.placed.v1"]}'
aws events put-targets --rule order-placed \
  --targets '[{"Id":"stint-inbound","Arn":"<inbound-queue-arn>","InputPath":"$.detail"}]'
```

The queue policy must allow `events.amazonaws.com` to `sqs:SendMessage` (not needed on floci).
Verified end to end on floci by `FlociDomainEventTriggerIT`.

## Worker bindings (worker-side runtime)
| Platform | Binding | MVP |
|---|---|---|
| Local/pool | direct (`WorkerRuntime.handle`) | 🟢 |
| AWS Lambda | `RequestHandler` shell → `WorkerRuntime` | ⚪ |
| Knative | HTTP CloudEvents receiver → `WorkerRuntime` | ⚪ |

### Lineage on the wire (SDD 2.5)
Every transport carries the optional `chainid`, `causationid`, `traceparent` and `tracestate` extensions
as-is (they're CloudEvent attributes: SQS/SNS/EventBridge bodies keep them). A custom connector must not
drop unknown extensions. Timer connectors keep their own format (`{timerId, workflowInstanceId}`): the
engine takes the chain of a timer-resumed instance from its snapshot.

## Capability honesty
Every transport declares an `AdapterCapabilities` so the core adapts instead of assuming uniformity:
payload > `maxPayloadBytes` → claim-check via `BlobStore`; no `dedupSupported` → core dedups by
`correlationId + attempt`; no `nativeDelaySupported` → fall back to the `TimerService`.

| Transport | Delivery | Ordered | Max payload | Native delay | Dedup |
|---|---|---|---|---|---|
| in-memory | exactly-once | yes | ∞ | yes | yes |
| SQS standard | at-least-once | no | 256 KB | 15 min | no |
| SNS→SQS | at-least-once | no | 256 KB | no | no |
| EventBridge | at-least-once | no | 256 KB | no | no |

## Writing your own connector: package migration (SDD 1.5)

If you built a connector against `Json`, `CeWire`, `DefaultCloudEventCodec` or `ClaimCheck` before SDD
1.5, they moved from `io.stintflow.core` to `io.stintflow.wire` (new module `stint-wire`). No behaviour
changed — only the import and the Maven dependency:

```diff
- import io.stintflow.core.Json;
- import io.stintflow.core.CeWire;
- import io.stintflow.core.DefaultCloudEventCodec;
- import io.stintflow.core.ClaimCheck;
+ import io.stintflow.wire.Json;
+ import io.stintflow.wire.CeWire;
+ import io.stintflow.wire.DefaultCloudEventCodec;
+ import io.stintflow.wire.ClaimCheck;
```

```diff
  <dependency>
      <groupId>io.github.stintflow</groupId>
-     <artifactId>stint-core</artifactId>
+     <artifactId>stint-wire</artifactId>
  </dependency>
```

A connector only needs `stint-core` if it also touches the tree model (`WorkflowDefinition`, the
interpreter, etc.) directly — the four wire classes above never required it, and neither did any
shipped connector. This is a breaking API change, acceptable pre-1.0 (no published version depended on
the old package).

## Consuming `stint-aws` as an external dependency

Found while validating Stint as a plain external library (not part of any SDD's registered scope):

- **CDI discovery — fixed in the library.** `stint-aws` now ships a build-time `META-INF/jandex.idx`
  (`jandex-maven-plugin`), so its `@ApplicationScoped` classes (`S3BlobStore`, `DynamoDbStateStore`,
  `Sqs/Sns/EventBridgeTaskTransport`, `SqsDelayTimerService`) are auto-discovered as CDI beans by any
  Quarkus consumer, exactly as they are inside the Stint reactor. No consumer-side
  `quarkus.index-dependency.*` workaround needed.
- **S3 path-style access — consumer's own config, not something the library should force.** Pointing
  `S3Client` at floci/LocalStack needs `quarkus.s3.path-style-access=true` (the default
  virtual-hosted-style addressing tries to resolve `<bucket>.<host>`, which doesn't exist for a local
  emulator). This is deliberately **not** hardcoded in `stint-aws`: real AWS S3 is moving away from
  path-style, so forcing it in the library would break production consumers to help local testing.
  Set it in your own `application.properties` alongside `quarkus.s3.endpoint-override`:
  ```properties
  quarkus.s3.endpoint-override=http://localhost:4566
  quarkus.s3.path-style-access=true
  quarkus.s3.aws.region=us-east-1
  quarkus.s3.aws.credentials.type=static
  quarkus.s3.aws.credentials.static-provider.access-key-id=test
  quarkus.s3.aws.credentials.static-provider.secret-access-key=test
  ```
