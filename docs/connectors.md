# Connectors

Each **port** (abstraction) is a socket; each **connector** is a plug. The core only knows the socket.
🟢 = implemented in this MVP · ⚪ = SPI-ready, roadmap.

## TaskTransport — dispatch invoke + receive result (CloudEvents)
| Ecosystem | Connector | MVP |
|---|---|---|
| Local | in-memory (same JVM) | 🟢 |
| AWS | SQS (default) | 🟢 |
| AWS | SNS (fan-out, `@Alternative`) | 🟢 |
| AWS | EventBridge (`@Alternative`, raw SDK) | 🟢 |
| Generic | Kafka · NATS · RabbitMQ | ⚪ |
| K8s | Knative Eventing | ⚪ |
| Azure / GCP | Service Bus · Pub/Sub | ⚪ |

## StateStore — checkpoint + correlation index
| Ecosystem | Connector | MVP |
|---|---|---|
| Local | in-memory | 🟢 |
| AWS | DynamoDB (`stint-instances` + `stint-waits` + `stint-outbox`) | 🟢 |
| Generic | Postgres · Redis · Mongo | ⚪ |
| Azure / GCP | Cosmos · Firestore | ⚪ |

## TimerService — timeouts / delays
| Ecosystem | Connector | MVP |
|---|---|---|
| Local | in-memory | 🟢 |
| AWS | SQS DelaySeconds (≤ 15 min) | 🟢 |
| AWS | EventBridge Scheduler (long delays) | ⚪ |
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
| Valid event, no binding matches | logged at INFO, dropped | yes |
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
