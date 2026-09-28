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
| AWS | DynamoDB (+ `waitingFor-index` GSI) | 🟢 |
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

## TriggerSource — ingress
| Ecosystem | Connector | MVP |
|---|---|---|
| Local | HTTP (REST resource) | 🟢 |
| AWS | API Gateway · EventBridge rule · SQS | ⚪ |
| K8s | Knative Service / source | ⚪ |

## Worker bindings (worker-side runtime)
| Platform | Binding | MVP |
|---|---|---|
| Local/pool | direct (`WorkerRuntime.handle`) | 🟢 |
| AWS Lambda | `RequestHandler` shell → `WorkerRuntime` | ⚪ |
| Knative | HTTP CloudEvents receiver → `WorkerRuntime` | ⚪ |

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
