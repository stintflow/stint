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
