# ADR-001: RabbitMQ as Message Broker

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2024-01-10 |
| **Author** | Platform Engineering |
| **Reviewers** | Backend Engineering |
| **Supersedes** | — |
| **Superseded by** | — |

---

## Context

The system requires an asynchronous message bus to decouple the gateway service (producer) from the messaging service (consumer). The two services run in separate processes and must communicate via a durable broker that survives process restarts on either side.

The primary delivery requirements are:

- **Reliable delivery**: events must not be lost if the consumer is temporarily unavailable
- **Per-message retry delay**: failed deliveries must be rescheduled with exponential backoff, with each message's delay computed individually rather than uniformly across a queue
- **Dead-letter routing**: messages that exhaust their retry budget must be routed to a separate queue for inspection and alerting, without blocking the primary queue
- **Manual acknowledgement**: the consumer must control when a message is acknowledged, not the broker

Secondary requirements include operational visibility into queue depth, consumer count, and message rates without additional tooling beyond the broker itself.

---

## Problem Statement

Selecting a broker involves a fundamental architectural tradeoff: **RabbitMQ's push-based routing model** versus **Kafka's pull-based log model**. The choice affects how retry semantics are implemented, what replay capabilities exist, how fan-out is handled, and what the operational baseline looks like. Choosing the wrong broker for the access pattern requires either significant workarounds or a complete replacement.

---

## Decision

**Use RabbitMQ as the message broker.**

RabbitMQ's exchange-queue-binding topology maps directly to the routing requirements of this system. The dead-letter exchange (DLX) mechanism provides first-class support for nack-based dead-lettering without application-level tracking. Per-message TTL on the retry queue (`messaging.retry.q`) allows each in-flight retry to expire independently, preventing synchronized retry storms after outage recovery.

The system does not require event log replay, consumer group rebalancing, long-term message retention, or throughput beyond what a single well-configured RabbitMQ cluster can sustain. These are the features that justify Kafka's additional operational complexity.

---

## Alternatives Considered

### Apache Kafka

**Why considered:** Kafka is the default choice for event-driven systems at scale. Its append-only log provides durable, replayable event history. Consumer groups allow multiple independent consumers to read the same stream at their own pace. Exactly-once semantics are achievable within the Kafka cluster using idempotent producers and transactional APIs.

**Why not chosen:**

1. **Per-message delay is not a native Kafka primitive.** Implementing exponential backoff in Kafka requires either a separate retry topic per delay tier (e.g., `retry-2s`, `retry-4s`, `retry-8s`) or consumer-side delay logic that blocks the partition. Both approaches add significant application complexity compared to RabbitMQ's `x-message-ttl` header and DLX binding.

2. **The DLQ pattern requires custom infrastructure.** Kafka has no native dead-letter queue concept. Dead-lettering must be implemented in the consumer by routing failed messages to a separate topic, which requires the consumer to perform two writes (commit to DLT and commit offset) in a way that is consistent under failure — essentially reimplementing the outbox pattern inside the consumer.

3. **Replay is not a requirement.** The use case is internal service-to-service communication with a single logical consumer per event type. Kafka's partitioned log is designed for fan-out to multiple independent consumer groups that need to read the same event at different offsets and times. This system does not have that requirement. Adding Kafka for replay capability that is never used adds operational overhead without delivering value.

4. **Operational baseline is higher.** Kafka requires ZooKeeper or KRaft, a separate schema registry for Avro/Protobuf (or gives up schema enforcement), and tooling such as Kafka UI or Conduktor for visibility. RabbitMQ ships with a management UI that provides queue depth, consumer count, and message rates out of the box.

### AWS SQS + SNS

**Why considered:** Managed infrastructure with no operational burden for the broker itself. SNS fan-out to SQS queues is a well-understood pattern for decoupled microservice communication.

**Why not chosen:** Per-message delay is limited to 15 minutes maximum in SQS. The DLQ configuration is available but retry behavior (linear delay only, not exponential) requires application-side workarounds. Running locally for development and testing requires LocalStack, which adds a dependency and introduces subtle behavioral differences from the real service. The system is designed to run fully locally without cloud dependencies.

### Redis Streams

**Why considered:** Redis Streams provide a log-like data structure with consumer groups, message acknowledgement, and pending entry tracking. The operational footprint is small if Redis is already in the stack.

**Why not chosen:** Redis Streams do not provide a dead-letter mechanism. The pending entries list (PEL) requires consumer-side logic to detect and reroute stuck messages. TTL-based delay queues are not a native primitive. Redis persistence guarantees (AOF/RDB) are weaker than a dedicated message broker under failure conditions.

---

## Tradeoffs

| Gains | Costs |
|---|---|
| Per-message TTL for exponential backoff without application code | No event log replay — historical events cannot be re-consumed from the broker |
| Native DLX routing eliminates application-side dead-letter bookkeeping | No partitioned ordering — throughput scaling via parallel consumers loses per-key ordering |
| Management UI provides queue-level observability without additional tooling | Single-node deployment is a SPOF — broker restart drops unacknowledged messages back to queue |
| Exchange-queue-binding model makes routing decisions explicit and auditable | Queue mirroring (classic HA) is deprecated in favor of quorum queues — migration required for HA |
| Nack-based requeue is a first-class AMQP primitive | Consumer throughput is bounded by single-queue delivery rate — no partition-based horizontal scaling |

---

## Consequences

1. **Replay requires the outbox table**, not the broker. The `gateway_outbox_events` and `outbox_events` tables serve as the durable record of intent. Replaying an event means reprocessing from the outbox, not seeking a Kafka offset.

2. **Retry topology must be declared at startup.** The exchange-queue-binding configuration is not self-healing. A misconfigured DLX binding silently drops nacked messages. `TopologyService` asserts the full topology on every service start using constants from `libs/contracts/topology`, and topology validation is included in the integration test suite.

3. **Per-message TTL has a head-of-queue limitation.** RabbitMQ only expires messages when they reach the head of the queue. A message with a 2-second TTL behind 10,000 messages with 60-second TTLs will not expire until it reaches the head. At high queue depths, backoff is approximate rather than precise. A separate-queue-per-tier topology (e.g., `retry.tier-1`, `retry.tier-2`) eliminates this but was deferred as premature optimization.

4. **Broker availability and database availability are not independent.** The outbox relay connects to both. A broker outage causes outbox rows to accumulate; a database outage causes the relay to stall. Both are recoverable, but they are no longer isolated failure domains.

---

## Operational Impact

### Alert rules

```yaml
- alert: RabbitMQWorkQueueDepth
  expr: rabbitmq_queue_messages{queue="messaging.work"} > 100
  for: 2m
  severity: warning
  annotations:
    summary: "Work queue depth elevated — consumer may be saturated or stalled"

- alert: RabbitMQDLQNonEmpty
  expr: rabbitmq_queue_messages{queue="messaging.dlq"} > 0
  for: 0m
  severity: critical
  annotations:
    summary: "Messages in DLQ — retry budget exhausted"
```

### Runbook — DLQ non-empty

1. Inspect `messaging.dlq` via the management UI or `GET /api/queues/%2F/messaging.dlq`
2. Read the `x-death` header on the message to identify the failure reason and attempt count
3. Check structured logs for `correlationId` and `eventId` from the message headers
4. If the failure is transient and resolved, use the outbox admin endpoint to replay the original event from `gateway_outbox_events`
5. Do not requeue directly from the DLQ without first understanding the failure cause — if the failure is permanent (schema violation, invalid payload), direct requeue will exhaust another retry budget and return to the DLQ

### Runbook — topology misconfiguration

Symptoms: nacked messages disappear instead of appearing in `messaging.retry.q` or `messaging.dlq`.

1. Check `TopologyService` startup logs for assertion errors
2. Inspect queue arguments via `GET /api/queues` — confirm `x-dead-letter-exchange` argument on `messaging.work` matches the declared DLX name exactly
3. Restart the affected service — `TopologyService` will re-assert the topology on startup

---

## Future Considerations

- **Quorum queues**: If high availability is required, `messaging.work` should be migrated to a quorum queue. Classic mirrored queues are deprecated as of RabbitMQ 3.12. Quorum queues require replication factor configuration and minimum cluster size.
- **Separate retry tiers**: If per-message TTL head-of-queue delay becomes measurable under production load, replace `messaging.retry.q` with three queues (`retry.tier-1` at 2s, `retry.tier-2` at 16s, `retry.tier-3` at 128s) with queue-level TTL. This eliminates the head-of-queue problem but triples the number of queues to monitor.
- **RabbitMQ Streams**: For use cases that require replay semantics within RabbitMQ without migrating to Kafka, RabbitMQ Streams (introduced in 3.9) provide a persistent, replayable log alongside the standard queue model. Evaluate if replay requirements emerge.
