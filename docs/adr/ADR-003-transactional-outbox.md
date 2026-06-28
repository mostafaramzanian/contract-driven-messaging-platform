# ADR-003: Transactional Outbox Pattern

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2024-01-12 |
| **Author** | Platform Engineering |
| **Reviewers** | Backend Engineering |
| **Supersedes** | — |
| **Superseded by** | — |

---

## Context

The gateway service must publish an event to RabbitMQ as part of handling an HTTP request. The naive implementation is:

```typescript
await businessRepository.save(entity);          // 1. write to database
await channel.publish(exchange, key, payload);  // 2. publish to broker
```

This dual-write has two failure windows:

- **Window A**: The database write succeeds; the process crashes before `channel.publish()`. The event is never sent. The caller received a `202 Accepted`; no event exists.
- **Window B**: `channel.publish()` is called but the broker is temporarily unavailable; the write returns an error. The business state is committed; no event was published.

In both cases the system is in an inconsistent state that is invisible to the caller and to the consumer. There is no mechanism to detect or recover from either failure without external coordination.

The same problem exists on the consumer side: the messaging service must persist the processed message to PostgreSQL and publish a domain event (`MessagePersisted`) to notify downstream systems. A crash between the two writes leaves the same inconsistency.

---

## Problem Statement

**The dual-write problem**: reliably writing to two independent systems (a relational database and a message broker) within a single logical operation without a distributed transaction coordinator.

The goal is to guarantee that every committed business write eventually produces a corresponding broker message, and that no broker message is produced without a committed business write, even in the presence of process crashes, broker unavailability, and relay restarts.

---

## Decision

**Implement the transactional outbox pattern on both the producer side (gateway) and the consumer side (messaging service).**

On the producer side:
- The HTTP handler writes the business payload and an outbox row to `gateway_outbox_events` in a single database transaction
- `GatewayOutboxRelayService` polls `gateway_outbox_events` for `pending` rows using `SELECT ... FOR UPDATE SKIP LOCKED`, publishes each to RabbitMQ with publisher confirms, and marks the row `sent` only after the broker confirms receipt

On the consumer side:
- The AMQP handler writes the business record (`messages`), the domain event row (`outbox_events`), and the idempotency record (`processed_events`) in a single database transaction
- `OutboxRelayService` polls `outbox_events` and publishes domain events to `messaging.events` exchange

In both cases, the database transaction is the serialization point. The relay is a best-effort publisher that can be restarted without data loss.

---

## Alternatives Considered

### Two-phase commit (2PC) across database and broker

**Why considered:** 2PC provides exactly-once semantics across two systems without application-level coordination. The transaction coordinator ensures both the database write and the broker write either both commit or both abort.

**Why not chosen:** RabbitMQ does not support XA transactions. Implementing an application-level 2PC coordinator would require: (a) a coordinator process that is itself a SPOF, (b) prepared transaction state that must survive coordinator crashes, and (c) timeout and rollback logic that is more complex than the outbox pattern. The failure modes introduced by a distributed transaction coordinator exceed the failure modes eliminated. At the message volumes this system targets, the outbox pattern provides equivalent observable behavior (eventual delivery) at far lower implementation complexity.

### Change Data Capture (CDC) via Debezium

**Why considered:** CDC reads the PostgreSQL write-ahead log (WAL) and streams row-level changes to a broker without polling. This eliminates the poll latency of the outbox relay and decouples relay throughput from poll frequency.

**Why not chosen:** CDC requires: (a) a PostgreSQL replication slot, which accumulates WAL if the CDC connector falls behind and can cause disk exhaustion, (b) a dedicated Debezium process (or Kafka Connect cluster) that is an additional operational dependency, (c) a Kafka topic as the CDC transport, which contradicts ADR-001's decision to use RabbitMQ. The polling-based outbox relay adds at most one poll interval of delivery latency (~5s at the default configuration) in exchange for significantly simpler operational requirements. PostgreSQL `LISTEN`/`NOTIFY` is the recommended upgrade path if sub-second relay latency is required without CDC's operational cost.

### Direct broker publish with retry

**Why considered:** The simplest implementation: publish to the broker, and if it fails, retry until it succeeds. No outbox table required.

**Why not chosen:** This approach works until the process crashes between the business write and the successful broker publish. On restart, there is no record of the uncommitted publish. The only recovery mechanism is re-processing the HTTP request, which requires the caller to detect the failure (the `202 Accepted` they received gives no indication) and replay the request. This shifts the reliability burden to the caller, which has no reliable way to know whether replay is safe.

### Saga with compensation

**Why considered:** A saga decomposes the dual-write into a sequence of local transactions with compensating transactions for each step. If step 2 (broker publish) fails, the compensation for step 1 (business write) rolls back the database.

**Why not chosen:** Compensation is appropriate when both writes have business-level semantics that must be reversed. In this system, the broker publish is not a business operation with a meaningful inverse — it is a delivery mechanism. Compensating by rolling back the business write on broker failure is semantically incorrect: the business intent was expressed, the payload is valid, and the delivery failure is transient. The outbox pattern correctly separates the business write (which succeeds atomically) from the delivery attempt (which is retried asynchronously).

---

## Tradeoffs

| Gains | Costs |
|---|---|
| No event loss on process crash — the outbox row survives and the relay will re-publish | Delivery latency is bounded below by the relay poll interval (~5s default) |
| Business write and event emission are atomic — no inconsistent state possible | The outbox table grows without bound and requires a purge strategy |
| Relay can be restarted, scaled, and deployed independently of the service | The relay couples database availability and broker availability — both must be up for delivery |
| Publisher confirms close the relay→broker delivery window | Running the relay in-process with the service couples their event loops |
| `SKIP LOCKED` enables multiple relay instances without distributed locking | `SKIP LOCKED` alone is insufficient under concurrent relay restart — requires fencing token (ADR-006) |

---

## Consequences

1. **End-to-end delivery latency has a floor.** Under the default 5-second poll interval, a message committed at second 0 may not be published until second 5. If sub-second relay latency is required, the poll interval can be reduced or PostgreSQL `LISTEN`/`NOTIFY` can be added to wake the relay immediately on INSERT.

2. **`gateway_outbox_events` and `outbox_events` require maintenance.** Rows with `status = 'sent'` accumulate indefinitely. A nightly purge job that deletes rows older than 7 days with `status = 'sent'` is required in production. The current implementation does not include this purge job — it is a known operational gap.

3. **The relay is a critical-path dependency.** If the relay process stalls (event loop saturation, OOM), outbox rows accumulate and `outbox_pending_events` rises. The `OutboxRelayLagging` alert fires at 200 pending events for 2 minutes. Recovery is a relay restart; no data is lost.

4. **In-process relay shares the Node.js event loop.** Under heavy consumer load, the relay's poll timer may be delayed by event loop saturation. Relay throughput will degrade before consumer throughput does. In production, the relay should run as a separate process or service to isolate its event loop.

5. **The relay must handle `SKIP LOCKED` stale-claim scenarios.** See ADR-006 for the fencing token mechanism that closes the race condition left open by `SKIP LOCKED` alone.

---

## Operational Impact

### Alert rules

```yaml
- alert: OutboxRelayLagging
  expr: outbox_pending_events{source="gateway"} > 200
  for: 2m
  severity: warning
  annotations:
    summary: "Gateway outbox relay not draining — check relay process and broker connectivity"

- alert: OutboxRelayLagging
  expr: outbox_pending_events{source="consumer"} > 200
  for: 2m
  severity: warning
  annotations:
    summary: "Consumer outbox relay not draining"

- alert: OutboxPublishConfirmFailure
  expr: increase(publisher_confirm_failures_total[5m]) > 0
  for: 0m
  severity: critical
  annotations:
    summary: "Outbox relay received a publisher confirm nack — at-least-once guarantee at risk"
```

### Runbook — relay not draining

1. Check `outbox_pending_events` gauge — confirm it is rising, not just elevated
2. Check relay logs for connectivity errors to RabbitMQ or PostgreSQL
3. Check `outbox_fenced_publishes_total` — a spike indicates concurrent relay instances are fighting for the same rows (see ADR-006 runbook)
4. Restart the relay process — on startup it will resume polling immediately
5. Confirm `outbox_pending_events` begins decreasing within one poll interval after restart

### Runbook — outbox table size

```sql
-- Check table size
SELECT pg_size_pretty(pg_total_relation_size('gateway_outbox_events'));

-- Count by status
SELECT status, COUNT(*) FROM gateway_outbox_events GROUP BY status;

-- Purge sent rows older than 7 days (run during low-traffic window)
DELETE FROM gateway_outbox_events
WHERE status = 'sent' AND created_at < NOW() - INTERVAL '7 days';
```

---

## Future Considerations

- **PostgreSQL `LISTEN`/`NOTIFY`**: The business write transaction can issue `NOTIFY outbox_relay` immediately after INSERT. The relay subscribes to this channel and wakes immediately rather than waiting for the next poll interval. This reduces relay latency from O(poll_interval) to O(network_roundtrip) for the common case, while retaining polling as a fallback for rows missed by a notify-then-crash scenario.
- **Separate relay process**: The in-process relay shares the Node.js event loop with the application. Under heavy load, relay poll delays are a leading indicator of application saturation. A separate relay process with its own event loop eliminates this coupling and allows the relay to be scaled independently.
- **Outbox table partitioning**: If the outbox table grows large enough that the `SELECT ... FOR UPDATE SKIP LOCKED` index scan degrades, range partitioning by `created_at` allows old partitions to be dropped rather than row-level deleted.
