# ADR-004: At-Least-Once Delivery with Idempotent Consumers

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

The outbox relay publishes events to RabbitMQ with publisher confirms (ADR-003). The consumer receives events and processes them. Between publish and consume, two classes of duplicate delivery can occur:

**Class 1 — Relay-side duplicates**: The relay publishes a message and receives a broker confirm, but crashes before marking the outbox row `sent`. On restart, the relay sees the row still in `pending` state and publishes it again. The broker receives the same event twice.

**Class 2 — Consumer-side duplicates**: The consumer processes a message and commits the business write, but crashes before calling `channel.ack()`. The broker re-delivers the message to the next available consumer. The business write is attempted again.

Both classes are inherent to the outbox relay + AMQP manual-ack combination. They cannot be eliminated without distributed transactions across the database and the broker (which introduces complexity exceeding the problem it solves — see ADR-003).

---

## Problem Statement

**How do we guarantee that each logical event produces exactly one business write, given that the delivery mechanism guarantees at-least-once?**

The options are:
1. Accept at-most-once delivery (drop duplicates by not retrying) — loses events on consumer crash
2. Implement exactly-once delivery via distributed transactions — excessive complexity for this use case
3. Implement at-least-once delivery with idempotent consumers — each delivery attempt is safe to retry, and the idempotency mechanism prevents duplicate business writes

---

## Decision

**Use at-least-once delivery with idempotent consumers enforced by a UNIQUE constraint.**

Every event carries a stable `eventId` (UUID generated at the producer and stored in the outbox row). The consumer writes to `processed_events(event_id)` with a UNIQUE constraint as part of the same database transaction as the business write. If a duplicate delivery arrives:

- The consumer begins a transaction
- It attempts to INSERT into `processed_events` — this violates the UNIQUE constraint
- PostgreSQL raises `23505 unique_violation`
- The transaction rolls back
- The consumer acks the message (the logical event was already processed by an earlier delivery)

This approach uses the database's serialization guarantees to provide exactly-one semantics at the business logic level, while the delivery layer remains at-least-once.

### Critical implementation detail

The idempotency INSERT and the business write must be in the **same database transaction** using a single `QueryRunner`. A check-then-write pattern (SELECT to check for existing record, then INSERT if not found) is not idempotent under concurrent delivery: two concurrent deliveries can both pass the SELECT check before either commits, and both will attempt the business write.

```typescript
// WRONG — race condition under concurrent delivery
const existing = await repo.findOne({ where: { eventId } });
if (existing) { return channel.ack(msg); }
await businessRepo.save(entity);   // two concurrent deliveries both reach here

// CORRECT — serialized by UNIQUE constraint
await queryRunner.startTransaction();
await queryRunner.manager.insert(ProcessedEvent, { eventId });  // 23505 on duplicate
await queryRunner.manager.insert(Message, entity);
await queryRunner.commitTransaction();
```

### Retry budget

Delivery attempts are tracked in `event_attempts(event_id, count)`. The retry budget is enforced by `DurableRetryBudgetService`, which increments the attempt counter and checks it against the configured maximum (default: 5) before the consumer processes the event. A message that has exceeded its budget is sent to the DLQ regardless of whether the current delivery would succeed.

The durable attempt counter is used instead of the `x-retry-count` AMQP header because the header is attached to the message and does not survive: (a) manual DLQ requeue via the management UI, (b) operator-initiated replay from the outbox admin endpoint, or (c) re-creation of the message by a separate process. An attempt counter in the database survives all of these scenarios.

---

## Alternatives Considered

### Exactly-once delivery via RabbitMQ transactions

**Why considered:** RabbitMQ supports AMQP transactions (`tx.select`, `tx.commit`, `tx.rollback`) which can bracket a publish, providing transactional publish semantics. Combined with a transactional consume, this could approximate exactly-once.

**Why not chosen:** RabbitMQ AMQP transactions are channel-level transactions that provide exactly-once publish from the producer's perspective but do not extend to the consumer's database write. A consumer that commits the AMQP transaction (marking the message as processed by the broker) and then crashes before committing the database transaction loses the business write with no re-delivery. Additionally, AMQP transactions reduce RabbitMQ throughput by 250x compared to confirm mode — they are not used in production systems.

### Exactly-once delivery via Kafka transactional API

**Why considered:** Kafka's transactional producer + idempotent consumer (read-process-write in a single transaction spanning a Kafka consumer group and a Kafka producer) provides exactly-once semantics within the Kafka ecosystem.

**Why not chosen:** Kafka was not selected as the broker (ADR-001). The Kafka transactional API does not extend exactly-once semantics to external systems (databases) — the consumer-side database write still requires idempotency. Even with Kafka, the idempotent consumer pattern described in this ADR would be required for the database write.

### Check-then-write idempotency

**Why considered:** Query `processed_events` before the business write. If the record exists, skip and ack. If not, proceed.

**Why not chosen:** This approach is not safe under concurrent delivery. Two concurrent deliveries of the same event (Class 2 duplicates from broker re-delivery, or concurrent relay publishes) will both pass the existence check before either commits. The result is two concurrent business writes for the same logical event. The UNIQUE constraint approach serializes this at the database level without application-level coordination.

### Message deduplication window (SQS-style)

**Why considered:** Some brokers (AWS SQS with deduplication enabled) deduplicate messages with the same `MessageDeduplicationId` within a 5-minute window at the broker level, before the consumer sees them.

**Why not chosen:** RabbitMQ does not provide broker-level deduplication. Implementing a deduplication window in the application (e.g., an in-memory or Redis set of recently processed `eventId` values) is vulnerable to race conditions under concurrent consumers and does not survive process restarts.

---

## Tradeoffs

| Gains | Costs |
|---|---|
| Business writes are exactly-once despite at-least-once delivery | Every delivery attempt requires a `processed_events` INSERT — even for the first delivery |
| Idempotency enforcement is a database constraint — no application-level state required | `processed_events` grows without bound and requires a purge strategy |
| The idempotency check and business write are atomic — no partial state possible | Any new consumer added to the system must implement idempotency independently |
| Duplicate deliveries are detected deterministically — no false positives | The `23505` unique violation must be caught and handled explicitly — it is not an error from the consumer's perspective |
| Durable retry budget survives operator replays and manual requeues | `event_attempts` table adds an additional write per delivery attempt |

---

## Consequences

1. **Every consumer in this system must implement the idempotent consumer pattern.** The broker does not protect consumers from duplicate delivery. A consumer that does not implement idempotency will produce duplicate business writes under any Class 1 or Class 2 duplicate scenario. This is a system-wide invariant, not a per-consumer choice.

2. **The `processed_events` table is a required infrastructure component, not an optimization.** Its absence makes the system incorrect under concurrent delivery. The table has a UNIQUE index on `event_id` — this index must not be dropped or disabled.

3. **`channel.ack()` on schema validation failure is correct behavior.** A message that fails schema validation at the consumer will never become valid through retry. Nacking it causes an infinite retry loop until the DLQ absorbs it. The correct response is to ack (removing the message from the queue), log the validation error with full payload, and emit a metric. This is counterintuitive but correct.

4. **The retry budget is enforced before processing, not after.** `DurableRetryBudgetService` checks `event_attempts.count` before any business logic runs. A message that arrives at attempt 6 (over budget) is sent to the DLQ immediately without processing, even if the current delivery would have succeeded. This prevents a pathological scenario where a message consumes unbounded retry budget after the underlying cause is fixed.

5. **`processed_events` rows accumulate.** A purge job that deletes rows older than 30 days is required in production. The retention window must be longer than the maximum expected replay window — if events can be replayed from the outbox up to 7 days after original processing, the `processed_events` retention must exceed 7 days to prevent re-processing replayed events.

---

## Operational Impact

### Alert rules

```yaml
- alert: IdempotencyDuplicateRateElevated
  expr: rate(idempotency_duplicates_prevented_total[5m]) > 10
  for: 5m
  severity: warning
  annotations:
    summary: "High duplicate delivery rate — check relay for stale-claim races or broker for re-delivery storms"

- alert: RetryBudgetExhaustion
  expr: increase(retry_exhausted_total[5m]) > 0
  for: 0m
  severity: critical
  annotations:
    summary: "Message exhausted retry budget — check DLQ and consumer error logs"
```

### Runbook — high idempotency duplicate rate

A sustained high rate of `idempotency_duplicates_prevented_total` indicates one of:
1. **Relay publishing duplicates**: check `outbox_fenced_publishes_total` for concurrent relay instances (see ADR-006 runbook)
2. **Broker re-delivering at high rate**: check `messages_redelivered_total` and `messaging.work` queue consumer count and prefetch settings
3. **Consumer slow to ack**: check `consumer.atomic_tx` p99 latency — if the business write is slow, the broker may timeout the delivery and re-deliver before the ack arrives

### Runbook — `processed_events` table maintenance

```sql
-- Check table size and oldest record
SELECT
  pg_size_pretty(pg_total_relation_size('processed_events')) AS size,
  MIN(created_at) AS oldest_record,
  COUNT(*) AS total_rows
FROM processed_events;

-- Purge records older than retention window (adjust interval as needed)
-- Run in batches during low-traffic periods to avoid lock contention
DELETE FROM processed_events
WHERE created_at < NOW() - INTERVAL '30 days'
  AND id IN (
    SELECT id FROM processed_events
    WHERE created_at < NOW() - INTERVAL '30 days'
    LIMIT 10000
  );
```

---

## Future Considerations

- **Partitioned `processed_events` table**: If the table grows large enough that INSERT latency degrades under the UNIQUE index, range-partition by `created_at` to allow old partitions to be dropped rather than row-deleted.
- **Bloom filter pre-check**: A probabilistic pre-check (Bloom filter in Redis, seeded from `processed_events`) can short-circuit the database round-trip for events that are definitely not duplicates. This reduces the `processed_events` INSERT cost for the common case (first delivery) at the cost of a Redis dependency and occasional false positives that fall through to the database.
- **Idempotency key TTL**: In high-throughput scenarios, the `processed_events` table can be replaced with a Redis key with a TTL equal to the replay window. This eliminates the database purge requirement at the cost of idempotency window limitations — an event replayed after the TTL expires will be re-processed.
