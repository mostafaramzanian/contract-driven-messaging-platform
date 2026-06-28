# Runbook: Publisher Confirm Failure

| Field | Value |
|---|---|
| **ID** | RB-004 |
| **Alert** | `OutboxPublishConfirmFailure` |
| **Severity** | Critical |
| **SLO Impact** | The at-least-once delivery guarantee is breached the moment this alert fires. A publisher confirm failure means the relay published a message to the broker but received a nack — the message was not durably accepted. If the outbox row was subsequently marked `sent`, the event is lost. |
| **On-call Action** | Immediate. This is a data integrity alert, not a latency alert. A single confirm failure requires investigation of whether any events were silently dropped. |
| **Last Updated** | 2024-01-18 |

---

## Symptoms

The relay received an AMQP `basic.nack` or `channel.close` in response to a published message. The broker refused to durably accept the message. This can mean:

- A specific message was not enqueued — it is as if the publish never happened
- If `markSent()` was called after the nack, the outbox row is marked `sent` despite the message not being in the broker
- Consumer never receives the message; downstream systems never see the event

Other signals you may observe:
- Relay logs: `publisher confirm received nack` with `deliveryTag` and `eventId`
- `publisher_confirm_failures_total` counter incremented
- `outbox_published_total` counter may appear to increment (the publish was attempted) but no corresponding consumer processing event follows
- `messages_processed_total` may show a gap for specific `eventId` values

---

## Detection Signals

### Primary alert

```yaml
alert: OutboxPublishConfirmFailure
expr: increase(publisher_confirm_failures_total[5m]) > 0
for: 0m
severity: critical
```

Zero tolerance. A single nack is an integrity event.

### Supporting queries

```promql
# Total confirm failures since service start
publisher_confirm_failures_total

# Rate of failures (should be 0; any positive value is an active incident)
rate(publisher_confirm_failures_total[1m])

# Is the relay still publishing successfully? (confirms vs failures)
rate(outbox_published_total[1m])

# Are there events that were "published" but never consumed?
# (outbox marked sent but no corresponding processed_events record)
# This requires a database query — see Investigation Step 3
```

---

## Metrics

| Metric | Normal | Incident |
|---|---|---|
| `publisher_confirm_failures_total` | 0 (ever) | > 0 |
| `publisher_confirm_failures_total` rate | 0 | > 0 |
| `outbox_published_total` vs `messages_processed_total` | equal over time | gap emerges (published not consumed) |
| `relay.publish_with_confirm` p99 | < 50ms | elevated or with errors |

---

## Grafana Panels

**Dashboard:** `cdmp-reliability` (UID: `cdmp-reliability`)

1. **Stat: Confirm failures** — non-zero value triggered this alert. The number tells you how many publishes were nacked.
2. **Time series: Idempotency duplicates vs redeliveries** — if failures lead to replay, duplicates will increase
3. **Stat: DLQ total (window)** — confirm failures may cascade to DLQ if the nacked message is not replayed and the consumer never receives it

**Dashboard:** `cdmp-distributed-tracing` (UID: `cdmp-distributed-tracing`)

4. **Table: Top slow operations** — `relay.publish_with_confirm` elevated latency may precede nack events
5. **Stat: E2E p99 latency** — a nacked publish breaks the trace chain for the affected event

---

## Root Cause Analysis

AMQP publisher confirms work as follows: the relay calls `channel.publish()` and then `channel.waitForConfirms()`. The broker responds with either `basic.ack` (message durably accepted) or `basic.nack` (message not accepted). A nack is not an error in the traditional sense — it is a signal from the broker that the message was not stored.

### Cause A: Queue not found or not durable

The exchange or target queue was deleted, misconfigured, or does not exist. The broker cannot route the message to a durable queue, so it cannot confirm durable receipt.

**Signals:** RabbitMQ management UI shows the exchange or queue as missing. Relay logs show `NOT_FOUND` or `NO_ROUTE` in the AMQP error. The topology was not asserted correctly on service startup.

### Cause B: Broker memory alarm — publishes blocked then nacked

The broker hit a memory watermark and entered flow control. After a timeout, blocked publishes may be nacked rather than held indefinitely.

**Signals:** RabbitMQ management UI shows a memory alarm banner. `rabbitmq_node_mem_alarm = 1` in Prometheus. The nack coincides with a spike in `rabbitmq_queue_messages` (consumer backlog). See also RB-002 for memory alarm recovery.

### Cause C: Channel error closes before confirm received

The AMQP channel was closed (by the broker or by a connection drop) after the publish but before the confirm arrived. `waitForConfirms()` may reject in this case, depending on the library's behavior.

**Signals:** Relay logs show channel close and confirm failure in the same time window. `getChannel()` is called immediately after (reconnection). The failure is isolated to a short window around a connection event.

### Cause D: Mandatory flag + no matching queue binding

If the publish uses the `mandatory` flag and the exchange has no binding that matches the routing key, the broker returns the message as unroutable. The `channel.on('return', ...)` event fires rather than a confirm nack — but depending on library implementation, this may surface as a confirm failure.

**Signals:** `messaging.direct` exchange has no binding to `messaging.work` queue. This is a topology misconfiguration. Check `TopologyService` startup logs.

### Cause E: Persistent publish to a non-durable queue

If `messaging.work` was accidentally declared as non-durable (survives only until broker restart), a broker restart can cause the confirm to nack for in-flight messages at the moment of restart.

**Signals:** The nack coincides exactly with a broker restart event. The queue is visible in the management UI as non-durable (the `D` flag is absent).

---

## Investigation Steps

### Step 1 — Identify the failing deliveryTag and eventId (2 minutes)

```bash
# Find confirm failure log entries
grep '"operation":"relay.publish_with_confirm"' /var/log/gateway-service/*.log \
  | grep '"outcome":"nack"\|"outcome":"error"' \
  | tail -20 \
  | jq '{
      time: .time,
      eventId: .eventId,
      correlationId: .correlationId,
      deliveryTag: .deliveryTag,
      error: .err,
      source: .source
    }'
```

Extract: `eventId` for each failed confirm. This is the event that may have been lost.

### Step 2 — Check if the outbox row was incorrectly marked sent (2 minutes)

```sql
-- Was markSent() called despite the nack?
-- A correctly implemented relay should NOT call markSent() on nack
-- But verify: the row should still be 'pending' or 'processing'
SELECT id, event_id, status, sent_at, lock_version, attempts
FROM gateway_outbox_events
WHERE event_id = '<eventId from Step 1>';
```

**Expected:** `status = 'pending'` or `'processing'` — the relay did not mark it sent.
**Alarm:** `status = 'sent'` with a `sent_at` timestamp after the confirm failure time — the relay incorrectly called `markSent()` despite the nack. The event may be lost.

### Step 3 — Verify whether the consumer received the message (3 minutes)

```sql
-- Was this event processed by the consumer?
-- If yes, the message was delivered despite the confirm failure
-- (possible if the broker accepted it to the queue but then nacked the confirm — rare but possible)
SELECT event_id, created_at
FROM processed_events
WHERE event_id = '<eventId>';

-- Was the business write committed?
SELECT id, correlation_id, created_at
FROM messages
WHERE correlation_id = '<correlationId>';
```

**If `processed_events` has a record:** The consumer processed the event. The nack was a false negative (broker accepted the message but then sent a nack — this can happen under certain failure conditions). No data was lost.

**If `processed_events` has no record AND the outbox row is `sent`:** The event was lost. The relay marked the row sent, but the broker never durably stored the message, and the consumer never received it. Go to **Recovery B** (critical path).

**If `processed_events` has no record AND the outbox row is `pending`/`processing`:** The relay correctly did not mark the row sent. The event will be retried on the next relay poll. Go to **Recovery A** (normal path).

### Step 4 — Check broker state and topology (2 minutes)

```bash
# Check exchanges
curl -s -u guest:guest http://localhost:15672/api/exchanges/%2F/messaging.direct \
  | jq '{name, durable, type}'

# Check queues
curl -s -u guest:guest http://localhost:15672/api/queues/%2F/messaging.work \
  | jq '{name, durable, messages, consumers, state}'

# Check bindings
curl -s -u guest:guest \
  http://localhost:15672/api/bindings/%2F/e/messaging.direct/q/messaging.work \
  | jq '.'

# Memory alarm?
curl -s -u guest:guest http://localhost:15672/api/nodes \
  | jq '.[].mem_alarm'
```

### Step 5 — Determine if the failure is recurring (1 minute)

```promql
# Is publisher_confirm_failures_total still increasing?
rate(publisher_confirm_failures_total[1m])
```

If still increasing: the relay is continuing to nack. The root cause is ongoing. Do not replay yet — replaying into a broker that continues to nack produces more nacks.

---

## Recovery Procedure

### Recovery A: Outbox row is pending — relay will retry automatically

```bash
# Confirm the row status
psql $DATABASE_URL -c "
SELECT id, event_id, status, attempts, lock_version
FROM gateway_outbox_events
WHERE event_id = '<eventId>';"

# If status = 'processing' with a stale lock, reset it
UPDATE gateway_outbox_events
SET status = 'pending',
    lock_version = lock_version + 1,
    locked_at = NULL
WHERE event_id = '<eventId>'
  AND status = 'processing'
  AND locked_at < NOW() - INTERVAL '60 seconds';

# The relay will pick it up on the next poll cycle (within 5s)
# Monitor: outbox_pending_events should decrease, messages_processed_total should increase
```

### Recovery B: Outbox row marked sent but consumer never received it — manual replay (CRITICAL)

This is a data integrity event. The event was lost. Perform carefully.

```sql
-- Step 1: Confirm the event is lost
-- (verified in Investigation Step 3: processed_events has no record)

-- Step 2: Reset the outbox row to pending for replay
-- This REQUIRES deleting the event_attempts record first (reset retry budget)
BEGIN;

-- Delete the stale attempt record (event has 0 successful deliveries)
DELETE FROM event_attempts WHERE event_id = '<eventId>';

-- Reset the outbox row for relay pickup
UPDATE gateway_outbox_events
SET status = 'pending',
    sent_at = NULL,
    lock_version = lock_version + 1,
    locked_at = NULL,
    attempts = 0,
    next_retry_at = NOW()
WHERE event_id = '<eventId>'
  AND status = 'sent';

COMMIT;
```

```bash
# Verify the row was reset
psql $DATABASE_URL -c "
SELECT id, event_id, status, attempts, lock_version
FROM gateway_outbox_events
WHERE event_id = '<eventId>';"

# Monitor for successful delivery within one poll cycle
watch -n 3 'psql $DATABASE_URL -t -c "
SELECT created_at FROM processed_events WHERE event_id = '"'"'<eventId>'"'"'"'
```

### Recovery C: Ongoing nack — address root cause first

```bash
# 1. Address the root cause (broker memory, missing queue, topology error)
# See Investigation Step 4 for specific remediation per cause

# 2. Restart the relay to force topology re-assertion and connection reset
docker compose restart gateway-service

# 3. Verify confirms are succeeding
grep '"operation":"relay.publish_with_confirm"' /var/log/gateway-service/*.log \
  | tail -10 \
  | jq '{time: .time, outcome: .outcome}'
# Expected: all entries show "outcome":"ack"

# 4. Only then replay any rows that were incorrectly marked sent
# (use Recovery B procedure for each affected eventId)
```

---

## Validation Checklist

- [ ] `publisher_confirm_failures_total` rate = 0 for at least 5 minutes
- [ ] For each `eventId` that received a nack: `processed_events` has a record
- [ ] For each `eventId` that received a nack: `messages` table has the expected business record
- [ ] `outbox_published_total` rate has recovered to pre-incident level
- [ ] `messaging.work` queue has a positive consumer count
- [ ] `messaging.work` and all related queues are `durable: true` in the management UI
- [ ] Broker has no memory or disk alarms
- [ ] Relay logs show `"outcome":"ack"` for all confirms in the post-recovery window
- [ ] `DLQ` has not grown (confirm failures do not go to DLQ directly, but lost events may cascade)
- [ ] Topology validation test passes (run the integration test suite confirm step)

---

## Postmortem Questions

1. Did the relay correctly handle the nack — i.e., did it NOT call `markSent()` for nacked messages? This is the critical code path. Confirm by reviewing the relay's nack handling branch.
2. Was any data permanently lost? How many events were nacked and how many were recovered via relay retry vs. manual replay?
3. What caused the broker to send a nack? Was it a topology issue, a memory alarm, or a transient channel error?
4. How was the confirm failure detected? Via the alert, or discovered after investigating a downstream data gap?
5. The confirm failure metric is zero-tolerance (alert on any single failure). Was the threshold correct, or does normal broker behavior under brief connection resets produce false positives?
6. Is the `waitForConfirms()` implementation correct under channel close? Does the relay correctly distinguish between "nack received" and "channel closed before confirm received"? These have different recovery paths.
7. Should the relay implement a confirm failure recovery path that automatically resets the outbox row rather than requiring manual intervention? What are the safety constraints on auto-recovery?
