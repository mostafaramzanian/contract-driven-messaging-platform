# Runbook: DLQ Growth Spike

| Field | Value |
|---|---|
| **ID** | RB-001 |
| **Alert** | `DLQMessageReceived` |
| **Severity** | Critical |
| **SLO Impact** | Direct — every DLQ event represents a message that will never be delivered without operator action |
| **On-call Action** | Immediate investigation required. Do not wait for business hours. |
| **Last Updated** | 2024-01-18 |

---

## Symptoms

You are paged because `dlq_messages_total` increased. At least one message has exhausted its retry budget and is now dead. It will not be retried automatically. If you do not act, the business event it represents is permanently unprocessed.

What you may also observe:
- Grafana **Reliability** dashboard shows `DLQ EVENTS — FIRING` alert banner
- `retry_exhausted_total` counter increased in the minutes before the DLQ spike
- `retry_count_total{attempt="5"}` has non-zero values (messages reaching the final retry)
- Consumer error logs show repeated failures for a specific `eventId` or `correlationId`
- Downstream systems may report missing data if the dropped event was a required trigger

---

## Detection Signals

### Primary alert

```yaml
alert: DLQMessageReceived
expr: increase(dlq_messages_total[5m]) > 0
for: 0m
severity: critical
```

Zero tolerance. This alert fires the moment a single message reaches the DLQ. There is no grace period because the DLQ is not a buffer — it is a dead end.

### Supporting signals to check immediately

```promql
# How many messages hit the DLQ in the last 30 minutes?
increase(dlq_messages_total[30m])

# What was the retry exhaustion rate before the spike?
rate(retry_exhausted_total[5m])

# What does the retry funnel look like? (should decrease monotonically)
rate(retry_count_total{attempt="1"}[5m])
rate(retry_count_total{attempt="5"}[5m])

# Is the consumer success rate degraded?
rate(messages_processed_total{status="success"}[5m]) /
rate(messages_processed_total[5m])

# Is this an isolated event or a sustained failure?
increase(dlq_messages_total[5m]) vs increase(dlq_messages_total[1h])
```

---

## Metrics

| Metric | Normal | Incident |
|---|---|---|
| `dlq_messages_total` (rate) | 0 | > 0 |
| `retry_exhausted_total` | 0 | > 0, rising |
| `retry_count_total{attempt="5"}` | 0 | > 0 |
| `messages_processed_total{status="error"}` rate | < 0.5% | elevated |
| `event_attempts` (max count in DB) | ≤ 5 | = 5 at failure time |

---

## Grafana Panels

**Dashboard:** `cdmp-reliability` (UID: `cdmp-reliability`)

1. **Stat: DLQ total (window)** — shows the count that triggered the alert
2. **Stat: Retry exhaustions** — confirms budget was exhausted, not a routing error
3. **Bar chart: Retry count by attempt** — a flat funnel (high attempt-5 relative to attempt-1) indicates systematic failure, not a one-off
4. **Time series: DLQ growth** — look for isolated spike (single bad message) vs. sustained slope (ongoing failure)

**Dashboard:** `cdmp-system-overview` (UID: `cdmp-system-overview`)

5. **Stat: Success rate** — a DLQ spike with a degraded success rate means the failure is broad; with a healthy success rate it is isolated

---

## Root Cause Analysis

DLQ events have three root causes. Identify which before taking action.

### Cause A: Permanent business logic failure

The consumer received a valid message but failed during processing for a non-transient reason. Examples:
- A business rule violation (referenced entity does not exist)
- A bug in consumer code that throws unconditionally on specific payload shapes
- A schema that passes Zod validation but fails domain validation

**Distinguishing signal:** The failure appears on the same `eventType` or similar payload shapes across multiple messages. Consumer logs show application-level errors (not infrastructure errors like connection refused).

### Cause B: Infrastructure failure that outlasted the retry budget

The consumer could not reach PostgreSQL or another dependency during all five retry attempts. Examples:
- PostgreSQL was down for more than ~62 seconds (the cumulative 5-attempt backoff window: 2+4+8+16+32s)
- Network partition between consumer and database during the retry window
- Database connection pool exhausted for the full retry window

**Distinguishing signal:** Consumer logs show infrastructure errors (`ECONNREFUSED`, `ETIMEDOUT`, PostgreSQL error codes `08006`, `57P03`) for all attempts. The message arrived during a known outage window.

### Cause C: Permanent schema validation failure reaching the DLQ

Schema validation failures are acked immediately (not nacked). If a schema-invalid message reached the DLQ, it was either: (a) nacked incorrectly due to a bug in the error classifier, or (b) a message that passed schema validation but failed during upcasting.

**Distinguishing signal:** DLQ message headers show `x-death` with `reason: rejected` and the `x-first-death-exchange` is the main exchange, not the DLX. Consumer logs show Zod parse errors.

---

## Investigation Steps

### Step 1 — Inspect the DLQ message (2 minutes)

```bash
# Via RabbitMQ management API
curl -s -u guest:guest \
  http://localhost:15672/api/queues/%2F/messaging.dlq/get \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{"count":10,"requeue":false,"encoding":"auto","ackmode":"ack_requeue_true"}' \
  | jq '.[].properties.headers'
```

Extract from the message headers:
- `x-death[0].reason` — `expired` (retry TTL elapsed) or `rejected` (nacked)
- `x-death[0].count` — number of times through the DLX
- `x-retry-count` — attempt number at death
- `correlationId` — use to find all log entries for this event
- `eventId` — use to check `event_attempts` and `processed_events` tables

### Step 2 — Correlate in logs (3 minutes)

```bash
# Find all log entries for the failing event
# Replace <correlationId> with the value from the message header
grep '"correlationId":"<correlationId>"' /var/log/messaging-service/*.log \
  | jq '{time: .time, operation: .operation, status: .status, error: .err}'
```

Look for:
- Which operation failed (`consumer.atomic_tx`, `consumer.schema_validation`, etc.)
- The error message and PostgreSQL error code if present
- The pattern of failures across attempts (same error each time = Cause A or B; different errors = transient + permanent)

### Step 3 — Check the database state (2 minutes)

```sql
-- Was the business write committed?
SELECT id, subject, status, created_at
FROM messages
WHERE correlation_id = '<correlationId>';

-- What was the attempt count at death?
SELECT event_id, count, last_attempt_at
FROM event_attempts
WHERE event_id = '<eventId>';

-- Was the idempotency record written? (if yes, business write succeeded at some point)
SELECT event_id, created_at
FROM processed_events
WHERE event_id = '<eventId>';
```

If `processed_events` has a record and `messages` has a record: the message was processed successfully at some point. The DLQ arrival is a late duplicate that the idempotency check would have caught — but it reached the DLQ via a different path. Safe to discard.

If neither table has a record: the business write never committed. The message payload represents unprocessed work. Go to recovery.

### Step 4 — Determine scope (1 minute)

```sql
-- How many events are affected?
SELECT COUNT(*) FROM event_attempts WHERE count >= 5;

-- Are multiple event types affected?
-- (requires joining event_attempts with outbox or message headers)
SELECT source, COUNT(*) FROM event_attempts
WHERE count >= 5
GROUP BY source;
```

If more than one event type or more than 10 events are in the DLQ simultaneously, this is a systemic failure (Cause B), not an isolated bad message (Cause A).

---

## Recovery Procedure

### Recovery A: Infrastructure failure — replay from outbox

Use this when: the message reached the DLQ because an infrastructure dependency was unavailable for the retry window, but the dependency has now recovered.

```bash
# 1. Confirm the dependency is healthy
curl -s http://localhost:3000/health | jq '.components'

# 2. Identify the original outbox row
# eventId is in the DLQ message headers
SELECT id, event_id, status, attempts, created_at
FROM gateway_outbox_events
WHERE event_id = '<eventId>';

# 3. Reset the outbox row for replay
UPDATE gateway_outbox_events
SET status = 'pending',
    attempts = 0,
    next_retry_at = NOW(),
    locked_at = NULL,
    lock_version = lock_version + 1
WHERE event_id = '<eventId>'
  AND status = 'sent';  -- safety: only reset if already sent (relay published but consumer failed)

# 4. Reset the event_attempts counter so the consumer has a fresh budget
DELETE FROM event_attempts WHERE event_id = '<eventId>';

# 5. Confirm the relay picks up the reset row (within one poll interval, default 5s)
# Watch: outbox_pending_events and then messages_processed_total
```

**Do not manually requeue from the DLQ.** The DLQ message carries a stale retry count (`x-retry-count = 5`) in its headers. If requeued directly, the consumer's durable budget check (`event_attempts` table) will immediately route it back to the DLQ without processing. Replay via the outbox resets both the row state and the attempt counter.

### Recovery B: Application bug — fix, deploy, then replay

Use this when: the consumer has a code bug that causes it to fail on a specific payload shape.

```
1. Identify the failing payload from DLQ message body
2. Reproduce locally with a unit test
3. Fix the bug in the consumer
4. Deploy the fixed consumer
5. Validate the fix with the local test
6. Replay the DLQ message via the outbox reset procedure above
7. Monitor consumer logs for successful processing
```

**Do not replay until the fix is deployed.** Replaying before the fix will exhaust another retry budget and return the message to the DLQ.

### Recovery C: Irretrievably bad message — discard with audit record

Use this when: the message payload is permanently invalid (schema drift with no upcaster, corrupted payload, test data in production) and no business action is required.

```bash
# 1. Log a structured audit record before discarding
echo '{
  "action": "dlq_discard",
  "eventId": "<eventId>",
  "correlationId": "<correlationId>",
  "reason": "<human-readable reason>",
  "operator": "<your name>",
  "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"
}' >> /var/log/dlq-audit.jsonl

# 2. Ack the message via the management API (removes it from DLQ)
curl -s -u guest:guest \
  http://localhost:15672/api/queues/%2F/messaging.dlq/get \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{"count":1,"requeue":false,"encoding":"auto","ackmode":"ack_requeue_false"}'
```

---

## Validation Checklist

After recovery, confirm all of the following before closing the incident:

- [ ] `dlq_messages_total` is not increasing
- [ ] `messaging.dlq` queue depth is 0 (or contains only known-discarded messages)
- [ ] `retry_exhausted_total` rate has returned to 0
- [ ] Consumer success rate (`messages_processed_total{status="success"}` / total) is ≥ 99.5%
- [ ] The specific `eventId` from the incident appears in `processed_events` (if recovery A or B) or in the audit log (if recovery C)
- [ ] `messages` table contains the expected record (if the event represented business data)
- [ ] `event_attempts.count` for the recovered event is ≤ 3 (confirms the replay took a clean retry path, not a recycled exhausted one)
- [ ] No new DLQ messages in the 15 minutes following recovery

---

## Postmortem Questions

1. What was the root cause of the original consumer failure? Was it a code bug, an infrastructure outage, or a bad payload?
2. Why did the failure persist across all five retry attempts? Was the retry window (cumulative ~62s) too short for the underlying dependency to recover?
3. Was the DLQ alert response time within SLO? How long between first DLQ message and operator awareness?
4. Was the business impact from the unprocessed event detectable downstream? Did any system notice the missing data?
5. Should the retry budget (5 attempts, 2ⁿ×2s backoff) be adjusted for this failure mode?
6. Was the investigation blocked by insufficient log correlation? Were `correlationId` and `eventId` present in all relevant log entries?
7. Was the recovery procedure clear and safe to execute under pressure at 3 AM? What would have made it faster?
8. Should this failure mode have a dedicated alert that fires earlier (e.g., at attempt 4) to give more lead time?
