# Capacity Planning Model

## Failure Sequence Under Increasing Load

This section answers the question: **what would fail first, and in what order?**

### Stage 1 failure — Relay throughput ceiling (expected at ~250 msg/s)

**What happens:** `outbox_pending_events` begins rising. The relay is publishing at its maximum rate but cannot drain as fast as the gateway is writing. The outbox accumulates.

**Observable signals:**
- `outbox_pending_events{source="gateway"}` trending upward
- `outbox_relay_latency_ms` p99 elevated (broker confirm wait)
- Grafana Outbox Health dashboard: pending line diverging from 0

**Impact:** Delivery latency increases. P99 E2E latency rises from ~1.2s toward the retry budget ceiling (62s). No data loss. No DLQ events (unless backlog persists > 62s continuously for any single message).

**Resolution:** Add relay instance (horizontal scale) OR increase `RELAY_BATCH_SIZE` (vertical optimization).

---

### Stage 2 failure — Consumer throughput ceiling (expected at ~800–1,200 msg/s)

**What happens:** `messaging.work` queue depth begins rising. The relay is successfully publishing to the broker, but the consumer cannot process messages as fast as the relay publishes them.

**Observable signals:**
- `rabbitmq_queue_messages{queue="messaging.work"}` trending upward
- `messages_processed_total` rate lower than `outbox_published_total` rate
- Consumer `atomic_tx` p99 span rising

**Impact:** Messages accumulate in the work queue. If the queue holds messages that were published with per-message TTL (not applicable here — `messaging.work` uses no TTL), they would expire to the DLQ. In this system, `messaging.work` has no TTL — messages wait indefinitely. No data loss.

**Resolution:** Add consumer instance (horizontal scale) OR increase `AMQP_PREFETCH` setting.

---

### Stage 3 failure — PostgreSQL write contention (expected at ~2,000–3,000 msg/s)

**What happens:** Multiple gateway instances + multiple relay instances + multiple consumer instances all contend for PostgreSQL connections and index hot spots on `processed_events(event_id)` and `gateway_outbox_events(status, next_retry_at)`.

**Observable signals:**
- PostgreSQL `pg_stat_activity` shows many active transactions
- Lock waits on `processed_events` UNIQUE index (concurrent idempotency INSERTs)
- Gateway p99 latency rises (outbox INSERT wait)
- Consumer `atomic_tx` p99 rises (transaction wait)

**Impact:** Cascading: gateway latency rises → relay backlog builds → consumer backlog builds. This is a snowball effect. Recovery requires reducing load or adding database capacity (read replicas for read workloads, connection pooling via PgBouncer).

**Resolution:** PgBouncer for connection pooling, partitioned `processed_events` table, PostgreSQL vertical scaling, or read replica for non-write workloads.

---

### Stage 4 failure — RabbitMQ memory alarm (expected at ~50,000 msg/s or under memory pressure)

**What happens:** The broker hits the memory watermark (default: 40% of RAM) and triggers flow control. Publishers are blocked. Relay `waitForConfirms()` calls time out.

**Observable signals:**
- `rabbitmq_node_mem_alarm = 1`
- `publisher_confirm_failures_total` increases
- `outbox_published_total` rate drops to 0
- This is rare before other components fail in this system

**Resolution:** Increase broker RAM, reduce message queue depth (fix consumer throughput), or enable lazy queues to move queue bodies to disk.

---

## Recommended Load Test Execution Order

Run scenarios in this order during capacity planning:

1. **Scenario 01** — Establish baseline at 50/200/500 msg/s. Confirm basic performance characteristics.
2. **Scenario 05** — Measure relay scaling efficiency. Determines how many relay instances are needed.
3. **Scenario 02** — Ramp to saturation. Find the actual ceiling with the current configuration.
4. **Scenario 03** — Backlog and drain. Confirm recovery behavior and measure drain time.
5. **Scenario 04** — Retry amplification. Validate retry storm behavior and idempotency catch rate.

Do not run scenarios concurrently — each test mutates the system state (outbox table, queue depths, event_attempts table) in ways that will confuse the next scenario.

---

## Pre-Test Checklist

```bash
# 1. All services healthy
curl -s http://localhost:3000/health | jq .

# 2. Outbox tables empty
psql $DATABASE_URL -c "SELECT COUNT(*) FROM gateway_outbox_events WHERE status='pending';"
# Expected: 0

# 3. DLQ empty
curl -s -u guest:guest http://localhost:15672/api/queues/%2F/messaging.dlq | jq .messages
# Expected: 0

# 4. All queues have consumers
curl -s -u guest:guest http://localhost:15672/api/queues | jq '.[] | {name, consumers}'
# Expected: messaging.work has >= 1 consumer

# 5. PostgreSQL tables empty (fresh state)
psql $DATABASE_URL -c "SELECT relname, n_live_tup FROM pg_stat_user_tables ORDER BY n_live_tup DESC;"

# 6. Prometheus targets all up
curl -s http://localhost:9090/api/v1/targets | jq '[.data.activeTargets[] | {job: .labels.job, health: .health}]'
# Expected: all health="up"
```

---

## Post-Test Analysis Queries

```sql
-- Message processing distribution by outcome
SELECT
  CASE WHEN pe.event_id IS NOT NULL THEN 'processed'
       WHEN ea.count >= 5           THEN 'dlq_candidate'
       ELSE 'pending'
  END AS outcome,
  COUNT(*) AS event_count
FROM gateway_outbox_events go
LEFT JOIN processed_events pe ON pe.event_id = go.event_id
LEFT JOIN event_attempts   ea ON ea.event_id = go.event_id
GROUP BY 1;

-- Retry distribution (where did messages end up attempt-wise)
SELECT count AS retry_attempt, COUNT(*) AS messages
FROM event_attempts
GROUP BY count
ORDER BY count;

-- Outbox processing latency (time from INSERT to markSent)
SELECT
  PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (sent_at - created_at)) * 1000) AS p50_ms,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (sent_at - created_at)) * 1000) AS p95_ms,
  PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (sent_at - created_at)) * 1000) AS p99_ms,
  MAX(EXTRACT(EPOCH FROM (sent_at - created_at)) * 1000) AS max_ms
FROM gateway_outbox_events
WHERE status = 'sent' AND sent_at IS NOT NULL;

-- Duplicate delivery rate (idempotency effectiveness)
SELECT
  COUNT(DISTINCT pe.event_id) AS unique_events_processed,
  COUNT(*) - COUNT(DISTINCT pe.event_id) AS duplicate_deliveries_caught,
  ROUND(
    (COUNT(*) - COUNT(DISTINCT pe.event_id))::numeric / NULLIF(COUNT(*), 0) * 100,
    2
  ) AS duplicate_rate_pct
FROM processed_events pe;
```

---

## What Would Fail First — Summary

| Rank | Component | Ceiling | Signal | Fix |
|---|---|---|---|---|
| 1 | **Outbox relay** | ~250 msg/s | `outbox_pending_events` rising | Add relay instances or increase batch size |
| 2 | **Consumer** | ~800 msg/s | `messaging.work` queue depth rising | Add consumer instances or increase prefetch |
| 3 | **PostgreSQL write throughput** | ~2,000 msg/s | Lock waits, p99 latency rising | PgBouncer, partitioning, vertical scale |
| 4 | **RabbitMQ memory** | Memory watermark | `rabbitmq_node_mem_alarm = 1` | Increase broker RAM, drain queue |
| 5 | **Gateway HTTP** | ~3,000 msg/s | 5xx responses | Add gateway instances |

**The relay is the first bottleneck in the current single-instance configuration. This is architecturally intentional** — the relay co-locates with the gateway service and shares its event loop. Moving the relay to a separate process is the highest-value optimization before horizontal scaling.
