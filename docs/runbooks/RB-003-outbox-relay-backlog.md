# Runbook: Outbox Relay Backlog

| Field | Value |
|---|---|
| **ID** | RB-003 |
| **Alert** | `OutboxRelayLagging` |
| **Severity** | Warning → Critical (if not resolved within 10 minutes) |
| **SLO Impact** | Delivery latency SLO breach. Events are not lost, but the system is not delivering within the expected latency window. If the backlog continues growing, retry budgets will begin expiring and messages will enter the DLQ (Critical). |
| **On-call Action** | Investigate within 5 minutes. The relay may recover on its own (transient), or may require intervention (structural). The difference matters. |
| **Last Updated** | 2024-01-18 |

---

## Symptoms

The outbox relay is not draining events as fast as they are being written. Evidence:

- `outbox_pending_events{source="gateway"}` > 200 sustained for > 2 minutes
- `outbox_published_total` rate is lower than `messages_processed_total` rate (consumer is faster than the relay can feed it, which means relay is the bottleneck)
- OR `outbox_published_total` rate is near zero despite pending rows existing
- Delivery latency is increasing — the gap between HTTP 202 and consumer processing is growing
- In extreme cases: retry queue depth growing because the consumer is retrying messages that were eventually delivered but with a delay that looked like failure

**What is NOT happening (in this runbook):**
- Data loss — rows are accumulating in the outbox, not being dropped
- Consumer failure — the consumer may be idle but it is not the cause

---

## Detection Signals

### Primary alert

```yaml
alert: OutboxRelayLagging
expr: outbox_pending_events{source="gateway"} > 200
for: 2m
severity: warning
```

```yaml
alert: OutboxRelayLagging
expr: outbox_pending_events{source="consumer"} > 200
for: 2m
severity: warning
```

### Distinguishing backlog from broker outage

```promql
# If this is > 0, the relay is publishing successfully — it is just slow
rate(outbox_published_total{source="gateway"}[1m])

# If this is 0 AND pending is rising, the relay cannot reach the broker (see RB-002)
# If this is > 0 AND pending is rising, the relay is publishing but not fast enough
```

### Supporting queries

```promql
# How fast is the backlog growing?
deriv(outbox_pending_events{source="gateway"}[5m])
# Positive = growing faster than draining
# Negative = draining (recovery in progress)
# Near zero = steady state (relay is keeping up, but barely)

# Relay cycle latency
histogram_quantile(0.99, rate(outbox_relay_latency_ms_bucket[5m]))

# Fencing events (relay competition for rows)
rate(outbox_fenced_publishes_total[5m])

# Stale lock reaper activity (indicates relay instance crashes)
rate(outbox_reaper_reclaimed_total[5m])

# Claims per relay cycle (how many rows the relay processes per poll)
outbox_relay_claims_per_cycle
```

---

## Metrics

| Metric | Normal | Backlog |
|---|---|---|
| `outbox_pending_events{source="gateway"}` | < 20 | > 200 and rising |
| `outbox_published_total` rate | matches incoming rate | lower than incoming rate |
| `outbox_relay_latency_ms` p99 | < 50ms | elevated or spiking |
| `outbox_relay_claims_per_cycle` | 5–20 | near max batch size (relay saturated) or 0 (relay stalled) |
| `outbox_fenced_publishes_total` rate | 0 | may spike if multiple relay instances competing |
| `outbox_reaper_reclaimed_total` rate | 0 | > 0 if relay is crashing and restarting |

---

## Grafana Panels

**Dashboard:** `cdmp-outbox-health` (UID: `cdmp-outbox-health`)

1. **Stat: Pending (gateway) / Pending (consumer)** — which source is affected
2. **Time series: Pending outbox events** — slope of the line (rising = getting worse, falling = recovering)
3. **Time series: Published events** — compare rate vs. expected throughput
4. **Time series: Relay latency percentiles** — p99 spike indicates broker or DB pressure
5. **Bar chart: SKIP LOCKED claims per relay cycle** — near-zero claims = relay stalled; max-batch claims = relay saturated
6. **Bar chart: Lock contention / Stale-lock reaper** — correlated spikes indicate relay instance instability

---

## Root Cause Analysis

### Cause A: Relay process stalled (event loop saturation)

The relay runs in-process with the application service. Under heavy application load, the Node.js event loop becomes saturated, delaying the relay's `setInterval` timer. The relay poll fires late and infrequently, causing rows to accumulate.

**Signals:** `outbox_relay_latency_ms` p99 is high. `outbox_relay_claims_per_cycle` is near 0 (relay fires infrequently but processes some rows when it does). Application CPU and event loop lag metrics are elevated. Relay log timestamps show gaps longer than the poll interval.

### Cause B: Relay poll batch size too small for current throughput

The relay is draining at its maximum batch rate, but the incoming event rate exceeds what the relay can drain in a single poll cycle. The relay is healthy but undersized for current load.

**Signals:** `outbox_relay_claims_per_cycle` is consistently at or near the configured `RELAY_BATCH_SIZE` maximum. `outbox_published_total` rate is steady but lower than `messages_processed_total` rate. No relay errors in logs.

### Cause C: Broker backpressure slowing confirms

The broker is experiencing high load and confirm round-trips are slow. The relay publishes successfully but waits longer than usual for each confirm before claiming the next batch.

**Signals:** `relay.publish_with_confirm` span p99 is elevated (> 50ms). `outbox_relay_latency_ms` p99 elevated. `rabbitmq_queue_messages{queue="messaging.work"}` is high (consumer backlog). No relay errors, but high relay latency.

### Cause D: Database pressure on SKIP LOCKED query

The `SELECT ... FOR UPDATE SKIP LOCKED` query is taking longer than expected, either due to table size, index bloat, or database load. Each relay cycle is spending more time in the claim phase.

**Signals:** PostgreSQL slow query log shows the relay's SELECT query above threshold. `outbox_relay_latency_ms` p99 is elevated but `relay.publish_with_confirm` span is normal. Database CPU or I/O metrics are elevated.

### Cause E: Multiple competing relay instances (deployment overlap)

Two or more relay instances are running simultaneously, competing for the same outbox rows. `SKIP LOCKED` distributes rows between them, but the fencing mechanism detects that each instance's effective throughput is reduced by the competition.

**Signals:** `outbox_fenced_publishes_total` is non-zero. `outbox_reaper_reclaimed_total` may also be non-zero. Two relay processes visible in the process list or two relay pods in Kubernetes.

### Cause F: Outbox table needs maintenance

The `gateway_outbox_events` table has accumulated a large number of `sent` rows. The SKIP LOCKED index scan must traverse more pages to find `pending` rows, increasing claim latency.

**Signals:** Relay claim latency is elevated even under low load. PostgreSQL table size for `gateway_outbox_events` is unusually large. `EXPLAIN ANALYZE` on the claim query shows a high page read count.

---

## Investigation Steps

### Step 1 — Determine if the backlog is growing or stable (1 minute)

```promql
# In Grafana or via Prometheus HTTP API:
deriv(outbox_pending_events{source="gateway"}[5m])
```

- **Negative (draining):** The relay is recovering. Monitor and do not intervene unless it reverses.
- **Near zero (steady-state):** Relay is exactly keeping up. Any increase in load will tip it to growing. Find the bottleneck (Cause B or C).
- **Positive (growing):** Relay is losing ground. Intervention required.

### Step 2 — Check if the relay is publishing at all (1 minute)

```bash
# Recent relay log entries
grep '"operation":"relay.publish"' /var/log/gateway-service/*.log \
  | tail -10 \
  | jq '{time: .time, claimed: .claimedCount, published: .publishedCount, latencyMs: .latencyMs}'

# If no entries in the last 30s, the relay is stalled
```

### Step 3 — Check relay cycle timing (1 minute)

```bash
# Are poll cycles firing on schedule?
grep '"operation":"relay.poll"' /var/log/gateway-service/*.log \
  | tail -20 \
  | jq '.time' \
  | awk 'NR>1{cmd="date -d "$prev" +%s"; cmd | getline t1; close(cmd); cmd="date -d "$0" +%s"; cmd | getline t2; close(cmd); print t2-t1, "seconds between polls"}; {prev=$0}'
```

Expected interval: ~5 seconds (default `RELAY_POLL_INTERVAL_MS = 5000`). Gaps significantly larger indicate event loop saturation (Cause A).

### Step 4 — Check database (2 minutes)

```sql
-- Pending row count and oldest pending row
SELECT
  COUNT(*) AS pending_rows,
  NOW() - MIN(created_at) AS oldest_pending_age,
  SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) AS processing_rows
FROM gateway_outbox_events
WHERE status IN ('pending', 'processing');

-- Check for stale processing rows (relay crashed while holding claim)
SELECT id, event_id, locked_at, NOW() - locked_at AS lock_age
FROM gateway_outbox_events
WHERE status = 'processing'
  AND locked_at < NOW() - INTERVAL '60 seconds'
ORDER BY locked_at ASC;

-- Table size
SELECT pg_size_pretty(pg_total_relation_size('gateway_outbox_events')) AS table_size;

-- Index health
SELECT indexname, idx_scan, idx_tup_fetch
FROM pg_stat_user_indexes
WHERE relname = 'gateway_outbox_events';
```

### Step 5 — Check for multiple relay instances (1 minute)

```bash
# Local: are multiple relay processes running?
pgrep -a node | grep gateway

# Kubernetes: are multiple relay pods running?
kubectl get pods -l app=gateway-service

# Check fencing metric
curl -s http://localhost:9090/api/v1/query \
  -d 'query=increase(outbox_fenced_publishes_total[5m])' \
  | jq '.data.result[0].value[1]'
```

---

## Recovery Procedure

### Recovery A: Event loop saturation — restart or scale

```bash
# Option 1: Restart the gateway service to reset the event loop
docker compose restart gateway-service
# Or Kubernetes:
kubectl rollout restart deployment/gateway-service

# Option 2: If load is genuinely high, scale the gateway service
# (Note: multiple relay instances require fencing tokens to be safe — they are implemented)
kubectl scale deployment/gateway-service --replicas=2

# Monitor recovery
watch -n 5 'psql $DATABASE_URL -t -c "
SELECT COUNT(*) FROM gateway_outbox_events WHERE status = '"'"'pending'"'"'"'
```

### Recovery B: Increase relay batch size

```bash
# Increase the batch size the relay claims per poll cycle
# Current default: RELAY_BATCH_SIZE=10

# Update environment and restart
RELAY_BATCH_SIZE=50 docker compose up -d gateway-service

# Monitor: outbox_relay_claims_per_cycle should now be higher
# Monitor: outbox_pending_events should begin decreasing
```

### Recovery C: Broker backpressure — investigate consumer

```bash
# Is the consumer keeping up with the work queue?
curl -s -u guest:guest http://localhost:15672/api/queues/%2F/messaging.work \
  | jq '{messages: .messages, consumers: .consumers, message_stats: .message_stats}'

# Scale the consumer if it is the bottleneck
kubectl scale deployment/messaging-service --replicas=2

# Or reduce consumer processing latency — check consumer.atomic_tx p99
```

### Recovery D: Database index maintenance

```sql
-- Vacuum and analyze the outbox table
VACUUM ANALYZE gateway_outbox_events;

-- If index bloat is severe, rebuild the index (takes an exclusive lock — schedule during low traffic)
REINDEX INDEX CONCURRENTLY gateway_outbox_events_status_next_retry_idx;

-- Purge old sent rows to reduce table size
DELETE FROM gateway_outbox_events
WHERE status = 'sent'
  AND created_at < NOW() - INTERVAL '7 days'
  AND id IN (
    SELECT id FROM gateway_outbox_events
    WHERE status = 'sent' AND created_at < NOW() - INTERVAL '7 days'
    LIMIT 50000
  );
```

### Recovery E: Competing relay instances — terminate stale instance

```bash
# Identify which instance is stale (lowest uptime, or from previous deployment)
# Kubernetes: check pod ages
kubectl get pods -l app=gateway-service --sort-by='.status.startTime'

# Terminate the old pod
kubectl delete pod <old-pod-name>

# Confirm fencing stops
curl -s http://localhost:9090/api/v1/query \
  -d 'query=rate(outbox_fenced_publishes_total[2m])' \
  | jq '.data.result[0].value[1]'
# Should return "0" after the stale instance is terminated
```

### Recovery F: Force stale lock cleanup

```sql
-- If the stale-lock reaper is not firing, manually reset stale processing rows
UPDATE gateway_outbox_events
SET status = 'pending',
    lock_version = lock_version + 1,
    locked_at = NULL
WHERE status = 'processing'
  AND locked_at < NOW() - INTERVAL '120 seconds';

-- Check how many rows were reset
-- (run SELECT first to confirm before UPDATE)
SELECT COUNT(*) FROM gateway_outbox_events
WHERE status = 'processing'
  AND locked_at < NOW() - INTERVAL '120 seconds';
```

---

## Validation Checklist

- [ ] `outbox_pending_events{source="gateway"}` is decreasing and approaching baseline (< 20)
- [ ] `deriv(outbox_pending_events{source="gateway"}[5m])` is negative (draining)
- [ ] `outbox_published_total` rate is positive and matches or exceeds incoming request rate
- [ ] `outbox_relay_claims_per_cycle` is in normal range (not 0, not pegged at max)
- [ ] `outbox_relay_latency_ms` p99 is below 50ms
- [ ] No stale `processing` rows in `gateway_outbox_events` (locked_at > 60s ago)
- [ ] `outbox_fenced_publishes_total` is not increasing (no competing relay instances)
- [ ] `messages_processed_total` rate has recovered to pre-incident level
- [ ] `messaging.dlq` has not gained new messages during the backlog window (if it has, run RB-001)

---

## Postmortem Questions

1. What was the root cause of the backlog? Was it a code path (event loop saturation), a configuration issue (batch size), or an infrastructure issue (database or broker pressure)?
2. How long did the backlog persist before the alert fired? The alert requires > 200 pending for 2 minutes — was the pre-alert accumulation period significant?
3. Did any messages exhaust their retry budget during the backlog? If the backlog caused delayed delivery, and the consumer was retrying a previous failure that the relay was also trying to deliver, was there confusion between the retry path and the outbox path?
4. Is the relay batch size appropriately sized for the current traffic volume? Should `RELAY_BATCH_SIZE` be increased as a permanent configuration change?
5. Is the in-process relay colocated with the application a structural risk? Should the relay be moved to a separate process to isolate its event loop?
6. Was the `outbox_pending_events` alert threshold (200) appropriate? Was the incident serious before 200 was reached, or was 200 too low and caused a false alarm?
7. Is the `gateway_outbox_events` purge job implemented? If not, is table bloat contributing to relay latency?
