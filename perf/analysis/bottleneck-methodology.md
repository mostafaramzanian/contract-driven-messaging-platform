# Bottleneck Analysis Methodology

## Theory of Constraints Applied to the Messaging Pipeline

The pipeline has four sequential processing stages. The slowest stage determines end-to-end throughput. A bottleneck in one stage does not automatically become apparent in another — monitoring only the gateway HTTP latency will not reveal a relay bottleneck until the outbox backlog grows large enough to affect gateway behavior (which in this system, it never will — the gateway returns 202 before checking the outbox).

```
[HTTP Client]
     ↓  HTTP
[Gateway Service]          ← Stage 1: HTTP handler + Zod + outbox INSERT
     ↓  PostgreSQL write
[gateway_outbox_events]    ← Stage 2: Outbox table (accumulation buffer)
     ↓  SKIP LOCKED poll + AMQP publish + confirm
[RabbitMQ: messaging.work] ← Stage 3: Broker (in-flight buffer)
     ↓  AMQP delivery
[Messaging Service]        ← Stage 4: Consumer (Zod + upcaster + atomic TX + ack)
```

The outbox table is the only stage that can absorb backlog. Stages 1, 3, and 4 do not buffer — they either process or reject.

---

## Component Throughput Ceilings (Back-of-Envelope)

These estimates assume a single instance of each component and the default configuration.

### Stage 1: Gateway HTTP Handler

```
Bottleneck: PostgreSQL INSERT latency
Estimated ceiling: ~3,000 msg/s

Calculation:
  - Handler steps: Zod validation (0.5ms) + outbox INSERT (3ms avg) = 3.5ms
  - Node.js event loop with 4 concurrent DB connections:
    4 connections × (1000ms / 3.5ms) ≈ 1,140 parallel writes/s
  - With connection pool of 10: ~2,860 writes/s
  - Practical ceiling (event loop overhead): ~2,000–3,000 msg/s

First to degrade: gateway p99 latency rises above 50ms
Observable signal: http_req_duration p99 on gateway
```

### Stage 2: Outbox Relay

```
Bottleneck: AMQP round-trip for publisher confirms
Estimated ceiling: ~250 msg/s (single instance, default batch of 25)

Calculation:
  - AMQP confirm latency (single message): ~2–8ms (local Docker network)
  - Batch of 25 with sequential waitForConfirms(): 25 × 5ms avg = 125ms per batch
  - Batches per second: 1000ms / 125ms = 8 batches/s
  - Throughput: 8 × 25 = 200 msg/s
  - With batch confirms (parallel waitForConfirms for batch): ~250 msg/s

First to degrade: outbox_pending_events begins rising
Observable signal: outbox_pending_events{source="gateway"} > 50

Relay is the most likely first bottleneck under sustained load.
```

### Stage 3: RabbitMQ Broker

```
Bottleneck: Message enqueue and routing throughput
Estimated ceiling: ~50,000 msg/s (single node, standard queue, durable)

Notes:
- Single durable queue throughput is well above the other stages' ceilings
- The broker is unlikely to bottleneck before Stage 2 (relay) or Stage 4 (consumer)
- Memory alarm at 40% of available RAM will trigger flow control
  → manifests as producer-side AMQP channel blocking
  → observable via rabbitmq_node_mem_alarm = 1 and publisher_confirm_failures rising
```

### Stage 4: Messaging Service Consumer

```
Bottleneck: Atomic transaction (three-table write)
Estimated ceiling: ~800–1,200 msg/s (single instance, 20 connection pool)

Calculation:
  - Consumer steps:
      Zod + schema dispatch:          0.5ms
      recordAttempt():                1ms   (event_attempts INSERT)
      Atomic TX (3 writes + commit):  5–10ms
      channel.ack():                  0.5ms
    Total: 7–12ms per message
  - With manual ACK prefetch = 10 (10 concurrent in-flight deliveries):
      10 × (1000ms / 9ms avg) ≈ 1,100 msg/s
  - With PostgreSQL connection pool of 20:
      20 × (1000ms / 9ms) ≈ 2,200 TX/s theoretical
      Practical (event loop, GC): ~800–1,200 msg/s

First to degrade: messaging.work queue depth grows (consumer not keeping up)
Observable signal: rabbitmq_queue_messages{queue="messaging.work"} rising
```

---

## Bottleneck Identification Decision Tree

```
START: Alert fires or performance degradation observed
                         │
                         ▼
         Is outbox_pending_events rising?
        /                              \
      YES                              NO
       │                               │
       ▼                               ▼
  Is outbox_published_total     Is messaging.work queue
  rate > 0?                     depth rising?
  /          \                  /              \
YES           NO              YES               NO
 │             │               │                │
 ▼             ▼               ▼                ▼
Relay is     RabbitMQ       Consumer is       Check gateway
slow (Stage  outage or      slow (Stage 4)    http_req_duration
2 bottleneck) network       → Scale consumer  → If p99 > 200ms:
→ See        (see RB-002)   instances or      Stage 1 bottleneck
bottleneck                  increase prefetch → Scale gateway
analysis A                                    → Add DB connections
```

---

## Bottleneck Analysis A: Relay Throughput

**When to use:** `outbox_pending_events` is rising, `outbox_published_total` rate > 0.

### Step 1: Measure the relay's actual throughput ceiling

```promql
# Relay throughput over the last 30s
rate(outbox_published_total{source="gateway"}[30s])

# Relay latency distribution
histogram_quantile(0.50, rate(outbox_relay_latency_ms_bucket[30s]))
histogram_quantile(0.99, rate(outbox_relay_latency_ms_bucket[30s]))
```

### Step 2: Identify whether the bottleneck is broker or database

```bash
# Is the broker confirm round-trip slow?
# Check relay.publish_with_confirm p99 in Jaeger or Prometheus
# Expected: < 10ms (local), < 50ms (remote broker)
# If > 100ms: broker is under pressure → scale broker, reduce batch size

# Is the SKIP LOCKED query slow?
psql $DATABASE_URL -c "
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, event_id, payload, trace_context, lock_version
FROM gateway_outbox_events
WHERE status = 'pending'
  AND next_retry_at <= NOW()
ORDER BY next_retry_at
LIMIT 25
FOR UPDATE SKIP LOCKED;"
# Look for: Seq Scan (should be Index Scan), high Shared Hit Blocks
```

### Step 3: Calculate relay throughput ceiling formula

```
relay_throughput = batch_size / (confirm_rtt_ms / 1000)

At current confirm_rtt_ms:
- If confirm_rtt = 5ms: ceiling = 25 / 0.005 = 5,000 msg/s
- If confirm_rtt = 20ms: ceiling = 25 / 0.020 = 1,250 msg/s
- If confirm_rtt = 50ms: ceiling = 25 / 0.050 = 500 msg/s

To increase ceiling: increase batch_size OR reduce confirm_rtt OR add relay instances
```

### Step 4: Scaling recommendations

| Scenario | Recommended Action | Expected Improvement |
|---|---|---|
| confirm_rtt > 20ms (broker load) | Scale consumer to drain broker faster | Reduces broker congestion |
| SKIP LOCKED taking > 10ms | VACUUM ANALYZE outbox table; check index health | 2–5× claim speed |
| Relay at batch ceiling, broker healthy | Double `RELAY_BATCH_SIZE` | ~2× throughput |
| Single relay instance at ceiling | Add second relay instance | ~1.8× throughput (80% efficiency) |
| Event loop saturated (relay in-process) | Move relay to separate process | Removes event loop sharing |

---

## Bottleneck Analysis B: Consumer Throughput

**When to use:** `messaging.work` queue depth is rising while relay rate is healthy.

### Step 1: Measure consumer processing rate

```promql
# Consumer throughput
rate(messages_processed_total[30s])

# Consumer atomic transaction latency
histogram_quantile(0.99, rate(consumer_atomic_tx_duration_ms_bucket[30s]))

# Prefetch utilization
# If all prefetch slots are in use, the consumer is at its in-flight limit
rabbitmq_channel_prefetch_count{queue="messaging.work"}
rabbitmq_channel_messages_unacknowledged{queue="messaging.work"}
```

### Step 2: Identify the slow step within the consumer

The consumer pipeline: `Zod → schema dispatch → recordAttempt → atomic TX → ack`

```bash
# Which span is slowest? Check Jaeger top slow operations panel
# Expected order: atomic_tx > recordAttempt > schema_validation > schema_dispatch

# If atomic_tx is > 20ms p99: PostgreSQL under write pressure
# Check: pg_stat_activity for long-running transactions
# Check: pg_locks for lock waits on processed_events UNIQUE index
psql $DATABASE_URL -c "
SELECT wait_event_type, wait_event, COUNT(*)
FROM pg_stat_activity
WHERE state = 'active'
GROUP BY 1, 2
ORDER BY 3 DESC;"
```

### Step 3: Scaling recommendations

| Scenario | Action | Expected Improvement |
|---|---|---|
| Prefetch = 10, all slots used | Increase `AMQP_PREFETCH` to 20–50 | 2–5× consumer throughput |
| atomic_tx p99 > 20ms | Increase PostgreSQL connection pool | Reduces TX wait time |
| processed_events index contention | Partition processed_events by date | Reduces index hot spots |
| Single consumer at CPU ceiling | Add second consumer instance | ~1.9× throughput |

---

## Capacity Planning Model

### Current configuration baseline (single instance of each component)

```
Sustainable throughput:  ~200–250 msg/s
Peak throughput (burst): ~400–500 msg/s (for up to 5 minutes before outbox accumulates)
End-to-end p50 latency:  ~150ms (includes ~120ms relay poll gap average)
End-to-end p99 latency:  ~1,200ms (worst-case relay poll gap: 5s, less broker/consumer time)
```

### Scaling projection

| Configuration | Throughput | p99 E2E | Notes |
|---|---|---|---|
| 1 gateway, 1 relay, 1 consumer | 250 msg/s | 1.2s | Current default |
| 1 gateway, 2 relays, 1 consumer | 450 msg/s | 1.2s | Relay bottleneck lifted |
| 1 gateway, 2 relays, 2 consumers | 900 msg/s | 1.2s | Consumer bottleneck lifted |
| 2 gateways, 4 relays, 4 consumers | ~2,000 msg/s | 1.2s | DB becomes bottleneck |
| 2 gateways, 4 relays, 4 consumers + PG read replica | ~3,000 msg/s | 1.2s | Practical ceiling |

E2E p99 latency does not improve with horizontal scaling because the relay poll interval (5s ceiling) dominates. To reduce p99: implement `LISTEN`/`NOTIFY` for hot-path relay triggering.

### Database capacity planning

```
processed_events growth rate:
  At 250 msg/s → 250 rows/s → 21.6M rows/day → 7.9B rows/year (without purge)
  Row size: ~100 bytes → 790GB/year raw
  With purge (30-day retention): ~648M rows maintained → ~65GB

gateway_outbox_events growth rate:
  At 250 msg/s → 250 rows/s consumed, same added → stable if relay keeps up
  Table grows when relay lags: monitor pg_total_relation_size()

event_attempts growth rate:
  At 250 msg/s → 250 rows/s → but most events succeed on attempt 1
  Actual growth ≈ 5-10% of processed_events rate (retry fraction)
  With 30-day retention: manageable

Recommended: Add TTL-based purge jobs before exceeding 10M rows in any table.
```
