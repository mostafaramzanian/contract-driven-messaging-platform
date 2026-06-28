# Benchmark Report — Sustained Load

| Field | Value |
|---|---|
| **Report ID** | SL-001 |
| **Scenario** | `02-ramp-to-peak.js` (sustained phase: held at saturation) |
| **Status** | <!-- PENDING_EXECUTION → replace with: PASS / FAIL / PARTIAL --> |
| **Executed by** | <!-- engineer name --> |
| **Executed on** | <!-- YYYY-MM-DD HH:MM UTC --> |
| **Git commit** | <!-- git rev-parse --short HEAD --> |
| **Duration** | 30 minutes (20m ramp + 10m hold) |

---

## Purpose

Validate system behavior under continuous operation at or near the saturation point identified in BT-001. Sustained load tests expose failure modes that do not appear in short burst tests: memory leaks in the relay or consumer, gradual outbox table bloat, increasing atomic TX latency as `processed_events` grows, and PostgreSQL connection pool exhaustion over time.

---

## Test Environment

### Host

| Parameter | Value |
|---|---|
| Machine type | <!-- e.g. MacBook Pro M2 / AWS c5.2xlarge --> |
| OS | <!-- e.g. macOS 14.2 / Ubuntu 22.04 LTS --> |
| CPU | <!-- e.g. Apple M2 Pro 10-core --> |
| CPU allocated to Docker | <!-- e.g. 6 cores --> |
| Total RAM | <!-- e.g. 32 GB --> |
| RAM allocated to Docker | <!-- e.g. 8 GB --> |

### Software versions

| Component | Version |
|---|---|
| Docker Engine | <!-- docker --version --> |
| Node.js (gateway) | <!-- node --version --> |
| Node.js (messaging) | <!-- node --version --> |
| PostgreSQL | <!-- psql --version --> |
| RabbitMQ | <!-- rabbitmq-diagnostics server_version --> |
| k6 | <!-- k6 version --> |

---

## Test Configuration

| Parameter | Value |
|---|---|
| k6 scenario file | `perf/k6/scenarios/02-ramp-to-peak.js` |
| Ramp profile | 10 msg/s → 2,000 msg/s over 20 minutes (linear) |
| Hold at saturation | 10 minutes at rate identified from BT-001 |
| Abort threshold | `p99 > 2000ms` OR `dlq_events > 0` OR `error_rate > 5%` |
| Max VUs | 1,000 |
| Payload | v2, ~412 bytes |
| Relay batch size | <!-- RELAY_BATCH_SIZE --> |
| Relay poll interval | <!-- RELAY_POLL_INTERVAL_MS --> |
| Consumer prefetch | <!-- AMQP_PREFETCH --> |
| PostgreSQL pool | <!-- DB_POOL_SIZE --> |

---

## Key Metrics

### Saturation point

| Measurement | Value |
|---|---|
| Traffic rate at saturation onset | `<!-- MEASURED -->` msg/s |
| First saturating component | `<!-- MEASURED -->` — gateway / relay / consumer / database |
| Observable signal at saturation | `<!-- MEASURED -->` — e.g. `outbox_pending_events > 200` |
| Time from ramp start to saturation | `<!-- MEASURED -->` minutes |
| Abort triggered? | <!-- YES (reason) / NO --> |

### Sustained phase (10m at saturation rate)

#### Throughput stability

| Metric | Start of hold | Mid hold (t+5m) | End of hold (t+10m) | Trend |
|---|---|---|---|---|
| Publish rate (msg/s) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | <!-- STABLE / DEGRADING --> |
| Relay rate (msg/s) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | <!-- STABLE / DEGRADING --> |
| Consumer rate (msg/s) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | <!-- STABLE / DEGRADING --> |

#### Latency over the hold window

| Metric | Start of hold | Mid hold | End of hold | Δ (drift) |
|---|---|---|---|---|
| Gateway p50 (ms) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` |
| Gateway p99 (ms) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` |
| Relay p99 (ms) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` |
| Atomic TX p99 (ms) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` |

> **Latency drift** is the difference between end and start values. Positive drift indicates the system is degrading under sustained load — a sign of table bloat, connection pool saturation, or memory pressure accumulating over time.

#### Resource utilization drift (sustained phase)

| Resource | t=0 (hold start) | t=5m | t=10m | Drift |
|---|---|---|---|---|
| gateway-service CPU (%) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` |
| gateway-service MEM (MB) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` |
| messaging-service CPU (%) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` |
| messaging-service MEM (MB) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` |
| postgres active connections | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` |
| postgres MEM (MB) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` |
| rabbitmq MEM (MB) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` |

#### Reliability counters (sustained phase totals)

| Metric | Total during 10m hold |
|---|---|
| DLQ events | `<!-- MEASURED -->` (threshold: 0) |
| Publisher confirm failures | `<!-- MEASURED -->` (threshold: 0) |
| Retry exhaustions | `<!-- MEASURED -->` |
| Fencing token fires | `<!-- MEASURED -->` |
| Idempotency duplicates caught | `<!-- MEASURED -->` |
| Orphan trace spans | `<!-- MEASURED -->` |

#### Database table growth during sustained hold

| Table | Rows at hold start | Rows at hold end | Growth rate (rows/s) |
|---|---|---|---|
| `gateway_outbox_events` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` |
| `processed_events` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` |
| `event_attempts` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` |
| `messages` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` |

```sql
-- Query used to capture table row counts at hold start and end:
SELECT relname AS table, n_live_tup AS rows
FROM pg_stat_user_tables
WHERE relname IN (
  'gateway_outbox_events', 'processed_events', 'event_attempts', 'messages'
)
ORDER BY relname;
```

---

## Grafana Screenshots

| # | Panel | Dashboard | What to look for |
|---|---|---|---|
| 1 | Throughput — Gateway vs Relay vs Consumer (full 30m window) | `cdmp-load-testing` | Ramp shape + plateau behavior. Look for divergence at saturation. |
| 2 | Gateway Latency Percentiles (full 30m) | `cdmp-load-testing` | Identify the traffic level at which p99 crosses each threshold. |
| 3 | Outbox Backlog — 30m window | `cdmp-load-testing` | Shows the inflection point where relay falls behind. |
| 4 | PostgreSQL Performance (connections + inserts/s) | `cdmp-load-testing` | Any upward drift in active connections during the hold phase. |
| 5 | Resource utilization — CPU and memory (all services) | Grafana Node Exporter | Confirm no memory leak across the 30m window. |
| 6 | Queue Depths — work/retry/DLQ | `cdmp-load-testing` | Work queue should be near zero if consumer is keeping up. |
| 7 | Relay Scalability panel (fencing events/s) | `cdmp-load-testing` | Should be near zero with single relay instance. |

```
<!-- SCREENSHOT PLACEHOLDER -->
![Sustained Load — 30m Throughput](../docs/screenshots/sl-001-throughput-30m.png)
*Fig 1. Full 30-minute window: 20-minute ramp (0→2,000 msg/s) and 10-minute hold.
Saturation point visible at [MEASURED] msg/s where relay line diverges from gateway line.*

<!-- SCREENSHOT PLACEHOLDER -->
![Sustained Load — Latency Drift](../docs/screenshots/sl-001-latency-drift.png)
*Fig 2. Gateway and consumer p99 latency during the 10-minute hold phase.
[STABLE / DRIFTING UPWARD] behavior indicates [STABLE OPERATION / ACCUMULATING DEGRADATION].*

<!-- SCREENSHOT PLACEHOLDER -->
![Sustained Load — Memory Over Time](../docs/screenshots/sl-001-memory.png)
*Fig 3. Gateway and messaging service heap memory during the sustained hold.
Flat profile = no memory leak. Rising slope = investigate with heap snapshot.*
```

---

## Bottleneck Analysis

### Saturation sequence observed

```
<!-- FILL AFTER EXECUTION -->

t=[MEASURED]m: outbox_pending_events crossed 200 at [MEASURED] msg/s.
               First bottleneck confirmed as: [RELAY / CONSUMER / DATABASE]

t=[MEASURED]m: [NEXT_SIGNAL] observed. Second bottleneck confirmed as: [COMPONENT]

t=[MEASURED]m: Abort threshold [FIRED / NOT FIRED].
               Reason: [DESCRIPTION]
```

### Root cause of first saturation

```
<!-- FILL AFTER EXECUTION -->
Component: [COMPONENT]
Rate at saturation: [MEASURED] msg/s
Measured ceiling calculation:

  [E.g. Relay: batch_size=25 / (confirm_p99_ms=12 / 1000) = 2,083 theoretical
   Observed: [MEASURED] msg/s actual — [MEASURED]% of theoretical ceiling
   Gap explained by: [event loop sharing / GC pauses / PostgreSQL claim latency]]
```

### Latency drift assessment

```
<!-- FILL AFTER EXECUTION -->
Gateway p99 drift over 10m hold: [+X ms / stable / improving]
Consumer atomic_tx p99 drift:    [+X ms / stable / improving]

If drift > 10ms over 10 minutes:
  - Check processed_events table size growth (index scan cost increases)
  - Check PostgreSQL shared_buffers hit ratio
  - Check gateway-service heap growth (relay event loop timer drift)
```

### Memory leak assessment

```
<!-- FILL AFTER EXECUTION -->
gateway-service heap drift: [+X MB / stable] over 30 minutes
messaging-service heap drift: [+X MB / stable] over 30 minutes

[CONCLUSION: No memory leak detected / Suspected leak in [COMPONENT] — recommend heap snapshot]
```

---

## Raw k6 Output

```
<!-- PASTE k6 SUMMARY OUTPUT HERE -->
```

---

## Conclusions

### Saturation point summary

```
<!-- FILL AFTER EXECUTION -->
Saturation onset:         [MEASURED] msg/s
First bottleneck:         [COMPONENT]
Bottleneck signal:        [METRIC] crossed [THRESHOLD] at t=[TIME]
Abort triggered:          [YES / NO]
DLQ events at saturation: [COUNT]
```

### Sustained operation assessment

```
<!-- FILL AFTER EXECUTION -->
System stability over 10m hold at [MEASURED] msg/s:
  Throughput:    [STABLE ±X% / DEGRADING at -X% over 10m]
  Latency:       [STABLE ±Xms / DRIFTING +Xms over 10m]
  Memory:        [STABLE / +X MB over 30m — investigate]
  Reliability:   [ZERO DLQ / X DLQ EVENTS — investigate]
```

### Safe operating range (updated)

```
<!-- FILL AFTER EXECUTION — supersedes BT-001 estimate -->
Sustainable production rate: [MEASURED] msg/s
  Basis: [COMPONENT] saturates at [MEASURED] msg/s with [MEASURED] latency drift.
         Conservative operating range = saturation × 0.7 = [CALC] msg/s.

Burst headroom (< 5 minutes): [MEASURED] msg/s
  Basis: Outbox absorbs burst for [MEASURED] minutes at [MEASURED] msg/s overload
         before relay backlog triggers OutboxRelayLagging alert.
```

### Scaling recommendations (evidence-based)

```
<!-- FILL AFTER EXECUTION -->
Priority 1: [ACTION]
  Evidence: [SPECIFIC METRIC AND VALUE]
  Expected impact: +[X]% throughput

Priority 2: [ACTION]
  Evidence: [SPECIFIC METRIC AND VALUE]
  Expected impact: +[X]% throughput
```
