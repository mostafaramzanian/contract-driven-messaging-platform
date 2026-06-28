# Benchmark Report — Peak Load

| Field | Value |
|---|---|
| **Report ID** | PL-001 |
| **Scenario** | `03-outbox-backlog.js` |
| **Status** | <!-- PENDING_EXECUTION → replace with: PASS / FAIL / PARTIAL --> |
| **Executed by** | <!-- engineer name --> |
| **Executed on** | <!-- YYYY-MM-DD HH:MM UTC --> |
| **Git commit** | <!-- git rev-parse --short HEAD --> |
| **Duration** | 20 minutes (5m overload + 10m drain + 5m recovery) |

---

## Purpose

Measure the system's behavior under peak load that deliberately exceeds the relay's sustainable throughput. This test answers three operational questions:

1. At what outbox row count does the relay queue become a DLQ risk (messages approaching their retry budget ceiling)?
2. How fast does the relay drain the outbox backlog after the load peak ends?
3. Does the gateway remain available and responsive throughout the overload window?

The outbox pattern exists precisely to decouple gateway availability from relay throughput. This test validates that decoupling under real overload conditions.

---

## Test Environment

### Host

| Parameter | Value |
|---|---|
| Machine type | <!-- e.g. MacBook Pro M2 / AWS c5.2xlarge --> |
| OS | <!-- e.g. macOS 14.2 / Ubuntu 22.04 LTS --> |
| CPU | <!-- e.g. Apple M2 Pro 10-core --> |
| RAM allocated to Docker | <!-- e.g. 8 GB --> |

### Software versions

| Component | Version |
|---|---|
| Docker Engine | <!-- docker --version --> |
| PostgreSQL | <!-- psql --version --> |
| RabbitMQ | <!-- rabbitmq-diagnostics server_version --> |
| k6 | <!-- k6 version --> |

---

## Test Configuration

| Parameter | Value |
|---|---|
| k6 scenario file | `perf/k6/scenarios/03-outbox-backlog.js` |
| Overload rate | 600 msg/s (target: 2× relay ceiling from BT-001) |
| Overload duration | 5 minutes |
| Drain observation window | 10 minutes (0 VUs) |
| Recovery rate | 100 msg/s |
| Recovery duration | 5 minutes |
| Relay batch size | <!-- RELAY_BATCH_SIZE --> |
| Relay poll interval | <!-- RELAY_POLL_INTERVAL_MS --> |
| Consumer prefetch | <!-- AMQP_PREFETCH --> |

---

## Key Metrics

### Phase 1 — Overload (5 minutes at 600 msg/s)

| Metric | Value | Threshold |
|---|---|---|
| Actual publish rate achieved (msg/s) | `<!-- MEASURED -->` | 600 ±5% |
| Relay drain rate during overload (msg/s) | `<!-- MEASURED -->` | < publish rate (backlog expected) |
| Outbox backlog at end of overload phase (rows) | `<!-- MEASURED -->` | — |
| Backlog growth rate (rows/s) | `<!-- MEASURED -->` | = publish_rate − relay_rate |
| Gateway p99 latency during overload (ms) | `<!-- MEASURED -->` | < 500 |
| Gateway HTTP 202 rate during overload (%) | `<!-- MEASURED -->` | ≥ 99.9% |
| DLQ events during overload phase | `<!-- MEASURED -->` | 0 |
| Retry exhaustions during overload | `<!-- MEASURED -->` | 0 |

> **DLQ risk window calculation:**
> The retry budget is 5 attempts with 2ⁿ×2s backoff: cumulative window = 2+4+8+16+32 = **62 seconds**.
> A message committed at overload start that is not consumed within 62 seconds will exhaust its retry budget.
> At relay drain rate of [MEASURED] msg/s and backlog of [MEASURED] rows, oldest messages
> wait [MEASURED] seconds before delivery. DLQ risk: [YES/NO].

### Phase 2 — Drain (10 minutes, 0 VUs)

| Metric | Value |
|---|---|
| Backlog at drain start (rows) | `<!-- MEASURED -->` (= end of Phase 1) |
| Drain rate (rows/s) | `<!-- MEASURED -->` |
| Time to drain to < 50 pending (seconds) | `<!-- MEASURED -->` |
| Time to drain to 0 pending (seconds) | `<!-- MEASURED -->` |
| DLQ events during drain phase | `<!-- MEASURED -->` |
| Relay latency p99 during drain (ms) | `<!-- MEASURED -->` |

> **Drain rate = relay ceiling without incoming load.** Compare to relay rate during overload phase.
> The difference between drain rate and relay rate during overload reveals the relay's event-loop overhead
> attributable to colocated application traffic.

### Phase 3 — Recovery (5 minutes at 100 msg/s)

| Metric | Value | Threshold |
|---|---|---|
| Gateway p99 latency (ms) | `<!-- MEASURED -->` | < 100 (back to baseline) |
| Relay rate (msg/s) | `<!-- MEASURED -->` | ≈ 100 (steady with publish rate) |
| Outbox pending (rows) | `<!-- MEASURED -->` | < 20 |
| DLQ events during recovery | `<!-- MEASURED -->` | 0 |
| Consumer throughput (msg/s) | `<!-- MEASURED -->` | ≈ 100 |

### Resource utilization at peak

| Resource | Overload phase avg | Drain phase avg | Recovery phase avg |
|---|---|---|---|
| gateway-service CPU (%) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` |
| gateway-service MEM (MB) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` |
| messaging-service CPU (%) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` |
| postgres CPU (%) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` |
| `gateway_outbox_events` table size | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` |

---

## Grafana Screenshots

| # | Panel | Dashboard | Capture timing | What to look for |
|---|---|---|---|---|
| 1 | Outbox Backlog — full 20m window | `cdmp-outbox-health` | Full test | Accumulation slope in Phase 1, drain slope in Phase 2, return to baseline in Phase 3 |
| 2 | Throughput — Gateway vs Relay | `cdmp-load-testing` | Full test | Divergence in Phase 1 = backlog accumulating. Convergence in Phase 2 = drain. |
| 3 | Queue Depths — work/retry/DLQ | `cdmp-load-testing` | Full test | DLQ must remain 0. Retry queue may briefly have traffic. |
| 4 | Gateway Latency Percentiles | `cdmp-load-testing` | Phase 1 only | Gateway must stay responsive even while backlog builds. |
| 5 | Relay Latency p99 | `cdmp-outbox-health` | Phase 2 (drain) | p99 during drain is the relay's ceiling throughput latency. |
| 6 | PostgreSQL — table size | custom query | End of Phase 1 | `gateway_outbox_events` size at peak backlog. |

```
<!-- SCREENSHOT PLACEHOLDER -->
![Peak Load — Backlog Accumulation and Drain](../docs/screenshots/pl-001-backlog.png)
*Fig 1. Outbox pending events over 20 minutes.
Phase 1 (0–5m): rising at [MEASURED] rows/s during 600 msg/s overload.
Phase 2 (5–15m): draining at [MEASURED] rows/s with 0 incoming load.
Phase 3 (15–20m): stable at ~0 pending with 100 msg/s recovery load.*

<!-- SCREENSHOT PLACEHOLDER -->
![Peak Load — Gateway Availability During Overload](../docs/screenshots/pl-001-gateway.png)
*Fig 2. Gateway latency and success rate during overload phase.
Demonstrates that the outbox pattern decouples gateway availability from relay throughput.*

<!-- SCREENSHOT PLACEHOLDER -->
![Peak Load — DLQ Remained Empty](../docs/screenshots/pl-001-dlq.png)
*Fig 3. DLQ queue depth remained at 0 throughout the test.
Confirms no messages exhausted their retry budget during the [MEASURED]-second maximum backlog wait.*
```

---

## Bottleneck Analysis

### Phase 1: Relay as intentional constraint

```
<!-- FILL AFTER EXECUTION -->
At 600 msg/s publish rate, the relay sustained [MEASURED] msg/s drain rate.
Backlog accumulated at [MEASURED] rows/s (= 600 − [MEASURED]).

Peak backlog reached: [MEASURED] rows.
Maximum wait time for oldest message: [MEASURED] seconds.
  → Compared to 62s retry budget ceiling: [SAFE / AT RISK / EXCEEDED]

Gateway remained available throughout: [YES / NO]
  p99 during overload: [MEASURED] ms (threshold: < 500ms)
```

### Phase 2: Relay throughput ceiling without application contention

```
<!-- FILL AFTER EXECUTION -->
Drain rate (no incoming load): [MEASURED] msg/s
Relay rate during overload:    [MEASURED] msg/s
Event-loop overhead from colocated app traffic: [MEASURED] msg/s ([MEASURED]%)

Interpretation:
[E.g. "The relay runs ~X% slower when colocated with the gateway under 600 msg/s application load.
This is the cost of sharing the Node.js event loop. Moving the relay to a separate process
would recover this overhead."]
```

### DLQ risk assessment

```
<!-- FILL AFTER EXECUTION -->
Retry budget window: 62 seconds cumulative (5 attempts, 2ⁿ×2s backoff)
Maximum observed backlog wait: [MEASURED] seconds

DLQ risk threshold: backlog_wait > 62s
Result: [NOT AT RISK / AT RISK — backlog_wait within X% of budget / EXCEEDED]

If at risk: the relay must drain the backlog faster than 62s or the MAX_RETRY_ATTEMPTS
should be increased to provide a larger budget window for sustained overload conditions.
```

---

## Raw k6 Output

```
<!-- PASTE k6 SUMMARY OUTPUT HERE -->
```

---

## Conclusions

### Peak load behavior summary

```
<!-- FILL AFTER EXECUTION -->
| Phase | Duration | Publish rate | Relay rate | Backlog change | DLQ events |
|---|---|---|---|---|---|
| Overload  | 5m  | [MEASURED] msg/s | [MEASURED] msg/s | +[MEASURED] rows | [COUNT] |
| Drain     | 10m | 0 msg/s          | [MEASURED] msg/s | −[MEASURED] rows | [COUNT] |
| Recovery  | 5m  | [MEASURED] msg/s | [MEASURED] msg/s | stable ~0 rows   | [COUNT] |
```

### Operating limits derived from this test

```
<!-- FILL AFTER EXECUTION -->
Maximum safe burst duration at 600 msg/s: [MEASURED] minutes
  Basis: Backlog reaches DLQ risk threshold ([MEASURED] rows) after [MEASURED] minutes.

Relay drain ceiling (single instance, no app traffic): [MEASURED] msg/s
Relay drain ceiling (single instance, under 100 msg/s app load): [MEASURED] msg/s
Gap: [MEASURED] msg/s — cost of in-process relay cohabitation.

Gateway p99 under 2× overload: [MEASURED] ms
  Assessment: [ACCEPTABLE / DEGRADED — investigate outbox INSERT contention]
```

### Recommendations

```
<!-- FILL AFTER EXECUTION -->
1. [EVIDENCE-BASED RECOMMENDATION]
2. [EVIDENCE-BASED RECOMMENDATION]
```
