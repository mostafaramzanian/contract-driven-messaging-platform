# Benchmark Report — Baseline Throughput

| Field | Value |
|---|---|
| **Report ID** | BT-001 |
| **Scenario** | `01-baseline-throughput.js` |
| **Status** | <!-- PENDING_EXECUTION → replace with: PASS / FAIL / PARTIAL --> |
| **Executed by** | <!-- engineer name --> |
| **Executed on** | <!-- YYYY-MM-DD HH:MM UTC --> |
| **Git commit** | <!-- git rev-parse --short HEAD --> |
| **Duration** | 22 minutes |

---

## Purpose

Establish the system's steady-state performance characteristics at three traffic levels — low (50 msg/s), medium (200 msg/s), and high (500 msg/s) — across all pipeline stages: gateway HTTP handler, outbox relay, RabbitMQ broker, and consumer atomic transaction. Results from this report define the safe operating range for production traffic planning.

---

## Test Environment

### Host

| Parameter | Value |
|---|---|
| Machine type | <!-- e.g. MacBook Pro M2 / AWS c5.2xlarge / bare metal --> |
| OS | <!-- e.g. macOS 14.2 / Ubuntu 22.04 LTS --> |
| CPU | <!-- e.g. Apple M2 Pro 10-core / Intel Xeon 8× vCPU --> |
| CPU allocated to Docker | <!-- e.g. 6 cores --> |
| Total RAM | <!-- e.g. 32 GB --> |
| RAM allocated to Docker | <!-- e.g. 8 GB --> |
| Disk | <!-- e.g. 512 GB NVMe SSD --> |
| Network (loopback) | <!-- e.g. localhost, no network hops --> |

### Software versions

| Component | Version |
|---|---|
| Docker Engine | <!-- docker --version --> |
| Docker Compose | <!-- docker compose version --> |
| Node.js (gateway) | <!-- node --version inside container --> |
| Node.js (messaging) | <!-- node --version inside container --> |
| PostgreSQL | <!-- psql --version --> |
| RabbitMQ | <!-- rabbitmq-diagnostics server_version --> |
| k6 | <!-- k6 version --> |
| NestJS | <!-- from package.json --> |

### Docker resource limits (from `docker-compose.yml`)

| Service | CPU limit | Memory limit |
|---|---|---|
| `gateway-service` | <!-- e.g. 1.0 --> | <!-- e.g. 512m --> |
| `messaging-service` | <!-- e.g. 1.0 --> | <!-- e.g. 512m --> |
| `postgres` | <!-- e.g. 1.0 --> | <!-- e.g. 1g --> |
| `rabbitmq` | <!-- e.g. 0.5 --> | <!-- e.g. 512m --> |

---

## Test Configuration

| Parameter | Value |
|---|---|
| k6 scenario file | `perf/k6/scenarios/01-baseline-throughput.js` |
| Load levels | 50 msg/s · 200 msg/s · 500 msg/s |
| Duration per level | 5 minutes (+ 2 min warmup at 50 msg/s) |
| Total test duration | 22 minutes |
| Executor type | `constant-arrival-rate` |
| Pre-allocated VUs | 20 (low) · 50 (medium) · 100 (high) |
| Max VUs | 50 (low) · 120 (medium) · 300 (high) |
| Message schema version | v2 |
| Payload size (uncompressed) | <!-- e.g. 412 bytes average --> |
| Queue — work | `messaging.work` · durable · manual ACK |
| Queue — retry | `messaging.retry.q` · per-message TTL · 2ⁿ×2s |
| Queue — DLQ | `messaging.dlq` · durable |
| Relay poll interval | <!-- RELAY_POLL_INTERVAL_MS env value, e.g. 5000ms --> |
| Relay batch size | <!-- RELAY_BATCH_SIZE env value, e.g. 25 --> |
| Consumer prefetch | <!-- AMQP_PREFETCH env value, e.g. 10 --> |
| PostgreSQL pool size | <!-- DB_POOL_SIZE env value, e.g. 10 --> |
| Max retry attempts | <!-- MAX_RETRY_ATTEMPTS env value, e.g. 5 --> |

---

## Key Metrics

### Gateway HTTP layer

| Metric | 50 msg/s | 200 msg/s | 500 msg/s | Threshold |
|---|---|---|---|---|
| Actual throughput (msg/s) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | = target ±5% |
| HTTP 202 rate (%) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | ≥ 99.9% |
| HTTP error rate (%) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | < 0.1% |
| Latency p50 (ms) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | < 30 · < 50 · < 100 |
| Latency p95 (ms) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | — |
| Latency p99 (ms) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | < 100 · < 200 · < 500 |
| Latency max (ms) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | — |

### Outbox relay

| Metric | 50 msg/s | 200 msg/s | 500 msg/s | Threshold |
|---|---|---|---|---|
| Relay throughput (msg/s) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | ≈ publish rate |
| Relay latency p50 (ms) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | < 10 |
| Relay latency p95 (ms) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | < 25 |
| Relay latency p99 (ms) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | < 50 |
| Outbox peak pending (rows) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | < 100 · < 200 · < 500 |
| Fencing events (total) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | < 5 |
| Publisher confirm failures | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | 0 |

### Consumer

| Metric | 50 msg/s | 200 msg/s | 500 msg/s | Threshold |
|---|---|---|---|---|
| Consumer throughput (msg/s) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | ≈ publish rate |
| Atomic TX latency p50 (ms) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | < 8 |
| Atomic TX latency p95 (ms) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | < 20 |
| Atomic TX latency p99 (ms) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | < 40 |
| messaging.work queue depth (avg) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | < 30 |
| messaging.work queue depth (peak) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | < 100 |
| DLQ events (total) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | 0 |
| Idempotency hits (total) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | — |

### End-to-end delivery

| Metric | 50 msg/s | 200 msg/s | 500 msg/s | Threshold |
|---|---|---|---|---|
| E2E delivery p50 (ms) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | — |
| E2E delivery p95 (ms) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | — |
| E2E delivery p99 (ms) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | < 2000 |

> **Note on E2E latency:** The dominant component of E2E p99 is the outbox relay poll gap (0–5s window, average ~2.5s). This is inherent to the polling-based relay design and is not a performance defect. True relay-side latency (Zod validation + outbox INSERT + AMQP publish + confirm) is captured separately in the relay latency rows above.

### Resource utilization

| Resource | 50 msg/s | 200 msg/s | 500 msg/s | Warning threshold |
|---|---|---|---|---|
| gateway-service CPU (%) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | > 70% |
| gateway-service MEM (MB) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | > 400 MB |
| messaging-service CPU (%) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | > 70% |
| messaging-service MEM (MB) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | > 400 MB |
| postgres CPU (%) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | > 60% |
| postgres MEM (MB) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | > 800 MB |
| rabbitmq CPU (%) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | > 40% |
| rabbitmq MEM (MB) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | > 300 MB |

---

## Grafana Screenshots

Embed the following screenshots captured during the test window. Use the **Load Testing & Capacity Planning** dashboard (`cdmp-load-testing`) unless noted.

| # | Panel | Dashboard | What to look for |
|---|---|---|---|
| 1 | Throughput — Gateway vs Relay vs Consumer | `cdmp-load-testing` | Three lines should track closely. Divergence = relay or consumer lag. |
| 2 | Gateway Latency Percentiles (p50/p95/p99) | `cdmp-load-testing` | Step increases at each load level. p99 must stay below threshold. |
| 3 | Outbox Backlog — Accumulation and Drain | `cdmp-load-testing` | Should stay near zero at 50 and 200 msg/s. May rise at 500 msg/s. |
| 4 | Relay Latency p50/p95/p99 | `cdmp-load-testing` | All three should be stable. p99 spike = broker confirm latency issue. |
| 5 | Queue Depths — work/retry/DLQ | `cdmp-load-testing` | All queues near zero. DLQ must be empty throughout. |
| 6 | Stat: Current Throughput | `cdmp-system-overview` | Confirm reported rate matches k6 target. |
| 7 | Stat: Success rate | `cdmp-system-overview` | ≥ 99.5% throughout all three levels. |
| 8 | PostgreSQL Performance | `cdmp-load-testing` | Active connections and insert rate at each load level. |

```
<!-- SCREENSHOT PLACEHOLDER -->
![Throughput — Baseline Test](../docs/screenshots/bt-001-throughput.png)
*Fig 1. Publish rate (blue), relay rate (green), and consumer rate (purple) over the 22-minute test window.*

<!-- SCREENSHOT PLACEHOLDER -->
![Gateway Latency — Baseline Test](../docs/screenshots/bt-001-gateway-latency.png)
*Fig 2. Gateway latency percentiles. Step increases visible at t=7m (200 msg/s) and t=12m (500 msg/s).*

<!-- SCREENSHOT PLACEHOLDER -->
![Outbox Pending — Baseline Test](../docs/screenshots/bt-001-outbox-pending.png)
*Fig 3. Outbox pending events. Near zero at 50 and 200 msg/s. Note behavior at 500 msg/s.*
```

---

## Bottleneck Analysis

> Fill in this section after execution. The analysis must reference actual measured values, not estimates.

### What saturated first

```
<!-- FILL AFTER EXECUTION -->
At [MEASURED] msg/s, [COMPONENT] showed the first signs of saturation.
Observable signal: [METRIC] increased from [VALUE_A] to [VALUE_B].
```

### Why

```
<!-- FILL AFTER EXECUTION -->
The [COMPONENT] bottleneck at [RATE] msg/s is explained by:

1. [PRIMARY_REASON] — e.g. "Relay confirm round-trip of [MEASURED]ms at p99
   limits the batch throughput to [BATCH_SIZE] / ([P99_MS] / 1000) = [CALC] msg/s."

2. [SECONDARY_REASON] — e.g. "Node.js event loop shared between application
   request handling and relay poll timer. At [RATE] msg/s, event loop lag
   was [MEASURED]ms, delaying relay polls by [MEASURED]ms on average."
```

### Scaling implications

```
<!-- FILL AFTER EXECUTION -->
Based on measurements:
- Single relay instance sustainable ceiling: ~[MEASURED] msg/s
- Consumer sustainable ceiling: ~[MEASURED] msg/s
- PostgreSQL write throughput headroom: [MEASURED] TPS available vs [MEASURED] used

To sustain 500 msg/s: [SPECIFIC_ACTION_BASED_ON_DATA]
To sustain 1000 msg/s: [SPECIFIC_ACTION_BASED_ON_DATA]
```

---

## Raw k6 Output

Paste the full k6 summary output below after execution.

```
<!-- PASTE k6 OUTPUT HERE -->

     ✓ gateway status is 202
     ✓ gateway response has eventId

     checks.........................: XX.XX%  ✓ XXXXXX ✗ XXXX
     data_received..................: XX MB   X.X kB/s
     data_sent......................: XX MB   X.X kB/s

     cdmp_events_accepted...........: XXXXXX
     cdmp_events_failed.............: XXXXXX
     cdmp_gateway_latency_ms........: avg=XX.XXms min=X.XXms med=XX.XXms max=XXXms p(90)=XX.XXms p(95)=XX.XXms
     cdmp_outbox_pending_current....: XX
     cdmp_dlq_events_observed.......: X

     http_req_duration..............: avg=XX.XXms min=X.XXms med=XX.XXms max=XXXms p(90)=XX.XXms p(95)=XX.XXms
     http_req_failed................: XX.XX%  ✓ XXXX ✗ XXXXXX
     iterations.....................: XXXXXX  XX/s
     vus............................: XX      min=XX max=XXX
     vus_max........................: XXX
```

---

## Conclusions

> Fill in after execution. Use measured values only.

### Sustainable throughput

```
<!-- FILL AFTER EXECUTION -->
| Load level | Throughput | Gateway p99 | Relay p99 | Consumer p99 | Assessment |
|---|---|---|---|---|---|
| Low (50 msg/s)    | [MEASURED] | [MEASURED]ms | [MEASURED]ms | [MEASURED]ms | [PASS/FAIL] |
| Medium (200 msg/s)| [MEASURED] | [MEASURED]ms | [MEASURED]ms | [MEASURED]ms | [PASS/FAIL] |
| High (500 msg/s)  | [MEASURED] | [MEASURED]ms | [MEASURED]ms | [MEASURED]ms | [PASS/FAIL] |
```

### Safe operating range

```
<!-- FILL AFTER EXECUTION -->
Based on this test run with [CONFIGURATION]:
- Recommended sustained rate:  [MEASURED] msg/s
- Burst tolerance (< 5 min):   [MEASURED] msg/s
- Hard ceiling (single-node):  [MEASURED] msg/s
```

### Scaling recommendations

```
<!-- FILL AFTER EXECUTION -->
To raise the ceiling to [TARGET] msg/s:
1. [ACTION] — expected impact: +[X] msg/s
2. [ACTION] — expected impact: +[X] msg/s

Evidence basis: [reference to specific measured values that support each recommendation]
```

### Open issues

```
<!-- FILL AFTER EXECUTION -->
- [ ] [Any unexpected behavior observed during the test]
- [ ] [Any metric that was unexpectedly close to its threshold]
- [ ] [Any infrastructure anomaly during the run]
```
