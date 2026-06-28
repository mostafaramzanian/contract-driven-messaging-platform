# Benchmark Report — Relay Horizontal Scaling

| Field | Value |
|---|---|
| **Report ID** | RHS-001 |
| **Scenario** | `05-relay-scalability.js` |
| **Status** | <!-- PENDING_EXECUTION → replace with: PASS / FAIL / PARTIAL --> |
| **Executed by** | <!-- engineer name --> |
| **Executed on** | <!-- YYYY-MM-DD HH:MM UTC --> |
| **Git commit** | <!-- git rev-parse --short HEAD --> |
| **Duration** | 19 minutes (5m × 3 stages + 2m × 2 transition gaps) |

---

## Purpose

Measure how relay throughput scales as relay instances are added horizontally. The outbox relay uses `SELECT ... FOR UPDATE SKIP LOCKED` combined with a fencing token (`lock_version`) to safely support concurrent instances without distributed locking. This test quantifies:

- The throughput ceiling for a single relay instance (the foundation for all scaling projections)
- The scaling efficiency at 2 and 4 relay instances (actual / theoretical)
- The fencing token activity under multi-instance concurrency (confirms safety mechanism is engaged)
- Whether any DLQ events occur under relay contention (confirms no data loss under concurrency)

**Scaling efficiency** is the most important output of this report. Below 70% at 4 instances, the bottleneck has shifted from relay compute to the database's `SKIP LOCKED` contention or the broker's confirm throughput — and adding more relay instances will produce diminishing returns.

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
| Docker Compose | <!-- docker compose version --> |
| PostgreSQL | <!-- psql --version --> |
| RabbitMQ | <!-- rabbitmq-diagnostics server_version --> |
| k6 | <!-- k6 version --> |

### Relay instance scaling method

```
<!-- Describe how relay instances were scaled for each stage: -->
Stage A (1 instance):  docker compose up -d gateway-service
Stage B (2 instances): docker compose up -d --scale gateway-service=2
Stage C (4 instances): docker compose up -d --scale gateway-service=4
```

---

## Test Configuration

| Parameter | Value |
|---|---|
| k6 scenario file | `perf/k6/scenarios/05-relay-scalability.js` |
| Stage A — relay instances / publish rate | 1 instance / 300 msg/s |
| Stage B — relay instances / publish rate | 2 instances / 600 msg/s |
| Stage C — relay instances / publish rate | 4 instances / 1,200 msg/s |
| Stage duration | 5 minutes each |
| Transition gap | 1 minute (for relay scaling) |
| Relay batch size | <!-- RELAY_BATCH_SIZE --> |
| Relay poll interval | <!-- RELAY_POLL_INTERVAL_MS --> |
| Stale-lock reaper TTL | <!-- STALE_LOCK_TTL_MS --> |
| Consumer prefetch | <!-- AMQP_PREFETCH --> |

---

## Key Metrics

### Stage A — 1 relay instance at 300 msg/s

| Metric | Value | Notes |
|---|---|---|
| Actual relay throughput (msg/s) | `<!-- MEASURED -->` | Should be near BT-001 ceiling |
| Outbox pending at end of stage (rows) | `<!-- MEASURED -->` | Rising = relay below 300 msg/s ceiling |
| Relay latency p50 (ms) | `<!-- MEASURED -->` | |
| Relay latency p95 (ms) | `<!-- MEASURED -->` | |
| Relay latency p99 (ms) | `<!-- MEASURED -->` | |
| Fencing events (total) | `<!-- MEASURED -->` | Expected: 0 (single instance) |
| Stale-lock reaper fires (total) | `<!-- MEASURED -->` | Expected: 0 (single instance) |
| DLQ events | `<!-- MEASURED -->` | Must be 0 |
| Single-instance ceiling (msg/s) | `<!-- MEASURED -->` | = avg relay throughput if backlog stable |

### Stage B — 2 relay instances at 600 msg/s

| Metric | Value | Notes |
|---|---|---|
| Actual relay throughput (msg/s) | `<!-- MEASURED -->` | |
| Theoretical throughput (2× Stage A) | `<!-- CALCULATED = 2 × Stage A ceiling -->` | |
| **Scaling efficiency** | `<!-- MEASURED / THEORETICAL × 100 -->%` | Expected: 80–90% |
| Outbox pending at end of stage (rows) | `<!-- MEASURED -->` | Should be < Stage A pending |
| Relay latency p99 (ms) | `<!-- MEASURED -->` | May rise slightly due to SKIP LOCKED contention |
| Fencing events total | `<!-- MEASURED -->` | Expected: low but > 0 (normal under 2 instances) |
| Fencing events / relay instance / min | `<!-- MEASURED / 2 / 5 -->` | Expected: < 2/min/instance |
| Stale-lock reaper fires | `<!-- MEASURED -->` | Expected: correlated with fencing spikes |
| DLQ events | `<!-- MEASURED -->` | Must be 0 — fencing protects against data loss |

### Stage C — 4 relay instances at 1,200 msg/s

| Metric | Value | Notes |
|---|---|---|
| Actual relay throughput (msg/s) | `<!-- MEASURED -->` | |
| Theoretical throughput (4× Stage A) | `<!-- CALCULATED = 4 × Stage A ceiling -->` | |
| **Scaling efficiency** | `<!-- MEASURED / THEORETICAL × 100 -->%` | Expected: 70–80% |
| Outbox pending at end of stage (rows) | `<!-- MEASURED -->` | |
| Relay latency p99 (ms) | `<!-- MEASURED -->` | May rise vs Stage B due to higher SKIP LOCKED contention |
| Fencing events total | `<!-- MEASURED -->` | Expected: higher than Stage B |
| Fencing events / relay instance / min | `<!-- MEASURED / 4 / 5 -->` | Expected: < 5/min/instance |
| Stale-lock reaper fires | `<!-- MEASURED -->` | |
| DLQ events | `<!-- MEASURED -->` | Must be 0 |

### Scaling efficiency summary

| Stage | Instances | Throughput | Theoretical | Efficiency | Assessment |
|---|---|---|---|---|---|
| A | 1 | `<!-- MEASURED -->` msg/s | — | — | Baseline |
| B | 2 | `<!-- MEASURED -->` msg/s | `<!-- 2× Stage A -->` msg/s | `<!-- CALC -->%` | <!-- PASS ≥80% / FAIL --> |
| C | 4 | `<!-- MEASURED -->` msg/s | `<!-- 4× Stage A -->` msg/s | `<!-- CALC -->%` | <!-- PASS ≥70% / FAIL --> |

### Fencing token activity summary

| Stage | Total fencing events | Per instance/min | Stale-lock reaper fires | Correlation observed? |
|---|---|---|---|---|
| A (1 relay) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED %>` | N/A |
| B (2 relays) | `<!-- MEASURED -->` | `<!-- MEASURED %>` | `<!-- MEASURED %>` | `<!-- YES / NO -->` |
| C (4 relays) | `<!-- MEASURED %>` | `<!-- MEASURED %>` | `<!-- MEASURED %>` | `<!-- YES / NO -->` |

> **Interpretation:** Fencing events are expected and correct under multi-relay concurrency.
> They indicate the fencing token mechanism is detecting stale claims and logging them.
> A fencing event means: a stale relay instance attempted to call `markSent()` after
> another relay had already claimed the row. No data was lost — the event was already
> published by the second relay. The `lock_version` CAS check prevented the stale
> `markSent()` from succeeding silently.

### Resource utilization per stage

| Resource | Stage A | Stage B | Stage C |
|---|---|---|---|
| gateway-service CPU (all instances combined) (%) | `<!-- MEASURED %>` | `<!-- MEASURED %>` | `<!-- MEASURED %>` |
| postgres CPU (%) | `<!-- MEASURED %>` | `<!-- MEASURED %>` | `<!-- MEASURED %>` |
| postgres active connections | `<!-- MEASURED %>` | `<!-- MEASURED %>` | `<!-- MEASURED %>` |
| rabbitmq CPU (%) | `<!-- MEASURED %>` | `<!-- MEASURED %>` | `<!-- MEASURED %>` |
| rabbitmq MEM (MB) | `<!-- MEASURED %>` | `<!-- MEASURED %>` | `<!-- MEASURED %>` |

---

## Grafana Screenshots

| # | Panel | Dashboard | Capture timing | What to look for |
|---|---|---|---|---|
| 1 | Relay throughput over full 19m | `cdmp-load-testing` | Full test | Step increases at each stage transition |
| 2 | Relay Scalability — throughput vs fencing events | `cdmp-load-testing` | Full test | Fencing rate rises with instance count — this is correct |
| 3 | Outbox pending (full test) | `cdmp-outbox-health` | Full test | Should decrease or stabilize at each higher stage |
| 4 | Lock contention + stale-lock reaper correlation | `cdmp-outbox-health` | Stage B and C | Fencing spike should correlate with reaper spike at same timestamp |
| 5 | Relay latency p99 | `cdmp-outbox-health` | Per stage | Slight increase across stages expected (SKIP LOCKED contention) |
| 6 | Queue depths — work/DLQ | `cdmp-load-testing` | Full test | DLQ must remain 0 throughout all stages |
| 7 | PostgreSQL connections | `cdmp-load-testing` | Per stage | Connection count rises with relay instances — confirm pool headroom |

```
<!-- SCREENSHOT PLACEHOLDER -->
![Relay Scaling — Throughput by Stage](../docs/screenshots/rhs-001-throughput-stages.png)
*Fig 1. Relay throughput over 19 minutes showing step increases at Stage B (t=6m) and Stage C (t=12m).
Stage A: [MEASURED] msg/s → Stage B: [MEASURED] msg/s → Stage C: [MEASURED] msg/s.
Scaling efficiency: B=[MEASURED]%, C=[MEASURED]%.*

<!-- SCREENSHOT PLACEHOLDER -->
![Relay Scaling — Fencing Events by Stage](../docs/screenshots/rhs-001-fencing.png)
*Fig 2. Fencing events per second across stages.
Rising fencing rate with instance count confirms the mechanism is engaged and protecting
against stale-claim races. Zero DLQ events confirms no data loss under contention.*

<!-- SCREENSHOT PLACEHOLDER -->
![Relay Scaling — Fencing + Reaper Correlation](../docs/screenshots/rhs-001-fencing-reaper.png)
*Fig 3. Correlated fencing token spikes and stale-lock reaper activity during Stage C.
This is the expected diagnostic signature: reaper resets a stale claim, new relay publishes it,
stale relay's markSent() CAS fails and is logged as a fencing event.*

<!-- SCREENSHOT PLACEHOLDER -->
![Relay Scaling — DLQ Empty Throughout](../docs/screenshots/rhs-001-dlq-empty.png)
*Fig 4. DLQ queue depth = 0 across all three stages.
Confirms that the fencing token + idempotent consumer combination prevents data loss
under multi-relay concurrent claim contention.*
```

---

## Bottleneck Analysis

### Scaling efficiency interpretation

```
<!-- FILL AFTER EXECUTION -->
Stage A → B efficiency: [MEASURED]%
  [If ≥ 80%: "Linear scaling holds at 2 instances. Relay is the dominant component."]
  [If < 80%: "Scaling efficiency below target at 2 instances. Possible causes:
              a. PostgreSQL SKIP LOCKED contention: check pg_stat_activity for lock waits
              b. Broker confirm throughput ceiling: check rabbitmq CPU and queue rates
              c. Stale-lock reaper TTL too short: check reaper fires correlating with fencing"]

Stage B → C efficiency: [MEASURED]%
  [If ≥ 70%: "Acceptable. Relay remains the dominant component at 4 instances."]
  [If < 70%: "Bottleneck has shifted. At [MEASURED]% efficiency with 4 instances:
              - PostgreSQL: check SKIP LOCKED query plan at 4× claim rate
              - Broker: check rabbitmq_channel_messages_unacknowledged
              - Network: check Docker network throughput between relay instances and broker"]
```

### Fencing token health assessment

```
<!-- FILL AFTER EXECUTION -->
Stage B fencing rate:         [MEASURED] events/instance/min
Stage C fencing rate:         [MEASURED] events/instance/min

Acceptable threshold:          < 5 events/instance/min
Result:                        [HEALTHY / ELEVATED — investigate stale-lock reaper TTL]

Stale-lock reaper TTL:         [STALE_LOCK_TTL_MS]ms
Observed broker confirm p99:   [MEASURED]ms
Margin (TTL / confirm p99):    [STALE_LOCK_TTL_MS / MEASURED]× (recommended: ≥ 10×)
Assessment:                    [SUFFICIENT / INSUFFICIENT — reaper firing on active claims]
```

### SKIP LOCKED contention at scale

```
<!-- FILL AFTER EXECUTION -->
PostgreSQL SKIP LOCKED query latency (Stage A):  [MEASURED]ms avg
PostgreSQL SKIP LOCKED query latency (Stage B):  [MEASURED]ms avg
PostgreSQL SKIP LOCKED query latency (Stage C):  [MEASURED]ms avg

Latency increase Stage A → C: [MEASURED]ms ([MEASURED]%)
Assessment: [NEGLIGIBLE / SIGNIFICANT — table scan cost increasing with concurrent claimer count]

If significant: consider indexing on (status, next_retry_at, lock_version) or
partitioning gateway_outbox_events by created_at.
```

---

## Raw k6 Output

```
<!-- PASTE k6 SUMMARY OUTPUT HERE -->
```

---

## Conclusions

### Horizontal scaling summary

```
<!-- FILL AFTER EXECUTION -->
| Instances | Throughput | Efficiency | Fencing/inst/min | DLQ events | Verdict |
|---|---|---|---|---|---|
| 1 | [MEASURED] msg/s | —      | 0                | 0 | Baseline |
| 2 | [MEASURED] msg/s | [MEAS%]| [MEASURED]       | 0 | [PASS/FAIL] |
| 4 | [MEASURED] msg/s | [MEAS%]| [MEASURED]       | 0 | [PASS/FAIL] |
```

### Throughput ceiling at each configuration

```
<!-- FILL AFTER EXECUTION -->
Single relay instance:         ~[MEASURED] msg/s sustainable (BT-001 + RHS-001 Stage A)
Two relay instances:           ~[MEASURED] msg/s sustainable
Four relay instances:          ~[MEASURED] msg/s sustainable

To reach 1,000 msg/s:          [CALCULATED number of instances] relay instances required
  Basis: [MEASURED] msg/s per instance × [EFFICIENCY]% efficiency × N = 1,000
  Solve: N = [CALC]
```

### Safety assessment under concurrency

```
<!-- FILL AFTER EXECUTION -->
DLQ events across all stages:  0
Data loss events:               0
Fencing mechanism:              [ENGAGED AND CORRECT / NOT ENGAGED — investigate]
Stale-lock reaper:              [FIRING CORRECTLY / OVER-FIRING — adjust TTL / NOT FIRING — investigate]

Conclusion: Horizontal relay scaling to [N] instances is [SAFE / UNSAFE] under the
current configuration. The fencing token mechanism [CORRECTLY PREVENTS / DOES NOT PREVENT]
data loss under concurrent relay claim races.
```

### Recommendations

```
<!-- FILL AFTER EXECUTION -->
Production deployment recommendation:
  Instances:   [N] relay instances
  Basis:       Supports [MEASURED] msg/s at [EFFICIENCY]% efficiency with zero DLQ events.

Priority optimizations before scaling beyond [N] instances:
1. [EVIDENCE-BASED RECOMMENDATION — e.g. "Move relay to separate process: eliminates
   event-loop sharing, expected to add [X]% throughput per instance."]
   Evidence: Stage A relay rate [MEASURED] msg/s vs single-process ceiling calc [MEASURED] msg/s.

2. [EVIDENCE-BASED RECOMMENDATION]
   Evidence: [SPECIFIC MEASUREMENT]
```
