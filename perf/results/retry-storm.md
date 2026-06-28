# Benchmark Report — Retry Storm

| Field | Value |
|---|---|
| **Report ID** | RS-001 |
| **Scenario** | `04-retry-amplification.js` |
| **Status** | <!-- PENDING_EXECUTION → replace with: PASS / FAIL / PARTIAL --> |
| **Executed by** | <!-- engineer name --> |
| **Executed on** | <!-- YYYY-MM-DD HH:MM UTC --> |
| **Git commit** | <!-- git rev-parse --short HEAD --> |
| **Duration** | 20 minutes (5m baseline + 5m failure + 5m recovery + 5m steady) |

---

## Purpose

Measure the impact of a sustained consumer failure on broker throughput (retry amplification), the effectiveness of the idempotency layer under duplicate delivery, and the time required for the retry queue to drain after the failure is resolved. This test simulates a realistic failure scenario: a transient consumer dependency (database, downstream service) becomes unavailable for five minutes during normal load.

**Retry amplification** is the ratio of total broker throughput to original publish rate:

```
amplification_factor = (publish_throughput + retry_throughput) / publish_throughput
```

At 80% consumer failure rate with 5 retry attempts, the theoretical amplification factor is approximately 4.0×. This test measures the actual factor and its impact on queue depth, consumer latency, and idempotency correctness.

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
| k6 scenario file | `perf/k6/scenarios/04-retry-amplification.js` |
| Normal publish rate | 100 msg/s (all 4 phases) |
| Consumer failure rate | 0% (baseline) · 80% (failure) · 0% (recovery) · 0% (steady) |
| Failure simulation mechanism | Consumer test hook: `force-transient-failure` payload tag |
| Max retry attempts | <!-- MAX_RETRY_ATTEMPTS env value --> |
| Retry backoff | 2ⁿ×2s per attempt (2s, 4s, 8s, 16s, 32s) |
| Relay batch size | <!-- RELAY_BATCH_SIZE --> |
| Consumer prefetch | <!-- AMQP_PREFETCH --> |

---

## Key Metrics

### Phase 1 — Baseline (5 minutes, 0% failure rate)

| Metric | Value | Expected |
|---|---|---|
| Publish rate (msg/s) | `<!-- MEASURED -->` | ~100 |
| Consumer success rate (%) | `<!-- MEASURED -->` | ~100% |
| Retry rate (msg/s) | `<!-- MEASURED -->` | ~0 |
| Amplification factor | `<!-- MEASURED -->` | 1.0 |
| messaging.work queue depth (avg) | `<!-- MEASURED -->` | < 20 |
| messaging.retry.q queue depth | `<!-- MEASURED -->` | 0 |
| DLQ events | `<!-- MEASURED -->` | 0 |

### Phase 2 — Failure (5 minutes, 80% failure rate)

| Metric | Value | Expected range |
|---|---|---|
| Publish rate (msg/s) | `<!-- MEASURED -->` | ~100 |
| Consumer success rate (%) | `<!-- MEASURED -->` | ~20% (80% fail) |
| Retry rate (msg/s) | `<!-- MEASURED -->` | ~320 (80% × 4× attempts) |
| **Amplification factor** | `<!-- MEASURED -->` | **~4.0×** |
| messaging.work queue depth (peak) | `<!-- MEASURED -->` | — |
| messaging.retry.q queue depth (peak) | `<!-- MEASURED -->` | — |
| DLQ events | `<!-- MEASURED -->` | < 10 (budget exhaustion) |
| Idempotency duplicates caught | `<!-- MEASURED -->` | > 0 (retried messages) |
| Idempotency catch rate (%) | `<!-- MEASURED -->` | ≥ 95% |
| Publisher confirm failures | `<!-- MEASURED -->` | 0 |
| Gateway p99 during storm (ms) | `<!-- MEASURED -->` | < 300 |

#### Retry funnel breakdown (Phase 2)

| Attempt | Message count | % of published |
|---|---|---|
| Attempt 1 (first retry) | `<!-- MEASURED -->` | `<!-- MEASURED %>` |
| Attempt 2 | `<!-- MEASURED -->` | `<!-- MEASURED %>` |
| Attempt 3 | `<!-- MEASURED -->` | `<!-- MEASURED %>` |
| Attempt 4 | `<!-- MEASURED -->` | `<!-- MEASURED %>` |
| Attempt 5 (budget exhaust → DLQ) | `<!-- MEASURED -->` | `<!-- MEASURED %>` |

> The funnel should decrease at each attempt level. A flat funnel (similar counts at all attempts)
> indicates the consumer is failing all deliveries unconditionally, including retries — the
> transient failure has become permanent. In this test, attempt 5 events should be near zero
> since the 80% failure rate allows ~20% to succeed at each attempt.

### Phase 3 — Recovery (5 minutes, failure disabled)

| Metric | t=0 (failure off) | t+1m | t+3m | t+5m |
|---|---|---|---|---|
| Retry queue depth | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` |
| Retry rate (msg/s) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` |
| Consumer success rate (%) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED %>` |
| Amplification factor | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED %>` |

> Recovery time is measured as the duration from failure-disabled to amplification_factor returning to 1.0.
> In-flight retries with queued backoff delays will continue to re-arrive even after the failure is resolved.
> A `messaging.retry.q` that was deep at t=0 will continue draining for up to 32 seconds (max backoff).

### Phase 4 — Steady state (5 minutes, 0% failure)

| Metric | Value | Threshold |
|---|---|---|
| Consumer success rate (%) | `<!-- MEASURED -->` | = Phase 1 baseline ±0.5% |
| Retry rate (msg/s) | `<!-- MEASURED -->` | < 0.01 |
| Amplification factor | `<!-- MEASURED -->` | ≤ 1.05 |
| DLQ events (phase 4 only) | `<!-- MEASURED -->` | 0 |

### Resource utilization during failure phase

| Resource | Phase 1 (baseline) | Phase 2 (storm peak) | Δ |
|---|---|---|---|
| messaging-service CPU (%) | `<!-- MEASURED -->` | `<!-- MEASURED -->` | `<!-- MEASURED %>` |
| messaging-service MEM (MB) | `<!-- MEASURED -->` | `<!-- MEASURED %>` | `<!-- MEASURED %>` |
| rabbitmq MEM (MB) | `<!-- MEASURED %>` | `<!-- MEASURED %>` | `<!-- MEASURED %>` |
| postgres active connections | `<!-- MEASURED %>` | `<!-- MEASURED %>` | `<!-- MEASURED %>` |

---

## Grafana Screenshots

| # | Panel | Dashboard | Capture timing | What to look for |
|---|---|---|---|---|
| 1 | Retry Funnel — count by attempt | `cdmp-reliability` | Phase 2 only | Stacked bars decreasing by attempt level |
| 2 | DLQ Growth | `cdmp-reliability` | Full 20m | Any DLQ events during phase 2 |
| 3 | Queue Depths — work/retry/DLQ | `cdmp-load-testing` | Full 20m | retry.q depth during storm, drain during recovery |
| 4 | Idempotency duplicates vs redeliveries | `cdmp-reliability` | Full 20m | Duplicates should track redeliveries |
| 5 | Throughput — Gateway vs Consumer | `cdmp-load-testing` | Full 20m | Consumer drops during failure, gateway stays constant |
| 6 | Consumer success rate | `cdmp-system-overview` | Full 20m | Drop during failure phase, recovery in phase 3 |
| 7 | Relay throughput + retry amplification | `cdmp-load-testing` | Phase 2 | Total broker throughput vs publish rate |

```
<!-- SCREENSHOT PLACEHOLDER -->
![Retry Storm — Amplification Factor](../docs/screenshots/rs-001-amplification.png)
*Fig 1. Retry amplification during the 5-minute failure phase.
Total broker throughput ([MEASURED] msg/s) vs publish rate (100 msg/s) = [MEASURED]× amplification.*

<!-- SCREENSHOT PLACEHOLDER -->
![Retry Storm — Retry Funnel](../docs/screenshots/rs-001-retry-funnel.png)
*Fig 2. Retry count by attempt number during failure phase.
Attempt 1 >> Attempt 2 > Attempt 3 > Attempt 4 >> Attempt 5 (near zero = budget not exhausted).*

<!-- SCREENSHOT PLACEHOLDER -->
![Retry Storm — Idempotency Catch Rate](../docs/screenshots/rs-001-idempotency.png)
*Fig 3. Idempotency duplicates caught vs redelivery events.
High catch rate ([MEASURED]%) confirms the UNIQUE constraint idempotency layer is functioning
correctly under concurrent retry redelivery.*

<!-- SCREENSHOT PLACEHOLDER -->
![Retry Storm — Recovery Timeline](../docs/screenshots/rs-001-recovery.png)
*Fig 4. messaging.retry.q depth during recovery phase.
Drain rate depends on per-message TTL expiry distribution. Queue empties within ~[MEASURED]s.*
```

---

## Bottleneck Analysis

### Amplification factor vs theoretical

```
<!-- FILL AFTER EXECUTION -->
Theoretical amplification at 80% fail rate, 5 attempts:
  Expected: ~4.0× (each message fails 4 times on average before succeeding on 5th)
  Measured: [MEASURED]×
  Delta:    [MEASURED]× ([OVER / UNDER] theoretical)

Explanation of delta:
[E.g. "Measured amplification is lower than theoretical because some messages succeed on
attempt 1 even during the 80% failure window (random selection). The test hook samples
at message ingestion, not per-retry, so the effective failure rate per delivery ≈ [MEASURED]%."]
```

### Idempotency correctness under retry storm

```
<!-- FILL AFTER EXECUTION -->
Total redeliveries during failure phase: [MEASURED]
Idempotency duplicates caught:          [MEASURED]
Catch rate:                             [MEASURED]%

Required: ≥ 95%
Result:   [PASS / FAIL]

[If fail: investigate the race condition scenario — two concurrent deliveries both passing
the UNIQUE check before either commits. This indicates the idempotency INSERT is not
occurring in the same transaction as the business write, violating ADR-004.]
```

### Consumer recovery time

```
<!-- FILL AFTER EXECUTION -->
Retry queue depth at failure end: [MEASURED] messages
Drain duration to < 10 messages:  [MEASURED] seconds
Total recovery window (amplification → 1.0): [MEASURED] seconds

At [MEASURED] msg/s retry queue drain rate, residual retries from the backoff schedule
continue arriving for up to [max_backoff=32]s after the failure is resolved.
[MEASURED]s recovery window is [ACCEPTABLE / EXCESSIVE — consider shorter max backoff].
```

---

## Raw k6 Output

```
<!-- PASTE k6 SUMMARY OUTPUT HERE -->
```

---

## Conclusions

### Retry storm impact summary

```
<!-- FILL AFTER EXECUTION -->
Amplification factor at 80% transient failure: [MEASURED]×
Peak broker throughput during storm:           [MEASURED] msg/s (vs 100 msg/s publish rate)
DLQ events during storm:                       [COUNT] ([ACCEPTABLE / ABOVE THRESHOLD])
Idempotency catch rate:                        [MEASURED]% ([PASS / FAIL])
Consumer recovery time after fix:              [MEASURED] seconds
```

### Retry budget assessment

```
<!-- FILL AFTER EXECUTION -->
At 80% failure rate and 5 attempts, [COUNT] messages exhausted their retry budget.
  → [COUNT] = 0: retry budget sufficient for a 5-minute 80% failure window at 100 msg/s.
  → [COUNT] > 0: budget exhaustion occurred. Evaluate:
       a. Increase MAX_RETRY_ATTEMPTS from 5 to [N]
       b. Increase backoff multiplier to extend total budget window from 62s to [Xs]
       c. Accept DLQ events at this failure severity and rely on RB-001 procedure

Current budget window:  62 seconds (5 attempts: 2+4+8+16+32s)
Required budget window: [MEASURED] seconds (actual max wait observed in Phase 2)
Gap:                    [MEASURED] seconds
```

### Recommendations

```
<!-- FILL AFTER EXECUTION -->
1. [EVIDENCE-BASED RECOMMENDATION]
   Evidence: [SPECIFIC MEASUREMENT]

2. [EVIDENCE-BASED RECOMMENDATION]
   Evidence: [SPECIFIC MEASUREMENT]
```
