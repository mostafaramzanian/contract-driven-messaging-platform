# PM-002 — Outbox Relay Backlog — Delivery Latency Degradation

| Field | Value |
|---|---|
| **Incident ID** | PM-002 |
| **Severity** | SEV-2 |
| **Status** | Resolved |
| **Incident start** | 2024-03-01 14:08 UTC |
| **Incident end** | 2024-03-01 14:55 UTC |
| **Total duration** | 47 minutes |
| **Incident commander** | On-call engineer (rotation: platform team) |
| **Postmortem author** | On-call engineer |
| **Review date** | 2024-03-03 |
| **Related runbook** | [RB-003 — Outbox Relay Backlog](../runbooks/RB-003-outbox-relay-backlog.md) |

---

## Summary

On 2024-03-01 at 14:08 UTC, `outbox_pending_events{source="gateway"}` began rising steadily, reaching a peak of 4,217 rows by 14:31 UTC. The relay had been publishing at approximately 220 msg/s against an incoming rate of 310 msg/s — a deficit of 90 rows/second that accumulated silently for 23 minutes before the `OutboxRelayLagging` alert fired.

The root cause was table bloat on `gateway_outbox_events`. The `sent` rows from the previous 14 days had not been purged (the purge job was never implemented). The SKIP LOCKED index scan on the `(status, next_retry_at)` index was degrading as the table grew past the point where the index fit in `shared_buffers`. Relay claim latency had increased from ~3ms to ~22ms over the two-week period, reducing effective relay throughput from ~260 msg/s to ~220 msg/s.

No events were lost. Delivery latency peaked at approximately 19 seconds (the queue depth divided by the relay drain rate) before the VACUUM and index rebuild restored claim latency and the backlog drained.

---

## Impact

| Area | Impact |
|---|---|
| Event delivery | Delayed — p99 E2E latency rose from ~1.2s to ~19s at peak backlog |
| Gateway HTTP availability | Unaffected |
| Data loss | None |
| DLQ events | 0 |
| Downstream systems | Data delays of 15–25 minutes reported by two consuming services |

---

## Timeline

All times UTC.

| Time | Event |
|---|---|
| **2024-02-15** | `gateway_outbox_events` purge job discussed in engineering sync. Decision: implement as part of the "maintenance" sprint. Not scheduled. |
| **2024-03-01 13:45** | Traffic increases above daily average (~310 msg/s vs ~180 msg/s typical). No alert — within normal traffic envelope. |
| **2024-03-01 13:48** | `outbox_relay_latency_ms` p99 rises from ~18ms to ~28ms. No alert configured for relay latency. |
| **2024-03-01 14:08** | `outbox_pending_events{source="gateway"}` crosses 200. `OutboxRelayLagging` alert fires. |
| **2024-03-01 14:09** | PagerDuty pages on-call. On-call acknowledges at 14:10. |
| **2024-03-01 14:11** | On-call opens Grafana. Confirms backlog is rising (~90 rows/s growth rate). `outbox_published_total` rate is 220/s, not zero. Identifies as relay-slow, not broker-down (RB-003 not RB-002). |
| **2024-03-01 14:13** | On-call checks relay logs. Poll cycles firing on schedule (every ~5s). Relay is claiming and publishing — just slower than expected. |
| **2024-03-01 14:15** | On-call runs EXPLAIN ANALYZE on the SKIP LOCKED query. Index scan returning correctly but with `Shared Buffers Hit = 23%, Buffers Read = 77%` — index is being read from disk, not cache. |
| **2024-03-01 14:16** | On-call queries table size: `gateway_outbox_events` is 2.7GB with 8.1M rows (7.9M status='sent', 200K status='pending'/'processing'). |
| **2024-03-01 14:18** | On-call runs `VACUUM ANALYZE gateway_outbox_events`. Vacuum starts. Table locks not required (VACUUM does not block reads or writes). |
| **2024-03-01 14:22** | VACUUM completes. Index statistics updated. Relay claim latency p99 drops from ~28ms to ~20ms. Backlog growth rate slows. |
| **2024-03-01 14:24** | On-call begins batched DELETE of sent rows older than 7 days: first batch of 50,000 rows, LIMIT to avoid long-running transaction. |
| **2024-03-01 14:27** | First DELETE batch completes (50,000 rows). Relay claim latency drops to ~14ms. Backlog growth rate turns negative — relay is now draining. |
| **2024-03-01 14:31** | Backlog peaks at 4,217 rows. Relay is draining at ~30 rows/s net (relay rate now exceeds publish rate). |
| **2024-03-01 14:34** | Second DELETE batch: 50,000 rows. Relay claim latency drops to ~8ms (near original baseline). |
| **2024-03-01 14:38** | Third DELETE batch completes. Table reduced to 1.1M rows (7M rows purged total). |
| **2024-03-01 14:44** | `outbox_pending_events` drops below 200. `OutboxRelayLagging` alert resolves. |
| **2024-03-01 14:55** | Backlog reaches 0. Relay latency p99 stable at ~9ms. Normal operations confirmed. On-call declares incident resolved. |
| **2024-03-01 15:30** | On-call adds a cron-based purge job to the backlog with P0 priority. |

---

## Detection

**Primary detection:** `OutboxRelayLagging` alert fired at 14:08 UTC when `outbox_pending_events > 200` for 2 minutes.

**What detection missed:** The relay latency degradation began much earlier. `outbox_relay_latency_ms` p99 had been creeping upward for approximately 14 days as the table grew. At the rate of ~180 msg/s throughput, the table was growing by approximately 15M rows per day (including sent rows). No alert was configured for relay latency drift.

**Lag between actual problem onset and alert:** The relay had been degraded since at least 2024-02-25 (relay latency p99 > 20ms). The alert fired 4 days after the underlying cause became measurable. The trigger was a traffic spike above normal, which pushed a degraded-but-functional relay into a visibly failing state.

---

## Metrics

| Metric | 14:00 UTC | 14:31 UTC (peak) | 14:55 UTC |
|---|---|---|---|
| `outbox_pending_events{source="gateway"}` | ~15 | 4,217 | 0 |
| `outbox_relay_latency_ms` p99 | ~28ms | ~28ms | ~9ms |
| `outbox_published_total` rate | ~220/s | ~220/s | ~260/s |
| `pg_total_relation_size('gateway_outbox_events')` | 2.7GB | 2.7GB | 0.3GB |
| SKIP LOCKED index cache hit ratio | ~23% | ~23% | ~91% |

### Grafana dashboards that showed anomalies

1. **Outbox Health** (`cdmp-outbox-health`): pending events rising steadily from 14:08. Relay latency p99 elevated but not spiking — subtle degradation rather than sharp failure.
2. **System Overview** (`cdmp-system-overview`): throughput slightly below normal. No dramatic change visible — the degradation was gradual enough that the throughput panel looked near-normal.
3. **Load Testing / PostgreSQL Performance** (`cdmp-load-testing`): insert rate normal but buffer cache hit rate for the outbox table was abnormally low.

---

## Root Cause Analysis

### Proximate cause

The SKIP LOCKED claim query on `gateway_outbox_events` was reading the `(status, next_retry_at)` index from disk rather than memory. At 2.7GB table size with a PostgreSQL `shared_buffers` allocation of 128MB, the index could not fit in the buffer cache. Each relay poll cycle's claim query required 8–12 disk reads, increasing claim latency from ~3ms to ~22ms and reducing effective relay throughput from ~260 msg/s to ~220 msg/s.

### Why the table was 2.7GB

No purge job existed for `gateway_outbox_events`. Every successfully processed event remained in the table indefinitely with `status='sent'`. At 180 msg/s average throughput over 14 days of operation since the last manual cleanup, the table had accumulated approximately 218 million row-equivalents. The VACUUM autovacuum process was running, but it cannot reclaim space without a subsequent manual VACUUM FULL (which requires an exclusive lock and was not feasible during business hours).

### Why the degradation went undetected for 14 days

Relay latency had been drifting upward at approximately 1ms per day. At no point did it cross an alerting threshold. The `outbox_relay_latency_ms` metric exists and is scraped, but no alert was configured on it. The visible consequence (backlog growth) only became observable when traffic exceeded the degraded relay's throughput ceiling.

### Why the traffic spike triggered the incident

At normal traffic (~180 msg/s), the relay's degraded throughput of ~220 msg/s was still above the publish rate — the system was slow but stable. When traffic rose to ~310 msg/s, the publish rate exceeded the relay's degraded ceiling for the first time, and the backlog began accumulating at 90 rows/s.

---

## Immediate Mitigation

1. `VACUUM ANALYZE gateway_outbox_events` — restored index statistics (03 minutes)
2. Three batched DELETE operations (50,000 rows each with 5-second sleep between batches) — reduced table from 2.7GB to 0.3GB (20 minutes)
3. Relay latency returned to ~9ms baseline after table reduction

No service restarts were required. The mitigation was entirely at the database level.

---

## Permanent Corrective Actions

### Action 1: Implement outbox table purge job

**Owner:** Backend team  
**Target:** 2024-03-08  
**Description:** A scheduled job runs nightly at 02:00 UTC and deletes `status='sent'` rows older than 7 days in batches of 10,000 with 100ms sleeps between batches to avoid lock contention.

```typescript
@Cron('0 2 * * *')
async purgeOutboxEvents(): Promise<void> {
  const cutoff = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
  let deletedTotal = 0;

  while (true) {
    const result = await this.dataSource.query(`
      DELETE FROM gateway_outbox_events
      WHERE id IN (
        SELECT id FROM gateway_outbox_events
        WHERE status = 'sent' AND created_at < $1
        LIMIT 10000
      )
    `, [cutoff]);

    deletedTotal += result.rowCount;
    if (result.rowCount < 10000) break;
    await new Promise(resolve => setTimeout(resolve, 100));
  }

  this.logger.log({ operation: 'purge.outbox', deletedTotal, cutoffDays: 7 });
}
```

Apply the same pattern to `processed_events` (30-day retention) and `event_attempts` (30-day retention).

### Action 2: Add relay latency drift alert

**Owner:** Platform team  
**Target:** 2024-03-08

```yaml
- alert: OutboxRelayLatencyDrift
  expr: histogram_quantile(0.99, rate(outbox_relay_latency_ms_bucket[5m])) > 20
  for: 10m
  severity: warning
  annotations:
    summary: "Relay claim p99 > 20ms for 10m — possible table bloat or broker pressure"
```

### Action 3: Add outbox table size monitoring

**Owner:** Platform team  
**Target:** 2024-03-08

```yaml
- alert: OutboxTableSizeWarning
  expr: pg_total_relation_size{relname="gateway_outbox_events"} > 500000000
  for: 0m
  severity: warning
  annotations:
    summary: "gateway_outbox_events exceeds 500MB — purge job may be failing or delayed"
```

### Action 4: Increase PostgreSQL shared_buffers allocation

**Owner:** Infrastructure  
**Target:** 2024-03-15  
**Description:** Current `shared_buffers = 128MB`. Increase to 256MB. This provides more buffer cache headroom for the outbox table index under sustained load. At current throughput (180 msg/s, ~20KB per index page), the active portion of the `(status, next_retry_at)` index is approximately 50MB. 256MB `shared_buffers` ensures the working set fits in memory with room to spare.

---

## Lessons Learned

**Table bloat is a slow-moving reliability risk.** The underlying cause existed for 14 days before it manifested as a user-visible incident. Relay latency drift is a leading indicator, but only if an alert is configured to detect it. Adding relay latency as a monitored metric (not just a scraped one) would have surfaced this two weeks earlier.

**The outbox pattern introduces a maintenance burden that must be actively managed.** An append-only outbox table without a purge job will grow without bound. This was documented as a known operational gap in ADR-003 and in the Architecture section of the README. The gap was acknowledged but not acted upon with a deadline. Known gaps need owners and target dates, not just documentation.

**Incident detection lagged onset by 14 days.** The `OutboxRelayLagging` alert is correct — it fires when the system is visibly degraded. But it fires after the user-visible impact has already begun. A relay latency drift alert would have fired before the backlog started growing, giving the on-call engineer a chance to run the purge job proactively.

**Batched DELETEs with sleep intervals are the right approach for large-table cleanup.** A single DELETE of 7M rows would have held a lock long enough to block relay claims during the recovery window, compounding the incident. The 50,000-row batches with 5-second sleeps between them allowed the relay to continue draining the backlog simultaneously with the cleanup.

---

## Follow-up Tasks

| # | Task | Owner | Priority | Target |
|---|---|---|---|---|
| 1 | Implement nightly purge job for `gateway_outbox_events`, `processed_events`, `event_attempts` | Backend | P0 | 2024-03-08 |
| 2 | Add `OutboxRelayLatencyDrift` alert (p99 > 20ms for 10m) | Platform | P0 | 2024-03-08 |
| 3 | Add `OutboxTableSizeWarning` alert (> 500MB) | Platform | P0 | 2024-03-08 |
| 4 | Increase `shared_buffers` to 256MB | Infrastructure | P1 | 2024-03-15 |
| 5 | Add table size panel to Outbox Health Grafana dashboard | Platform | P2 | 2024-03-15 |
| 6 | Document purge job in operational runbooks | Platform | P1 | 2024-03-08 |
