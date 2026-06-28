# ADR-006: Fencing Tokens for Outbox Relay Concurrency

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2024-01-18 |
| **Author** | Platform Engineering |
| **Reviewers** | Backend Engineering |
| **Supersedes** | — |
| **Superseded by** | — |

---

## Context

The outbox relay uses `SELECT ... FOR UPDATE SKIP LOCKED` to claim rows from the outbox table for publishing. `SKIP LOCKED` allows multiple relay instances to run concurrently without blocking each other: each instance claims a distinct subset of rows, and rows claimed by one instance are invisible to others until the lock is released.

This mechanism works correctly when relay instances are well-behaved: a relay claims a row, publishes it, receives a publisher confirm, and releases the lock by marking the row `sent`. The row is claimed by exactly one relay instance for exactly the duration of its publish-confirm cycle.

The mechanism fails under a specific failure sequence:

1. Relay A claims row R at `t=0`. Row R is locked; `lock_version = 1`.
2. Relay A begins publishing but is delayed (GC pause, broker latency spike, network hiccup).
3. The stale-lock reaper detects that row R has been locked longer than the configured TTL (default: 30s). It resets the lock, marking row R as `pending` again and incrementing `lock_version` to 2.
4. Relay B claims row R (now at `lock_version = 2`) and successfully publishes it. It calls `markSent()` using `lock_version = 2`. Row R is marked `sent`.
5. Relay A recovers from its delay. It calls `markSent()` using the `lock_version = 1` it captured at claim time.

At step 5, two outcomes are possible:

- **Without fencing**: `UPDATE gateway_outbox_events SET status='sent' WHERE id=R` — matches on `id` alone, succeeds. Relay A silently overwrites Relay B's `sent` record. The row was already correctly published by Relay B; Relay A's success is a no-op. However, there is no record that a concurrent claim occurred. The double-publish is invisible.

- **With fencing**: `UPDATE gateway_outbox_events SET status='sent' WHERE id=R AND lock_version=1` — matches zero rows because `lock_version` is now 2. The update fails. Relay A detects the mismatch, logs a structured warning with both the claimed and current `lock_version` values, and emits `outbox_fenced_publishes_total`. The event was already published by Relay B; no data is lost. But the detection makes the concurrent claim visible in metrics and logs.

The second problem in this failure sequence is the double-publish itself: row R was published by both Relay A and Relay B. This produces a duplicate event in the broker. The idempotency mechanism at the consumer (ADR-004) handles the duplicate correctly, but the duplicate is still produced. The fencing token detects the condition after the fact; it does not prevent the duplicate.

---

## Problem Statement

**`SELECT ... FOR UPDATE SKIP LOCKED` prevents concurrent claims on idle rows but does not protect against a stale relay instance that holds a lock beyond the reaper TTL and then completes its operation after the reaper has reassigned the row.**

Two specific gaps:

1. **Silent double-publish**: Without fencing, a stale relay instance that recovers and completes its `markSent()` call produces no error and no metric. The double-publish is invisible.

2. **Invisible concurrent claim**: Without fencing, an operator cannot determine from metrics or logs whether the relay ever experienced a concurrent claim race. The system appears correct even when it has produced duplicates.

---

## Decision

**Add a `lock_version` column (integer, default 0) to both outbox tables. The relay increments `lock_version` atomically when claiming a row and includes the claimed `lock_version` in the `markSent()` update predicate.**

### Schema

```sql
ALTER TABLE gateway_outbox_events ADD COLUMN lock_version INTEGER NOT NULL DEFAULT 0;
ALTER TABLE outbox_events         ADD COLUMN lock_version INTEGER NOT NULL DEFAULT 0;
```

### Claim

```typescript
// Relay claims the row and records the post-increment lock_version
const claimed = await queryRunner.manager
  .createQueryBuilder()
  .update(OutboxEvent)
  .set({ status: 'processing', lockVersion: () => 'lock_version + 1' })
  .where('id = :id AND status = :status', { id: row.id, status: 'pending' })
  .returning(['lock_version'])
  .execute();

const claimedLockVersion = claimed.raw[0].lock_version;
```

### markSent (CAS update)

```typescript
const result = await queryRunner.manager
  .createQueryBuilder()
  .update(OutboxEvent)
  .set({ status: 'sent', sentAt: new Date() })
  .where('id = :id AND lock_version = :claimedLockVersion', {
    id: row.id,
    claimedLockVersion
  })
  .execute();

if (result.affected === 0) {
  // Fencing: another relay instance claimed this row after the reaper reset it
  logger.warn({
    operation: 'relay.markSent',
    eventId: row.eventId,
    claimedLockVersion,
    msg: 'markSent CAS failed — stale relay instance detected'
  });
  metrics.increment('outbox_fenced_publishes_total');
}
```

The `lock_version` check (compare-and-swap) is the fencing token. A stale relay instance whose `claimedLockVersion` no longer matches the current `lock_version` in the database cannot mark the row `sent`.

### Stale-lock reaper

The stale-lock reaper runs on a fixed interval (default: 60s). It resets rows that have been in `processing` state longer than the configured TTL (default: 30s):

```typescript
await queryRunner.manager
  .createQueryBuilder()
  .update(OutboxEvent)
  .set({
    status: 'pending',
    lockVersion: () => 'lock_version + 1',  // invalidate any stale relay's token
    lockedUntil: null
  })
  .where('status = :status AND locked_at < :threshold', {
    status: 'processing',
    threshold: new Date(Date.now() - STALE_LOCK_TTL_MS)
  })
  .execute();
```

The reaper increments `lock_version` as part of the reset. This ensures that a stale relay instance whose claim pre-dates the reaper will have a `claimedLockVersion` that no longer matches, and its `markSent()` will be detected and logged.

---

## Alternatives Considered

### `SKIP LOCKED` alone (no fencing)

**Why considered:** The simplest implementation. `SKIP LOCKED` prevents concurrent claims; the race described above is unlikely in practice.

**Why not chosen:** The race is unlikely but not impossible. Under slow broker conditions (confirm latency spike), a relay instance can easily hold a lock for longer than the reaper TTL. The failure is silent — no error, no metric, no log. A system that fails silently under specific timing conditions is harder to operate and debug than one that detects and logs the condition explicitly. The fencing token adds two integer columns and a predicate to the UPDATE; the implementation cost is low relative to the diagnostic value.

### Distributed lock (Redis Redlock)

**Why considered:** A Redis-based distributed lock (Redlock algorithm) provides exclusive access to a resource across multiple processes without database-level locking semantics.

**Why not chosen:** Redis is not currently in the system stack. Adding Redis as a dependency solely for relay locking introduces an additional failure domain: if Redis is unavailable, the relay cannot claim any rows. The PostgreSQL-based fencing token provides equivalent protection without an additional dependency. Redlock also has known edge cases under network partitions (the "unsafe period" after lock expiry) that require careful TTL tuning.

### Single relay instance (no concurrency)

**Why considered:** If only one relay instance runs at a time, there are no concurrent claims and no fencing requirement.

**Why not chosen:** A single relay instance is a SPOF. If the relay instance crashes, outbox rows accumulate until it restarts. A second relay instance provides availability during relay restarts without requiring downtime. The fencing mechanism is the mechanism that makes concurrent relay instances safe.

### Optimistic locking via ORM (TypeORM `@VersionColumn`)

**Why considered:** TypeORM's `@VersionColumn()` provides optimistic locking with automatic version increment and CAS updates.

**Why not chosen:** TypeORM's optimistic locking throws `OptimisticLockVersionMismatchError` on conflict, which is an exception path rather than an expected control flow path. The relay's `markSent()` conflict is an expected and recoverable condition (the event was already published by another relay), not an error. Using the exception path for expected control flow conflates errors with expected concurrent races. The manual CAS update allows the relay to distinguish between "error" (unexpected failure) and "fenced" (expected concurrent race) without exception handling overhead.

---

## Tradeoffs

| Gains | Costs |
|---|---|
| Concurrent relay races are detected and logged — previously silent failures become visible | `lock_version` adds one integer column per outbox table |
| `outbox_fenced_publishes_total` metric enables alerting on concurrent relay races | The `markSent()` UPDATE predicate adds `AND lock_version = :claimedLockVersion` — marginally more complex |
| Stale relay instances are detected before they can silently mark rows sent | The reaper TTL must be tuned relative to worst-case broker confirm latency — too short causes false reaper firings |
| The fencing mechanism is self-correcting — no operator intervention required for normal concurrent races | A fenced relay instance produces a warning log and metric but does not retry the `markSent()` — the correct behavior (the row was already sent by another instance), but requires understanding to not create alert fatigue |
| The mechanism is implemented entirely in the relay and database — no additional infrastructure | — |

---

## Consequences

1. **The reaper TTL must exceed the worst-case broker confirm latency.** If the reaper TTL is shorter than the time a relay can legitimately hold a lock (e.g., under broker backpressure), the reaper will reset active (not stale) claims. This produces spurious fencing events: the relay that was legitimately processing a row will find its `markSent()` rejected after the reaper fires. The reaper TTL default (30s) was set to be approximately 10× the p99 broker confirm latency (3s) to provide a comfortable margin. If broker confirm latency increases (e.g., under sustained high throughput), the reaper TTL should be increased accordingly.

2. **A fencing event does not indicate data loss.** When a fencing event is logged, the event was already published by another relay instance. The fencing log entry is diagnostic, not an error. Alerts on `outbox_fenced_publishes_total` should be severity `warning`, not `critical`. A sustained high fencing rate (more than 1–2 per minute) indicates a structural problem (relay instances with overlapping lifecycles, excessively slow broker confirms) that warrants investigation, but an isolated fencing event is normal behavior under concurrent relay restarts.

3. **The correlation between `outbox_fenced_publishes_total` and `outbox_reaper_reclaimed_total` is a diagnostic signal.** A fencing spike that is not preceded by a reaper spike indicates a different class of concurrent claim than the one the reaper handles — possibly two relay instances both claiming the same row before either reaches the reaper TTL. This should be investigated.

4. **The fencing token does not prevent the double-publish.** It detects that the event was published twice and makes the detection observable. The consumer's idempotency mechanism (ADR-004) handles the duplicate correctly. The fencing token and the idempotency mechanism together provide the full protection; neither is sufficient alone.

---

## Operational Impact

### Alert rules

```yaml
- alert: OutboxRelayFencingElevated
  expr: rate(outbox_fenced_publishes_total[5m]) > 0.1
  for: 5m
  severity: warning
  annotations:
    summary: "Elevated relay fencing rate — check for concurrent relay instances or slow broker confirms"

- alert: OutboxReaperActivity
  expr: increase(outbox_reaper_reclaimed_total[5m]) > 0
  for: 0m
  severity: info
  annotations:
    summary: "Stale-lock reaper reclaimed rows — correlate with fencing events"
```

### Runbook — elevated fencing rate

1. Check `outbox_reaper_reclaimed_total` — if it spikes at the same time as `outbox_fenced_publishes_total`, the pattern is expected: reaper reset stale claims, new relay claimed the rows, old relay recovered and found its token invalidated
2. Check broker confirm latency (`relay.publish_with_confirm` p99 span duration) — if it exceeds 15s, the reaper TTL may be too short for current broker conditions
3. Check relay process count — if more than two relay instances are running simultaneously, something is wrong with the deployment (e.g., two concurrent deploys, stuck old process)
4. If fencing persists without reaper activity: inspect the relay logs for `claimedLockVersion` vs current `lock_version` — the difference indicates how many reaper cycles have run since the claim was made

### Runbook — stale-lock reaper not firing

Symptoms: rows in `processing` state older than the reaper TTL, no `outbox_reaper_reclaimed_total` increment.

1. Check reaper scheduler logs for errors
2. Check database connectivity from the relay process
3. Manually reset stale rows if needed:

```sql
-- Identify stale processing rows
SELECT id, event_id, lock_version, locked_at
FROM gateway_outbox_events
WHERE status = 'processing'
  AND locked_at < NOW() - INTERVAL '120 seconds';

-- Manual reset (use only if reaper is confirmed broken)
UPDATE gateway_outbox_events
SET status = 'pending',
    lock_version = lock_version + 1,
    locked_at = NULL
WHERE status = 'processing'
  AND locked_at < NOW() - INTERVAL '120 seconds';
```

---

## Future Considerations

- **Reaper TTL auto-tuning**: The reaper TTL could be set dynamically based on observed `relay.publish_with_confirm` p99 latency — e.g., `TTL = p99_confirm_latency × 10`. This would automatically adapt to changing broker conditions. Current implementation uses a fixed TTL from environment configuration.
- **Relay heartbeat**: Instead of a time-based TTL, the relay could write a heartbeat timestamp to a separate `relay_leases` table on a fixed interval. The reaper would reclaim rows whose relay has not heartbeated recently. This provides tighter TTL semantics under variable broker latency but adds complexity to the relay's main loop.
- **Formal verification of the concurrent relay protocol**: The fencing token + reaper + SKIP LOCKED combination can be modeled as a concurrent state machine. A TLA+ or Alloy model would formally verify that the combination provides the desired safety properties (no row marked sent by a stale relay) and liveness properties (every claimed row is eventually either marked sent or reclaimed by the reaper).
