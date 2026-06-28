# PM-004 — Publisher Confirm Failures — RabbitMQ Memory Alarm

| Field | Value |
|---|---|
| **Incident ID** | PM-004 |
| **Severity** | SEV-2 |
| **Status** | Resolved |
| **Incident start** | 2024-04-02 09:14 UTC |
| **Incident end** | 2024-04-02 09:32 UTC |
| **Total duration** | 18 minutes |
| **Incident commander** | On-call engineer (rotation: platform team) |
| **Postmortem author** | On-call engineer |
| **Review date** | 2024-04-04 |
| **Related runbook** | [RB-004 — Publisher Confirm Failure](../runbooks/RB-004-publisher-confirm-failure.md) |

---

## Summary

On 2024-04-02 between 09:14 and 09:32 UTC, the relay received 12 AMQP `basic.nack` responses from the broker during a period of RabbitMQ memory pressure. The broker had entered flow control at 09:12 UTC as a result of a large number of unacknowledged messages accumulating in `messaging.work` — caused by a consumer that had stalled after a NestJS lifecycle hook took longer than expected during a rolling deploy. When the relay continued publishing into a flow-controlled broker, the channel eventually closed and the subsequent `waitForConfirms()` call returned with nack errors.

The relay's `markSent()` was not called for any of the 12 nacked messages — the relay correctly detected the nack and left the outbox rows in `processing` state. The stale-lock reaper reset them to `pending` within 30 seconds, and all 12 were successfully republished on the next relay cycle. No events were lost and no DLQ entries were created.

The incident was resolved when the stalled consumer was identified and the rolling deploy completed, clearing the unacknowledged message backlog and releasing the broker's memory alarm.

---

## Impact

| Area | Impact |
|---|---|
| Event delivery | 12 events delayed by 30–35 seconds (stale-lock reaper + one relay poll cycle) |
| DLQ events | 0 |
| Data loss | 0 |
| Gateway availability | Unaffected |
| Publisher confirm failures | 12 |
| Downstream systems | No reported impact — 35-second delay was within downstream polling tolerance |

---

## Timeline

All times UTC.

| Time | Event |
|---|---|
| **2024-04-02 09:05** | Rolling deploy of `messaging-service` v1.5.0 begins. Kubernetes rolling update: terminates one pod, starts replacement. |
| **2024-04-02 09:06** | New pod starts. NestJS lifecycle hook `onApplicationBootstrap()` begins schema validation fixture check — unexpectedly slow due to a new fixture added in v1.5.0 that loads a 2MB test file. Hook takes 67 seconds instead of the expected < 2 seconds. |
| **2024-04-02 09:06–09:13** | During the 67-second bootstrap period, the new pod is not consuming from `messaging.work`. The remaining pod is consuming alone. Queue depth begins rising. |
| **2024-04-02 09:09** | `messaging.work` queue depth: 380 messages. Remaining consumer pod handling ~100/s (normal is ~200/s split across two pods). |
| **2024-04-02 09:12** | `messaging.work` queue depth: 870 messages. RabbitMQ heap crosses memory watermark (40% of 1GB = 409MB). Broker enters flow control. Relay `channel.publish()` calls begin blocking. |
| **2024-04-02 09:13** | Relay's `waitForConfirms()` times out on 12 in-flight messages. AMQP channel closes. Relay receives nack events for the 12 messages. |
| **2024-04-02 09:13:15** | Relay logs: `publisher confirm nack received — eventId=[...] deliveryTag=[...]` (12 entries). `publisher_confirm_failures_total` increments by 12. |
| **2024-04-02 09:13:15** | `OutboxPublishConfirmFailure` alert fires immediately (zero-tolerance threshold). |
| **2024-04-02 09:13:30** | Relay does NOT call `markSent()` for the 12 nacked rows. Rows remain in `processing` state with original `lock_version`. |
| **2024-04-02 09:14** | PagerDuty pages on-call. On-call acknowledges at 09:15:10. |
| **2024-04-02 09:13:30** | Stale-lock reaper runs (60-second interval). The 12 rows have `locked_at > 30s ago`. Reaper resets them to `pending`, increments `lock_version`. `outbox_reaper_reclaimed_total` increments by 12. |
| **2024-04-02 09:15** | On-call opens Grafana. Observes `publisher_confirm_failures_total = 12`, not rising. Relay latency p99 elevated (~180ms). No new confirm failures since 09:13:15. |
| **2024-04-02 09:16** | On-call verifies the 12 affected rows: all show `status='pending'` in `gateway_outbox_events`. No rows marked `sent`. No records in `processed_events` for those `eventId`s. Confirms no data loss. |
| **2024-04-02 09:17** | On-call investigates broker. Management UI shows memory alarm active. `messaging.work` queue depth at 1,240 messages. |
| **2024-04-02 09:17** | On-call checks Kubernetes pod status. Identifies the stalled bootstrap pod: `messaging-service-v1.5.0-xyz` with `status=Running` but 0 messages consumed since start. |
| **2024-04-02 09:18** | On-call checks pod logs. Identifies: `[SchemaFixtureLoader] loading fixture file contracts/fixtures/create-message-event-v3.json (2.1MB) — this may take a moment`. Fixture loading is synchronous and blocking the bootstrap hook. |
| **2024-04-02 09:13–09:23** | New pod completes bootstrap (fixture loading finishes at 09:13:17 — 67 seconds after pod start). Pod begins consuming. |
| **2024-04-02 09:23** | Both pods consuming. Combined consumer throughput: ~200/s. `messaging.work` queue begins draining rapidly. |
| **2024-04-02 09:28** | `messaging.work` queue empty. Memory alarm clears. Broker exits flow control. |
| **2024-04-02 09:28** | Relay reconnects to broker channel. `waitForConfirms()` succeeds. Publisher confirms resume normally. |
| **2024-04-02 09:30** | The 12 nacked rows (now `pending`) are picked up by the relay and published successfully. `processed_events` records confirmed for all 12. |
| **2024-04-02 09:32** | On-call confirms: 0 DLQ events, `publisher_confirm_failures_total` not rising, all 12 events in `processed_events`. Incident resolved. |

---

## Detection

**Primary detection:** `OutboxPublishConfirmFailure` alert fired at 09:13:15 UTC — within 2 seconds of the first nack. This was the correct and expected behavior for a zero-tolerance alert.

**What the alert did not tell us:** The alert fired once (12 failures at the same moment) and did not recur. This was ambiguous at acknowledgement time — the on-call engineer did not initially know whether the failures were isolated or ongoing. The first investigation step was to determine whether `publisher_confirm_failures_total` was still rising (it was not).

**What was not alerted before the incident:** The memory alarm activation at 09:12 had no alert (PM-001 follow-up task #1 was only partially implemented — the `RabbitMQMemoryPressure` alert was added but had a 5-minute `for:` condition that hadn't yet elapsed). The stalled consumer pod had no alert — `messaging.work` depth was rising for 7 minutes before the broker entered flow control, with no alert configured at the work queue level for a single-pod reduction in throughput.

---

## Metrics

| Metric | 09:12 | 09:13 | 09:28 | 09:32 |
|---|---|---|---|---|
| `publisher_confirm_failures_total` | 0 | 12 | 12 (static) | 12 (static) |
| `rabbitmq_node_mem_alarm` | 0 | 1 | 0 | 0 |
| `rabbitmq_queue_messages{queue="messaging.work"}` | 870 | 1,100 | 0 | 0 |
| `messages_processed_total` rate | ~100/s | ~100/s | ~200/s | ~200/s |
| `outbox_reaper_reclaimed_total` | 0 | 12 | 12 (static) | 12 (static) |
| `outbox_fenced_publishes_total` | 0 | 0 | 0 | 0 |

### Grafana dashboards that showed anomalies

1. **Reliability** (`cdmp-reliability`): `publisher_confirm_failures_total` spike visible at 09:13. Alert banner active.
2. **Outbox Health** (`cdmp-outbox-health`): stale-lock reaper spike at 09:13:30 (12 rows reclaimed). Correlated with fencing panel (0 fencing events — correct, reaper reclaimed before any competing relay claimed them).
3. **System Overview** (`cdmp-system-overview`): throughput drop from ~200/s to ~100/s from 09:06 onward. Not dramatic enough to alert on its own.

---

## Root Cause Analysis

### Proximate cause

The relay received AMQP `basic.nack` responses for 12 in-flight messages after the broker's channel closed during a flow-control event. The broker entered flow control because `messaging.work` accumulated 870+ unacknowledged messages while the new `messaging-service` pod was blocked in its bootstrap lifecycle hook.

### Why the bootstrap hook was slow

The `onApplicationBootstrap()` hook in v1.5.0 includes a schema validation step that loads contract fixture files to verify backward compatibility. A v3 fixture file added in v1.5.0 was 2.1MB — significantly larger than the sub-1KB fixtures from previous versions. The file is loaded synchronously using `fs.readFileSync()`. On the pod's filesystem (an ephemeral Kubernetes volume backed by EBS), the first read of a cold file took 67 seconds due to a combination of cold page cache and EBS cold-start latency for the specific availability zone.

### Why the broker entered flow control

After PM-001, the broker memory limit was increased to 1GB and the watermark remains at 40% (409MB). During the 7-minute period of reduced consumer throughput, `messaging.work` accumulated 870 messages. At approximately 1KB per message average, the queue consumed ~870KB of broker memory. However, the broker's total heap pressure also includes the Erlang runtime, management plugin overhead, and retained message metadata — the combination pushed heap past the 409MB watermark.

### Why the relay's nack handling worked correctly

The relay's `markSent()` is gated on receiving a `basic.ack`. When `waitForConfirms()` returns with nack events, the relay logs the nack and does not advance the row to `sent`. The rows remained in `processing` state. The stale-lock reaper, running on its 60-second interval, found them 17 seconds after the nack (because `locked_at` was already 13 seconds old when the nack occurred, and the reaper's next run was 17 seconds later). The reaper reset them correctly. The next relay poll picked them up and published successfully.

### Why there were no fencing token events despite the reaper reclaiming rows

The reaper incremented `lock_version` on the 12 rows. The relay that had originally claimed them had already dropped its channel reference — it could not call `markSent()` even if it tried, because the channel was closed. No competing relay instance claimed the rows during the 17-second gap. Therefore no fencing event occurred. The sequence was: claim → nack → reaper reset → clean re-claim. No concurrency.

---

## Immediate Mitigation

No active mitigation was required during the incident. The relay's nack handling and the stale-lock reaper resolved the immediate failure automatically. The on-call engineer's actions were limited to verification:

1. Confirmed `publisher_confirm_failures_total` was not rising
2. Confirmed the 12 affected outbox rows were reset to `pending`
3. Confirmed the 12 events were processed and in `processed_events` after relay republish
4. Identified the stalled bootstrap pod as the root cause
5. Filed a P0 engineering ticket for the fixture loading issue

No manual intervention was needed to recover from the confirm failures. The automated mechanisms (nack handling + stale-lock reaper) recovered the affected messages within 35 seconds of the failure.

---

## Permanent Corrective Actions

### Action 1: Make fixture loading in bootstrap hook asynchronous and bounded

**Owner:** Backend team  
**Target:** 2024-04-09  
**Description:** Replace `fs.readFileSync()` in the schema fixture loader with `fs.promises.readFile()` with a 5-second timeout. If the fixture file cannot be loaded within 5 seconds, log a warning and skip the validation rather than blocking the bootstrap hook indefinitely. The backward-compatibility fixture test should run in CI, not in production bootstrap.

```typescript
// Before:
const fixture = JSON.parse(fs.readFileSync(fixturePath, 'utf-8'));

// After:
const fixture = await Promise.race([
  fs.promises.readFile(fixturePath, 'utf-8').then(JSON.parse),
  new Promise((_, reject) =>
    setTimeout(() => reject(new Error('Fixture load timeout')), 5000)
  ),
]);
```

### Action 2: Add Kubernetes readiness probe that waits for consumer to register

**Owner:** Infrastructure  
**Target:** 2024-04-09  
**Description:** The rolling deploy started routing traffic reduction assumptions before the new pod was actually consuming. Add a readiness probe that checks the AMQP consumer channel is active before the pod is marked ready. This prevents the rolling deploy from reducing consumer capacity below the single-pod threshold before the new pod is consuming.

```yaml
readinessProbe:
  httpGet:
    path: /health/ready
    port: 3001
  initialDelaySeconds: 10
  periodSeconds: 5
  failureThreshold: 6   # 30 seconds of grace
```

The `/health/ready` endpoint returns 200 only after `DlqConsumerService` and the main AMQP consumer are both connected.

### Action 3: Reduce `RabbitMQMemoryPressure` alert `for:` from 5m to 1m

**Owner:** Platform team  
**Target:** 2024-04-05  
**Description:** PM-001 follow-up task added this alert with `for: 5m`. In this incident the broker entered flow control within 2 minutes of the memory threshold crossing. The 5-minute window is too long — reduce to 1 minute. The alert at 09:12 would have fired at 09:13, giving the on-call engineer information before the nack rather than after.

### Action 4: Add `messaging.work` single-pod throughput alert

**Owner:** Platform team  
**Target:** 2024-04-12  
**Description:** A 50% drop in consumer throughput that persists for more than 2 minutes indicates a pod has stalled or crashed. This alert would have fired at 09:08 — 5 minutes before the broker entered flow control.

```yaml
- alert: ConsumerThroughputHalved
  expr: |
    rate(messages_processed_total[2m])
    < (rate(messages_processed_total[10m] offset 5m) * 0.6)
  for: 2m
  severity: warning
  annotations:
    summary: "Consumer throughput dropped > 40% vs 5-minute baseline — check consumer pods"
```

---

## Lessons Learned

**The reliability mechanisms worked.** The nack handling, the stale-lock reaper, and the absence of fencing events all behaved exactly as designed. The 12 affected messages were recovered automatically in 35 seconds without operator intervention and without data loss. This is the expected behavior, and it should be documented explicitly: "the system self-heals from publisher confirm failures within one reaper cycle (60 seconds) provided the broker recovers."

**Production bootstrap hooks are not the right place for I/O-bound validation.** The fixture loading was added as a defense-in-depth check for backward compatibility. In principle, this is valuable. In practice, it made the pod's startup time unpredictable and dependent on filesystem cold-start latency. Backward-compatibility checks belong in CI, not in production pod startup. A slow bootstrap hook has a blast radius that extends to broker memory pressure.

**A zero-tolerance alert on confirm failures creates the right pressure.** The on-call engineer was paged immediately for 12 events that resolved automatically. This may seem noisy in retrospect, but confirm failures indicate a potential data loss path — the zero-tolerance threshold is correct even if the specific incident self-healed. The alert correctly identified a real failure mode; the system's automated recovery is what prevented damage.

**The stale-lock reaper TTL (30 seconds in this case — rows were ~13 seconds old when nacked) is well-calibrated for this failure pattern.** A reaper TTL shorter than 13 seconds would have caused false reclaims during normal relay operation. The observed 30-second end-to-end recovery time (nack → reaper → re-publish) is acceptable.

---

## Follow-up Tasks

| # | Task | Owner | Priority | Target |
|---|---|---|---|---|
| 1 | Make fixture loading async with 5s timeout | Backend | P0 | 2024-04-09 |
| 2 | Add AMQP consumer readiness probe to Kubernetes deployment | Infrastructure | P0 | 2024-04-09 |
| 3 | Reduce `RabbitMQMemoryPressure` alert `for:` from 5m to 1m | Platform | P0 | 2024-04-05 |
| 4 | Add `ConsumerThroughputHalved` alert | Platform | P1 | 2024-04-12 |
| 5 | Document self-healing behavior from confirm failures in README | Backend | P2 | 2024-04-12 |
| 6 | Add fixture file size limit to CI check (warn if > 100KB) | Backend | P2 | 2024-04-12 |
