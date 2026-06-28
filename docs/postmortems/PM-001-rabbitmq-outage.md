# PM-001 — RabbitMQ Broker Process Crash

| Field | Value |
|---|---|
| **Incident ID** | PM-001 |
| **Severity** | SEV-1 |
| **Status** | Resolved |
| **Incident start** | 2024-02-14 03:17 UTC |
| **Incident end** | 2024-02-14 03:40 UTC |
| **Total duration** | 23 minutes |
| **Incident commander** | On-call engineer (rotation: platform team) |
| **Postmortem author** | On-call engineer |
| **Review date** | 2024-02-15 |
| **Related runbook** | [RB-002 — RabbitMQ Outage](../runbooks/RB-002-rabbitmq-outage.md) |

---

## Summary

At 03:17 UTC on 2024-02-14, the RabbitMQ broker process crashed due to an OOM kill by the Linux kernel. The broker had been running for 31 days without a restart. A steady accumulation of messages in the `messaging.retry.q` queue — caused by a slow consumer the previous evening — had not fully drained before the nightly deploy of an unrelated service. The deploy triggered a brief spike in publish rate that pushed the broker's heap above the 40% memory watermark. The broker entered flow control, then the process was OOM-killed at 03:17 UTC.

Message delivery was fully halted for 23 minutes. The gateway service continued accepting HTTP requests and writing to the outbox (the outbox pattern preserved all events). No events were lost. Three messages in the DLQ at the time of the crash were recovered via outbox replay after the broker recovered.

---

## Impact

| Area | Impact |
|---|---|
| Event delivery | Fully halted for 23 minutes |
| Gateway HTTP availability | Unaffected — 202 responses continued throughout |
| Consumer processing | Halted for 23 minutes |
| Data loss | None — outbox preserved all events |
| DLQ events | 3 messages dead-lettered during the outage (recovered via replay) |
| Downstream systems | Reported data delays of 23–45 minutes (depending on their polling interval) |
| Users | No direct user-facing errors — effects were data delays in dependent systems |

---

## Timeline

All times UTC.

| Time | Event |
|---|---|
| **2024-02-13 21:30** | Scheduled deploy of `messaging-service` v1.4.2 (unrelated: schema validation improvement). Deploy completes without incident. |
| **2024-02-13 22:15** | Monitoring shows `retry_count_total{attempt="3"}` elevated. Consumer is retrying at higher-than-normal rate. On-call is not paged (no threshold breach). |
| **2024-02-13 23:00** | `messaging.retry.q` queue depth peaks at 2,847 messages. Consumer slowly draining. On-call observes in Grafana but does not intervene — queue is draining and DLQ is empty. |
| **2024-02-14 00:30** | `messaging.retry.q` depth reduced to 340. Consumer catches up. |
| **2024-02-14 03:14** | Scheduled cron job triggers a batch of 1,200 deferred events from a business process. Publish rate briefly spikes to 380 msg/s. |
| **2024-02-14 03:15** | RabbitMQ heap usage crosses 38% (watermark: 40%). Management UI shows memory usage rising. No alert configured for sub-threshold memory. |
| **2024-02-14 03:16** | RabbitMQ enters flow control. Relay `waitForConfirms()` calls begin blocking. `publisher_confirm_failures_total` starts incrementing. `OutboxPublishConfirmFailure` alert fires. |
| **2024-02-14 03:17:04** | Linux OOM killer terminates the `rabbitmq` process. All AMQP connections drop. `up{job="rabbitmq"}` drops to 0. `OutboxRelayLagging` alert fires. |
| **2024-02-14 03:17:30** | PagerDuty pages on-call. On-call acknowledges at 03:18:12. |
| **2024-02-14 03:18** | On-call opens Grafana. Observes `outbox_pending_events{source="gateway"}` rising at ~380 rows/s. Confirms `up{job="rabbitmq"} = 0`. |
| **2024-02-14 03:19** | On-call checks RabbitMQ management UI — unreachable. Checks broker process: not running. Reviews Docker logs: `OOM killer invoked`. |
| **2024-02-14 03:21** | On-call executes `docker compose restart rabbitmq`. Broker starts. Management UI responsive at 03:22. |
| **2024-02-14 03:22** | Topology re-assertion begins. Relay reconnects within one poll cycle. `outbox_published_total` rate resumes. |
| **2024-02-14 03:24** | `messaging-service` does not auto-reconnect (known gap in DlqConsumerService). On-call manually restarts `messaging-service`. |
| **2024-02-14 03:25** | Consumer reconnected. `messages_processed_total` rate resumes. |
| **2024-02-14 03:30** | `outbox_pending_events` peak reaches 3,100 rows. Relay draining at ~250 msg/s. |
| **2024-02-14 03:40** | `outbox_pending_events` returns to < 20. All queued events delivered. `OutboxRelayLagging` alert resolves. |
| **2024-02-14 03:45** | On-call inspects DLQ: 3 messages found (exhausted retry budget during outage). Recovers via outbox replay. Declares incident resolved. |
| **2024-02-14 04:30** | Downstream systems report data fully synchronized. |

---

## Detection

**Primary detection:** `OutboxPublishConfirmFailure` alert fired at 03:16 UTC (1 minute before the OOM kill). This was the first signal — the broker entered flow control before the process crashed, causing confirm failures.

**Secondary detection:** `OutboxRelayLagging` alert fired at 03:17 UTC when `outbox_pending_events > 200`.

**What was missed before the incident:** `rabbitmq_node_mem_used_bytes / rabbitmq_node_mem_limit_bytes` was at 87% for approximately 4 hours before the OOM kill. No alert was configured for broker memory utilization above 70%. An alert at this threshold would have given approximately 3 hours of warning before the OOM kill.

**Time to detection:** 13 seconds from OOM kill to alert fire. Time from page to engineer acknowledgement: 68 seconds.

---

## Metrics

### Metrics that changed during the incident

| Metric | Pre-incident | During (03:17–03:25) | Post-recovery |
|---|---|---|---|
| `up{job="rabbitmq"}` | 1 | 0 | 1 |
| `outbox_pending_events{source="gateway"}` | ~15 | Rising to 3,100 | < 20 |
| `outbox_published_total` rate | ~180/s | 0 | ~250/s |
| `messages_processed_total` rate | ~180/s | 0 | ~250/s |
| `publisher_confirm_failures_total` | 0 | 47 | 0 |
| `dlq_messages_total` | 0 | 3 | 0 (after replay) |
| `rabbitmq_node_mem_used_ratio` | 0.87 | N/A (process down) | 0.31 |

### Grafana dashboards that showed anomalies

1. **System Overview** (`cdmp-system-overview`): throughput lines dropped to zero at 03:17. Success rate dropped to 0%.
2. **Reliability** (`cdmp-reliability`): `publisher_confirm_failures_total` spike visible at 03:16, preceding the outage.
3. **Outbox Health** (`cdmp-outbox-health`): pending events rising steeply from 03:17 to 03:30. Drain visible from 03:25 to 03:40.

---

## Root Cause Analysis

### Proximate cause

The Linux OOM killer terminated the RabbitMQ process at 03:17 UTC. The broker's heap had grown to 89% of the configured memory watermark (40% of Docker-allocated 512MB = 204MB limit). When flow control was triggered at 88% and a brief publish burst arrived, the allocator could not satisfy the request and the kernel invoked the OOM killer.

### Contributing factor 1: Undrained retry queue from the previous evening

At 23:00 UTC the previous evening, `messaging.retry.q` held 2,847 messages. These messages consume broker memory while queued. Although the queue drained by 00:30 UTC, the broker's memory did not fully return to baseline — RabbitMQ's Erlang runtime does not immediately release freed heap back to the OS. The broker entered the 14-day window after 03:14 UTC carrying a higher-than-baseline heap baseline.

### Contributing factor 2: RabbitMQ memory limit undersized for observed traffic

The Docker memory limit for the broker was 512MB. At 180 msg/s sustained throughput plus the residual retry queue heap, the broker was operating at 87% of its memory watermark before the cron job fired. The broker had no headroom for a legitimate publish burst.

### Contributing factor 3: No memory utilization alert below the watermark

The `rabbitmq_node_mem_alarm` metric fires at the watermark (40%). There was no alert at 70%, 80%, or 85% of the watermark. The 87% utilization at 03:14 was visible in Grafana but not acted upon because no alert fired and the on-call engineer was asleep.

### Contributing factor 4: DlqConsumerService does not auto-reconnect

After the broker recovered, the relay service reconnected automatically via its lazy-connect pattern. The `DlqConsumerService` in the messaging service did not — it requires a service restart. This extended the consumer downtime by 2 minutes (03:23 to 03:25) while the on-call engineer recognized the gap and restarted the service manually.

### Why no data was lost

The transactional outbox pattern preserved all events during the broker outage. The gateway service continued writing to `gateway_outbox_events` throughout the 23-minute window. After the broker recovered, the relay drained 3,100 accumulated rows. The three DLQ messages were recovered via outbox replay because the original outbox rows were still present and marked `sent` — the recovery procedure reset them to `pending` and the relay republished them.

---

## Immediate Mitigation

1. `docker compose restart rabbitmq` — broker restarted at 03:21 UTC
2. `docker compose restart messaging-service` — DlqConsumerService reconnected at 03:25 UTC
3. Manual outbox replay for 3 DLQ messages — completed at 03:45 UTC
4. Increased Docker memory limit for RabbitMQ from 512MB to 1GB — applied at 04:00 UTC as a configuration hotfix

---

## Permanent Corrective Actions

### Action 1: Add broker memory utilization alerts

**Owner:** Platform team  
**Target:** 2024-02-21

```yaml
- alert: RabbitMQMemoryPressure
  expr: rabbitmq_node_mem_used_bytes / rabbitmq_node_mem_limit_bytes > 0.70
  for: 5m
  severity: warning
  annotations:
    summary: "RabbitMQ memory at {{ $value | humanizePercentage }} of watermark"

- alert: RabbitMQMemoryCritical
  expr: rabbitmq_node_mem_used_bytes / rabbitmq_node_mem_limit_bytes > 0.85
  for: 2m
  severity: critical
  annotations:
    summary: "RabbitMQ memory critical — OOM risk within minutes"
```

### Action 2: Implement DlqConsumerService auto-reconnect

**Owner:** Backend team  
**Target:** 2024-02-28  
**Description:** Add an AMQP connection error handler to `DlqConsumerService` that re-initializes the consumer channel on connection close, using exponential backoff with a 30-second ceiling. This is already implemented in the main consumer handler — the pattern should be extracted to a shared `AmqpReconnectMixin` and applied to all consumers.

### Action 3: Increase RabbitMQ memory allocation and document capacity model

**Owner:** Infrastructure  
**Target:** 2024-02-21  
**Description:** Increase Docker memory limit to 1GB in `docker-compose.yml`. Document the broker memory capacity model: at 180 msg/s with batch size 25, the broker holds approximately 4,500 in-flight messages at any time. At ~1KB per message, steady-state heap for messages is ~4.5MB. Memory pressure comes from Erlang runtime overhead and queue metadata, not message payload. The 40% watermark (204MB at 512MB limit) was too low given the Erlang runtime baseline of ~160MB.

### Action 4: Add retry queue depth alert with broker memory correlation

**Owner:** Platform team  
**Target:** 2024-02-28  
**Description:** A deep retry queue is an early warning for broker memory pressure. Add an alert when `rabbitmq_queue_messages{queue="messaging.retry.q"} > 1000` for more than 10 minutes — this is a leading indicator that should trigger broker memory inspection before it becomes critical.

---

## Lessons Learned

**The outbox pattern worked exactly as designed.** During 23 minutes of complete broker unavailability, zero events were lost. The gateway continued accepting requests and writing to the outbox. This validated the core reliability guarantee of the system and should be documented explicitly in the architecture overview.

**The first alert was the confirm failure, not the broker down.** `OutboxPublishConfirmFailure` fired 60 seconds before the OOM kill. If the on-call engineer had been awake and responsive to that alert, there was a window to intervene before the broker crashed — specifically, to temporarily reduce the broker's flow control watermark or trigger a graceful memory release. This argues for a lower alerting threshold and a documented response procedure for confirm failures that precede an OOM condition.

**Long-running broker processes accumulate heap that does not return to the OS.** The broker had been running for 31 days. The Erlang runtime's memory allocator retains freed blocks for reuse rather than returning them immediately. A monthly restart policy or a scheduled GC invocation (`rabbitmqctl eval 'erlang:garbage_collect().'`) would have reduced the baseline heap before the cron job fired.

**The DlqConsumerService auto-reconnect gap was a known issue that was deprioritized.** It was documented in RB-002 and had been open for 6 weeks. A known gap that extends incident duration during an SEV-1 is not an acceptable backlog item — it needs a completion deadline.

---

## Follow-up Tasks

| # | Task | Owner | Priority | Target |
|---|---|---|---|---|
| 1 | Add `RabbitMQMemoryPressure` and `RabbitMQMemoryCritical` alerts | Platform | P0 | 2024-02-21 |
| 2 | Implement `DlqConsumerService` auto-reconnect | Backend | P0 | 2024-02-28 |
| 3 | Increase RabbitMQ Docker memory limit to 1GB | Infrastructure | P0 | 2024-02-21 |
| 4 | Add `messaging.retry.q` depth alert (> 1000 for 10m) | Platform | P1 | 2024-02-28 |
| 5 | Document monthly broker restart policy or scheduled GC procedure | Platform | P1 | 2024-02-28 |
| 6 | Write memory capacity model for RabbitMQ (message count → heap) | Backend | P2 | 2024-03-07 |
| 7 | Add outbox replay to runbook for DLQ messages caused by broker outage | Platform | P1 | 2024-02-21 |
