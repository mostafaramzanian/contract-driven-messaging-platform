# PM-003 — DLQ Growth Spike — PostgreSQL Connection Pool Exhaustion

| Field | Value |
|---|---|
| **Incident ID** | PM-003 |
| **Severity** | SEV-1 |
| **Status** | Resolved |
| **Incident start** | 2024-03-18 11:22 UTC |
| **Incident end** | 2024-03-18 11:53 UTC |
| **Total duration** | 31 minutes |
| **Incident commander** | On-call engineer (rotation: backend team) |
| **Postmortem author** | On-call engineer |
| **Review date** | 2024-03-20 |
| **Related runbook** | [RB-001 — DLQ Growth Spike](../runbooks/RB-001-dlq-growth-spike.md) |

---

## Summary

On 2024-03-18 between 11:22 and 11:53 UTC, 47 messages were routed to `messaging.dlq`. The root cause was PostgreSQL connection pool exhaustion on the messaging service. At 11:19 UTC, a long-running analytical query began on the same PostgreSQL instance, consuming 8 of the 10 available connections in the `messages_db` pool. Consumer workers attempting to commit their atomic transactions were blocked waiting for available connections. The blocking lasted long enough for messages to exhaust their retry budget (5 attempts × exponential backoff, cumulative 62s window).

No events were permanently lost: all 47 DLQ messages were recovered via outbox replay after the analytical query was killed and the connection pool recovered. The recovery procedure took 22 minutes because the on-call engineer had to manually identify each affected `eventId`, reset the outbox rows, and clear the attempt counters.

---

## Impact

| Area | Impact |
|---|---|
| Event delivery | Degraded — consumer throughput dropped from ~200 msg/s to ~22 msg/s for 8 minutes |
| DLQ events | 47 messages dead-lettered |
| Gateway availability | Unaffected |
| Data loss | None — recovered via outbox replay |
| Recovery time | 22 minutes after root cause resolved |
| Downstream systems | 47 business events delayed by 45–65 minutes |

---

## Timeline

All times UTC.

| Time | Event |
|---|---|
| **2024-03-18 11:19** | Data analyst runs an ad-hoc `GROUP BY` query on `messages` table for a business report. Query does not use an index. Full table scan begins. 8 connections from the `messaging-service` pool are consumed. |
| **2024-03-18 11:19:40** | `messages_processed_total` rate drops from ~200/s to ~22/s. Consumer workers queued waiting for DB connections. |
| **2024-03-18 11:20** | `messaging.work` queue depth begins rising. Consumer is not keeping up. |
| **2024-03-18 11:21** | First messages begin entering retry queue — consumer workers timing out waiting for connections, being classified as TRANSIENT errors. |
| **2024-03-18 11:22** | `DLQMessageReceived` alert fires — first message exhausted retry budget (entered at 11:20:58, exhausted 62s budget at 11:22:00). |
| **2024-03-18 11:22:30** | PagerDuty pages on-call. On-call acknowledges at 11:23:15. |
| **2024-03-18 11:24** | On-call opens Grafana. Observes DLQ growth at approximately 5 messages/minute. Consumer throughput ~22 msg/s. Relay throughput normal (~200 msg/s). Broker queues: `messaging.work` at 420 messages and growing. |
| **2024-03-18 11:25** | On-call checks consumer logs. Error pattern: `Connection pool timeout waiting for connection (timeout: 5000ms)`. Identifies as database connection exhaustion. |
| **2024-03-18 11:26** | On-call queries `pg_stat_activity`. Identifies the long-running analytical query: `SELECT subject, COUNT(*) ... FROM messages GROUP BY subject` — running for 7 minutes, no index, full table scan. |
| **2024-03-18 11:27** | On-call kills the analytical query: `SELECT pg_terminate_backend(pid)`. |
| **2024-03-18 11:27:30** | Connection pool drains. Consumer workers regain connections. `messages_processed_total` rate recovers to ~200/s. |
| **2024-03-18 11:28** | `messaging.work` queue begins draining. New DLQ events stop. |
| **2024-03-18 11:31** | `messaging.work` queue empty. All in-flight messages processed. |
| **2024-03-18 11:31** | On-call begins DLQ recovery. 47 messages in `messaging.dlq`. |
| **2024-03-18 11:31–11:53** | On-call iterates through 47 DLQ messages: reads `eventId` from each, verifies `processed_events` has no record, resets `gateway_outbox_events` row to `pending`, clears `event_attempts` counter. Relay picks up and redelivers. |
| **2024-03-18 11:53** | All 47 events confirmed in `processed_events`. DLQ empty. Incident resolved. |

---

## Detection

**Primary detection:** `DLQMessageReceived` alert fired at 11:22 UTC. This was 3 minutes after the connection pool was exhausted.

**What was not alerted:** The connection pool exhaustion itself had no alert. `pg_stat_activity_count{state='active'}` rising to 10 (pool exhausted) fired no signal. The consumer throughput drop from 200 to 22 msg/s also had no alert — only the downstream consequence (DLQ) was alerted.

**Detection gap:** There was a 3-minute window between pool exhaustion (11:19) and the first DLQ alert (11:22). During those 3 minutes, 47 messages entered and exhausted their retry budget. An alert on connection pool utilization would have fired at 11:19:40 when pool utilization crossed 80%, before any messages reached the DLQ.

---

## Metrics

| Metric | 11:19 | 11:22 | 11:27 | 11:31 |
|---|---|---|---|---|
| `messages_processed_total` rate | ~200/s | ~22/s | ~22/s | ~200/s |
| `dlq_messages_total` (cumulative) | 0 | 1 | 34 | 47 |
| `rabbitmq_queue_messages{queue="messaging.work"}` | ~10 | ~180 | ~420 | 0 |
| `pg_stat_activity_count{state='active'}` | ~3 | ~10 (pool exhausted) | ~10 | ~3 |
| `retry_count_total{attempt="5"}` rate | 0 | rising | rising | 0 |

### Grafana dashboards that showed anomalies

1. **Reliability** (`cdmp-reliability`): DLQ growth and retry funnel collapse at attempt 5. Alert banner visible.
2. **System Overview** (`cdmp-system-overview`): consumer throughput drop from 200 to 22 msg/s — dramatic change visible in the throughput panel.
3. **Distributed Tracing** (`cdmp-distributed-tracing`): `consumer.atomic_tx` spans showing timeout errors in the error rate panel.

---

## Root Cause Analysis

### Proximate cause

PostgreSQL connection pool exhaustion. The `messaging-service` was configured with a pool of 10 connections. An ad-hoc analytical query from the same PostgreSQL instance consumed 8 connections with a full-table sequential scan. Consumer workers waiting for connections experienced timeouts, which were classified by `classifyError()` as `TRANSIENT` errors, triggering retries. Messages that entered the retry queue at 11:19:40 had a maximum retry budget of 62 seconds. They exhausted their budget and entered the DLQ at 11:22.

### Why the analytical query consumed 8 connections

The analytical query was issued from a database client tool (DBeaver) connected directly to `messages_db` with no pool. Each ORDER BY and GROUP BY operation in the query planner opened additional internal connections for parallel query execution. PostgreSQL's `max_parallel_workers_per_gather` was set to 4, which combined with the query planner's decisions to use parallel aggregation, caused 8 worker connections to be opened.

### Why 47 messages and not more

The 62-second retry budget window limited the damage. Messages that entered the retry queue after 11:20:38 (22 seconds after pool exhaustion, when the last available connection was claimed) experienced a 62-second budget from their first retry. The pool recovered at 11:27:30. Messages that entered the retry queue before 11:26:28 (= 11:27:30 - 62s) exhausted their budget before the pool recovered.

### Why the error classifier used TRANSIENT for connection timeouts

`classifyError()` maps `QueryRunnerAlreadyReleasedError`, connection timeout errors, and `ETIMEDOUT` to the `TRANSIENT` class. This is correct — a connection timeout is generally transient. However, when the cause of the timeout is sustained pool exhaustion rather than a brief spike, the TRANSIENT classification causes the message to cycle through retries until the budget is exhausted. There is currently no distinction between "briefly unavailable" (correct to retry) and "systematically unavailable" (should not retry — escalate immediately).

### Why the database was shared between analytical and transactional workloads

`messages_db` is used by both the `messaging-service` (transactional) and by the data analyst's DBeaver connection (analytical). There is no query isolation between workloads. The data analyst was not aware that analytical queries on the `messages` table would compete for connections with the production application.

---

## Immediate Mitigation

1. Killed the analytical query via `pg_terminate_backend()` at 11:27 UTC
2. Connection pool recovered immediately
3. Consumer throughput restored within seconds
4. Manual DLQ replay for 47 messages — completed at 11:53 UTC

---

## Permanent Corrective Actions

### Action 1: Separate analytical and transactional database access

**Owner:** Infrastructure  
**Target:** 2024-03-25  
**Description:** Create a read replica for analytical queries. Direct all DBeaver and data-tool access to the read replica. The `messaging-service` connects to the primary only. This prevents analytical workloads from consuming transactional connection pool slots.

### Action 2: Add connection pool utilization alert

**Owner:** Platform team  
**Target:** 2024-03-22

```yaml
- alert: DBConnectionPoolExhausted
  expr: |
    pg_stat_activity_count{state="active", datname="messages_db"}
    / pg_settings{name="max_connections"} > 0.80
  for: 30s
  severity: critical
  annotations:
    summary: "PostgreSQL connection pool > 80% utilized — consumer throughput at risk"
```

### Action 3: Add `pg_stat_statements` monitoring for long-running queries

**Owner:** Platform team  
**Target:** 2024-03-29

```yaml
- alert: LongRunningQuery
  expr: pg_stat_activity_max_tx_duration{datname="messages_db"} > 30
  for: 0m
  severity: warning
  annotations:
    summary: "Query running > 30s on messages_db — may be impacting connection pool"
```

### Action 4: Implement DLQ bulk replay tooling

**Owner:** Backend team  
**Target:** 2024-03-29  
**Description:** The 47-message manual replay took 22 minutes. A bulk replay script that accepts a list of `eventId` values and atomically resets the outbox rows and attempt counters would reduce this to under 2 minutes. The script should: (1) validate that `processed_events` has no record for the eventId, (2) delete from `event_attempts`, (3) UPDATE `gateway_outbox_events` to `pending` with `lock_version + 1`, (4) log the replay action with operator name and timestamp.

### Action 5: Distinguish "brief transient" from "sustained unavailability" in error classifier

**Owner:** Backend team  
**Target:** 2024-04-05  
**Description:** Add a circuit-breaker pattern to the consumer: if the same TRANSIENT error class appears for the same error code on 3 consecutive attempts within 10 seconds, escalate to a `CIRCUIT_OPEN` state that routes to the DLQ immediately rather than consuming the retry budget. This preserves retry budget for genuinely transient failures and avoids budget exhaustion under sustained unavailability.

---

## Lessons Learned

**The 62-second retry budget is a fixed window that applies regardless of why the consumer is failing.** During a genuine 8-minute database outage, the retry budget is insufficient — messages will exhaust their budget and enter the DLQ regardless of how many attempts they make. The current design assumes transient failures resolve within 62 seconds. For longer outages, either the budget must be larger or there must be a mechanism to recognize and handle extended unavailability differently.

**Connection pool exhaustion has no alert.** The metric exists — `pg_stat_activity_count` — but no threshold was configured. This is a missing alert for a failure mode that is entirely predictable and preventable.

**Shared databases between transactional and analytical workloads create unpredictable blast radius.** The analyst had no visibility into the production impact of their query. The production service had no isolation from the analyst's query. Neither party had the information needed to prevent the conflict. Read replicas are the standard architectural response to this class of problem.

**Manual DLQ replay at 47 messages is acceptable but at 470 it would not be.** The bulk replay tooling should have been implemented at the same time as the DLQ alert. A zero-tolerance DLQ alert without automated or semi-automated recovery tooling means every DLQ event requires manual operator time proportional to the number of messages.

**The `classifyError()` function is working correctly, but the retry budget sizing was calibrated for transient failures, not sustained unavailability.** Both are valid failure modes, but they require different recovery strategies.

---

## Follow-up Tasks

| # | Task | Owner | Priority | Target |
|---|---|---|---|---|
| 1 | Provision PostgreSQL read replica and redirect analytical traffic | Infrastructure | P0 | 2024-03-25 |
| 2 | Add `DBConnectionPoolExhausted` alert | Platform | P0 | 2024-03-22 |
| 3 | Add `LongRunningQuery` alert (> 30s) | Platform | P0 | 2024-03-22 |
| 4 | Build DLQ bulk replay script | Backend | P0 | 2024-03-29 |
| 5 | Document: "Do not run analytical queries on primary messages_db" in team wiki | Platform | P0 | 2024-03-22 |
| 6 | Investigate circuit-breaker for sustained TRANSIENT errors | Backend | P1 | 2024-04-05 |
| 7 | Increase DB connection pool from 10 to 20 as interim measure | Backend | P1 | 2024-03-22 |
| 8 | Add `messaging.work` queue depth alert (> 200 for 2m) | Platform | P1 | 2024-03-22 |
