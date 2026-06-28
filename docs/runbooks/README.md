# Operational Runbooks

This directory contains on-call runbooks for the `contract-driven-messaging-platform`. Each runbook is written for an engineer responding to a production alert, including at 3 AM without prior context.

## Runbook Index

| RB | Alert | Severity | What Broke |
|---|---|---|---|
| [RB-001](RB-001-dlq-growth-spike.md) | `DLQMessageReceived` | Critical | A message exhausted its retry budget. Business event unprocessed. |
| [RB-002](RB-002-rabbitmq-outage.md) | `RabbitMQWorkQueueDepth` · `OutboxRelayLagging` | Critical | Broker unreachable. No events delivered. Outbox accumulating. |
| [RB-003](RB-003-outbox-relay-backlog.md) | `OutboxRelayLagging` | Warning → Critical | Relay not draining. Delivery latency growing. DLQ risk if unresolved. |
| [RB-004](RB-004-publisher-confirm-failure.md) | `OutboxPublishConfirmFailure` | Critical | At-least-once guarantee at risk. Possible event loss. |
| [RB-005](RB-005-trace-propagation-failure.md) | `TraceOrphanSpansElevated` | Warning | Trace context lost at outbox boundary. Debugging impaired. |

## How to Use These Runbooks

Each runbook follows a consistent structure:

1. **Symptoms** — What you observe when paged
2. **Detection Signals** — Prometheus queries and Grafana panels to open first
3. **Root Cause Analysis** — Named causes (A, B, C...) — identify which before acting
4. **Investigation Steps** — Ordered steps with exact commands
5. **Recovery Procedure** — Per-cause recovery, labeled to match the RCA section
6. **Validation Checklist** — Confirm resolution before closing the incident
7. **Postmortem Questions** — Document answers for the postmortem

## Alert → Runbook Mapping

```
DLQMessageReceived              → RB-001
RabbitMQWorkQueueDepth          → RB-002 (if broker is down)
OutboxRelayLagging              → RB-002 (if broker is down) OR RB-003 (if broker is up)
OutboxPublishConfirmFailure     → RB-004
TraceOrphanSpansElevated        → RB-005
```

**Distinguishing RB-002 from RB-003:**
Both trigger `OutboxRelayLagging`. Check `up{job="rabbitmq"}`:
- `up{job="rabbitmq"} = 0` → RB-002 (broker down)
- `up{job="rabbitmq"} = 1` and `rate(outbox_published_total[1m]) = 0` → RB-002 (network partition or auth failure)
- `up{job="rabbitmq"} = 1` and `rate(outbox_published_total[1m]) > 0` → RB-003 (relay is publishing, just too slowly)

## Emergency Contact Escalation

| Tier | Contact | When |
|---|---|---|
| On-call Engineer | PagerDuty rotation | All severity: warning and critical |
| Platform Lead | Phone | RB-004 (data loss risk), RB-002 > 10 min outage |
| Engineering Manager | Phone | RB-004 confirmed data loss, RB-002 > 30 min outage |

## Common SQL Queries (Quick Reference)

```sql
-- Outbox pending count
SELECT COUNT(*) FROM gateway_outbox_events WHERE status = 'pending';

-- Stale processing rows (relay crashed while holding claim)
SELECT id, event_id, locked_at, NOW() - locked_at AS lock_age
FROM gateway_outbox_events
WHERE status = 'processing' AND locked_at < NOW() - INTERVAL '60 seconds';

-- Event attempt counts (retry budget usage)
SELECT event_id, count, last_attempt_at FROM event_attempts ORDER BY count DESC LIMIT 10;

-- Idempotency check for a specific event
SELECT event_id, created_at FROM processed_events WHERE event_id = '<eventId>';

-- Was the business write committed?
SELECT id, correlation_id, created_at FROM messages WHERE correlation_id = '<correlationId>';

-- Outbox table size
SELECT pg_size_pretty(pg_total_relation_size('gateway_outbox_events'));
```

## Common RabbitMQ Commands (Quick Reference)

```bash
# Broker health
curl -s -u guest:guest http://localhost:15672/api/healthchecks/node | jq .

# Queue depths
curl -s -u guest:guest http://localhost:15672/api/queues \
  | jq '.[] | {name, messages, consumers}'

# Get DLQ message (peek, requeue=true)
curl -s -u guest:guest http://localhost:15672/api/queues/%2F/messaging.dlq/get \
  -X POST -H "Content-Type: application/json" \
  -d '{"count":1,"requeue":true,"encoding":"auto","ackmode":"ack_requeue_true"}'

# Memory and disk alarms
curl -s -u guest:guest http://localhost:15672/api/nodes \
  | jq '.[] | {name, mem_alarm, disk_free_alarm}'
```
