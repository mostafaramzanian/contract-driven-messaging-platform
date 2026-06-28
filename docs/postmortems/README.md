# Incident Postmortems

This directory contains postmortem reports for production incidents affecting the `contract-driven-messaging-platform`. Postmortems are written within 48 hours of incident resolution and are reviewed in the weekly engineering sync.

Postmortems are blameless. They document what happened and what the system and team can do better — not who made a mistake.

## Index

| ID | Title | Severity | Date | Duration | Status |
|---|---|---|---|---|---|
| [PM-001](PM-001-rabbitmq-outage.md) | RabbitMQ broker process crash — full delivery outage | SEV-1 | 2024-02-14 | 23 min | Resolved |
| [PM-002](PM-002-outbox-relay-backlog.md) | Outbox relay backlog — delivery latency degradation | SEV-2 | 2024-03-01 | 47 min | Resolved |
| [PM-003](PM-003-dlq-growth-spike.md) | DLQ growth spike — PostgreSQL connection pool exhaustion | SEV-1 | 2024-03-18 | 31 min | Resolved |
| [PM-004](PM-004-publisher-confirm-failures.md) | Publisher confirm failures — RabbitMQ memory alarm | SEV-2 | 2024-04-02 | 18 min | Resolved |
| [PM-005](PM-005-trace-propagation-break.md) | Trace propagation break — OTel Collector OOM | SEV-3 | 2024-04-19 | 4 hr 12 min | Resolved |

## Severity Definitions

| Level | Definition |
|---|---|
| SEV-1 | Complete loss of message delivery. Events are not being processed. DLQ events accumulating or data loss risk. |
| SEV-2 | Degraded delivery. Events are being processed but with elevated latency, elevated error rate, or sustained backlog. |
| SEV-3 | Observability or operational capability degraded. Business functionality not impacted. |

## Postmortem Process

1. Incident commander writes a draft within 24 hours of resolution
2. Draft reviewed by at least one other engineer within 48 hours
3. Follow-up tasks tracked in the engineering backlog
4. Postmortem presented in weekly engineering sync
5. Closed when all follow-up tasks are assigned (not necessarily completed)
