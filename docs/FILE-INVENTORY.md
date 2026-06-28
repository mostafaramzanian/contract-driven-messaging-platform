# docs/ — File Inventory

This file is the quick-reference index for everything in `docs/`. Each entry states what the file is for and when to read it.

---

## Repository Context

| File | When to read |
|------|-------------|
| `project-scope.md` | Repository goals, non-goals, why ERP modules are private, target audience, and interview value |
| `hiring-review.md` | Staff/Principal Engineer perspective — what the repository communicates, remaining gaps |

## Architecture & Design

| File | When to read |
|------|-------------|
| `architecture-diagram.md` | Mermaid source for the full system diagram |
| `architecture.svg` | Hero SVG — five-layer system overview |
| `architecture-internals.md` | Deep-dive into EventRegistry, envelope schema, code paths |
| `evolution.md` | 10-phase narrative of how the system was built from scratch |
| `contract-evolution.md` | How to evolve a contract version; v1 → v2 lifecycle |

## Decision Records

See `adr/` — 6 ADRs covering every major architectural choice in the infrastructure layer.

| ADR | Decision |
|-----|---------|
| ADR-001 | RabbitMQ as message broker |
| ADR-002 | Contract-driven event design |
| ADR-003 | Transactional outbox pattern |
| ADR-004 | At-least-once delivery with idempotent consumers |
| ADR-005 | Immutable event versioning with upcasting |
| ADR-006 | Fencing tokens for outbox relay concurrency |

## Implementation

| File | When to read |
|------|-------------|
| `observability.md` | Correlation ID propagation, tracing setup, metrics wiring |
| `testing.md` | Four-layer test strategy and how to run each layer |
| `production-readiness-fixes.md` | Changes made in response to staff-level production-readiness review |
| `architectural-gap-closure.md` | Producer-side outbox + consumer event routing fixes |

## Runbooks & Postmortems

- `runbooks/` — 5 operator runbooks with step-by-step investigation commands
  - RB-001: DLQ growth spike
  - RB-002: RabbitMQ outage
  - RB-003: Outbox relay backlog
  - RB-004: Publisher confirm failure
  - RB-005: Trace propagation failure
- `postmortems/` — 5 blameless postmortems documenting actual failure sequences
  - PM-001: RabbitMQ broker OOM kill
  - PM-002: Outbox relay backlog
  - PM-003: DLQ growth / connection pool exhaustion
  - PM-004: Publisher confirm failures
  - PM-005: Trace propagation break / OTel Collector OOM

## Observability Assets

| File | When to use |
|------|-------------|
| `grafana-dashboards.json` | Import into Grafana — 4 pre-built dashboards |
| `dashboard-system-overview.svg` | Screenshot: system throughput and SLO panel |
| `dashboard-reliability.svg` | Screenshot: retry funnel and DLQ alert |
| `dashboard-outbox-health.svg` | Screenshot: outbox pending depth and relay latency |
| `dashboard-distributed-tracing.svg` | Screenshot: E2E latency and trace orphan count |
