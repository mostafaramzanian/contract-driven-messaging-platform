# docs/ — File Inventory

This file is the quick-reference index for everything in `docs/`. Each entry states what the file is for and when to read it.

---

## Architecture & Design

| File | When to read |
|------|-------------|
| `architecture-diagram.md` | Mermaid source for the full system diagram |
| `architecture.svg` | Hero SVG — five-layer system overview |
| `architecture-internals.md` | Deep-dive into EventRegistry, envelope schema, code paths |
| `evolution.md` | 10-phase narrative of how the system was built from scratch |
| `contract-evolution.md` | How to evolve a contract version; v1 → v2 lifecycle |

## Decision Records

See `adr/` — 6 ADRs covering every major architectural choice.

## Operational

| File | When to read |
|------|-------------|
| `observability.md` | Correlation ID propagation, tracing setup, metrics wiring |
| `production-readiness-fixes.md` | Changes from staff-level production-readiness review |
| `architectural-gap-closure.md` | Producer-side outbox + consumer event routing fixes |
| `testing.md` | Four-layer test strategy and how to run each layer |

## Runbooks & Postmortems

- `runbooks/` — 5 operator runbooks with step-by-step investigation commands
- `postmortems/` — 5 blameless postmortems documenting real failure sequences

## Observability Assets

| File | When to use |
|------|-------------|
| `grafana-dashboards.json` | Import into Grafana — 4 pre-built dashboards |
| `dashboard-system-overview.svg` | Screenshot of the system overview dashboard |
| `dashboard-reliability.svg` | Screenshot of the reliability dashboard |
| `dashboard-outbox-health.svg` | Screenshot of the outbox health dashboard |
| `dashboard-distributed-tracing.svg` | Screenshot of the distributed tracing dashboard |
