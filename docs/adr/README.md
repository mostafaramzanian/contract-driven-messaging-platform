# Architecture Decision Records

This directory contains the Architecture Decision Records (ADRs) for the messaging infrastructure layer of the ERP platform. Each ADR documents a significant architectural decision within the **infrastructure layer** — the messaging backbone, reliability mechanisms, contract governance, and observability instrumentation. Decisions about the ERP's business modules, domain logic, and deployment topology are outside the scope of this directory.

Each ADR documents the context that motivated the decision, the alternatives that were considered, and the consequences that follow — including operational consequences. ADRs are immutable once accepted. Superseded decisions are marked as such and link to the superseding ADR.

## Index

| ADR | Title | Status | Date |
|---|---|---|---|
| [ADR-001](ADR-001-rabbitmq-vs-kafka.md) | RabbitMQ as Message Broker | Accepted | 2024-01-10 |
| [ADR-002](ADR-002-contract-driven-design.md) | Contract-Driven Event Design | Accepted | 2024-01-10 |
| [ADR-003](ADR-003-transactional-outbox.md) | Transactional Outbox Pattern | Accepted | 2024-01-12 |
| [ADR-004](ADR-004-at-least-once-delivery.md) | At-Least-Once Delivery with Idempotent Consumers | Accepted | 2024-01-12 |
| [ADR-005](ADR-005-event-versioning.md) | Immutable Event Versioning with Upcasting | Accepted | 2024-01-15 |
| [ADR-006](ADR-006-fencing-tokens.md) | Fencing Tokens for Outbox Relay Concurrency | Accepted | 2024-01-18 |

## How to Read an ADR

Each ADR follows this structure:

- **Status** — Current state: `Proposed`, `Accepted`, `Deprecated`, `Superseded by ADR-NNN`
- **Context** — The situation and constraints that made a decision necessary
- **Problem Statement** — The specific problem the decision addresses
- **Decision** — What was decided and the primary rationale
- **Alternatives Considered** — Other options that were evaluated and why they were not chosen
- **Tradeoffs** — What the decision gains and what it costs, stated honestly
- **Consequences** — Observable changes to the system as a result of the decision
- **Operational Impact** — Runbooks, alerts, and operational concerns introduced by the decision
- **Future Considerations** — Known limitations that may require a follow-up decision

## Proposing a New ADR

Copy `ADR-000-template.md`, increment the number, fill in all sections, and open a pull request. A decision is accepted when it has been reviewed and merged. Do not edit accepted ADRs; create a new ADR that supersedes the old one.
