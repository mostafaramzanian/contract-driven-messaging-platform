# Engineering Scope

This document defines what this repository is, who it is for, and what it deliberately does not contain.

---

## Repository Goals

**1. Demonstrate the infrastructure layer of a production-oriented ERP**

The public ERP has a private business layer (Inventory, Accounting, CRM, HR, Purchasing, Sales, Workflow). This repository extracts and documents the distributed systems layer that sits beneath all of those modules — the messaging backbone, reliability mechanisms, observability instrumentation, and operational procedures.

**2. Verify reliability claims against real failure modes**

Every reliability guarantee in this repository is backed by a test that induces the failure it claims to handle. The transactional outbox, idempotent consumer, fencing tokens, and retry budget are not described — they are exercised against a live broker and database in the reliability suite.

**3. Document engineering decisions with their actual tradeoffs**

Architecture Decision Records are included not to explain what was chosen, but to explain what alternatives were considered and why the chosen approach was preferred in this specific context. ADRs that document tradeoffs honestly are more useful than ones that justify decisions after the fact.

**4. Provide operational artifacts that match the code**

Runbooks, dashboards, alert rules, and postmortems are treated as engineering outputs with the same review rigor as code. Each runbook maps to a named alert. Each postmortem maps to an incident that resulted in a code or configuration change.

**5. Make the system legible to a technical reviewer in under 10 minutes**

The Repository Tour, the architecture diagram, and the Reliability Guarantees section are structured so that someone who has never read this codebase can identify what it does, how it handles failure, and where to look for the code that implements each claim.

---

## Repository Non-Goals

**Business modules and domain logic**

Inventory, Accounting, CRM, HR, Purchasing, Sales, Workflow — none of these are in this repository. They belong to the private ERP. Including domain-specific entities would require exposing proprietary business rules and data models that are not the subject of this work.

**Authentication and authorization**

The system includes a simple internal API key guard on admin endpoints. Full authentication (OAuth, JWT, session management), RBAC, and multi-tenancy are application-layer concerns handled by the private ERP's business modules.

**Production infrastructure configuration**

Kubernetes manifests, Helm charts, Terraform modules, and cloud provider configuration are deployment concerns. They are specific to the ERP's internal infrastructure team and are not transferable to a public portfolio context without significant modification.

**Exactly-once delivery**

The system guarantees at-least-once delivery with idempotent consumption. True exactly-once delivery across a message broker and a relational database requires distributed transactions that RabbitMQ does not support. This is documented as a tradeoff in the README and in ADR-004, not masked.

**Feature completeness**

This is a focused demonstration of specific distributed systems patterns, not a general-purpose messaging platform. It implements one event type across two services. The narrow scope is what makes the reliability mechanisms legible — a broader scope would require more abstraction layers that would obscure what this repository is trying to show.

---

## What Is Out of Scope

The following are known gaps that are not planned for this repository:

| Gap | Why out of scope |
|-----|-----------------|
| `processed_events` TTL purge job | Operational concern; implementation is straightforward but requires a deployment target to be meaningful |
| PostgreSQL `LISTEN`/`NOTIFY` relay wake | Performance optimization; not required to demonstrate the outbox pattern |
| Change Data Capture via Debezium | Adds Kafka Connect as an operational dependency; CDC is documented as a tradeoff, not a planned addition |
| Saga or CQRS | Different architectural patterns; this repository demonstrates event-driven messaging, not long-running workflows or read-model separation |
| Kafka | Documented as an alternative in ADR-001; adding Kafka would not improve the demonstration and would obscure the RabbitMQ topology decisions |
| Multi-region replication | Requires a concrete business domain to reason about correctly; documented as a tradeoff in the README |
| Schema registry | Central governance improvement; documented as a future consideration in ADR-002 |
| Automated DLQ replay | Operator tooling; the admin endpoints exist, automated scheduling is a deployment concern |

---

## Why the ERP Modules Remain Private

Business modules contain domain logic, pricing rules, inventory calculations, accounting formulas, and HR data structures that are specific to the ERP's customers and business model. Exposing that logic would:

1. Reveal proprietary business rules that belong to the ERP's domain, not to the infrastructure layer
2. Require sanitizing or faking domain data in a way that would make the code misleading
3. Add coupling between the infrastructure demonstration and the specific business domain, making the infrastructure patterns harder to read in isolation

Keeping the split clean means readers can focus on the infrastructure decisions without reasoning about unrelated business logic.

---

## Why the Infrastructure Layer Is Public

The engineering problems in this repository — schema drift, lost events, duplicate processing, retry storms, trace context propagation across async boundaries — are not ERP-specific. They occur in any event-driven distributed system. Publishing this layer:

1. Separates the infrastructure decisions from the business decisions, making both clearer
2. Provides a concrete codebase behind the engineering patterns described in the ADRs and lessons-learned sections
3. Allows technical reviewers to verify the reliability claims against the test suite, not just against prose descriptions

---

## Target Audience

**Technical reviewers in a hiring context**

This repository is structured to support a deep technical discussion. Every claim in the README is backed by code. Every reliability pattern has a corresponding failure-mode test. The Interview Discussion Topics section maps directly to the patterns implemented here.

**Engineers evaluating distributed systems approaches**

The ADRs, lessons-learned section, and tradeoffs section are written for engineers who are making similar decisions in their own systems. The goal is not to present the chosen approach as definitively correct, but to document what the alternatives were and why this approach was preferred for this context.

**On-call engineers operating similar infrastructure**

The runbooks and dashboards are usable artifacts. They are not representative of the full ERP's operations, but they demonstrate what production-grade operational documentation looks like for this class of system.

---

## Interview Value

This repository is structured to support technical discussions in these areas:

**Distributed Systems**
- Why `SELECT ... FOR UPDATE SKIP LOCKED` is insufficient without a fencing token
- The failure modes that at-least-once delivery introduces (and how idempotency addresses them)
- Trace context propagation across async boundaries

**Reliability Engineering**
- Defense in depth: validation at producer, validation at consumer, idempotency at the database, retry budget at the AMQP layer and the durable counter
- How to distinguish transient from permanent failures, and why the distinction matters for DLQ hygiene
- What a reliability test exercises that a unit test does not

**Operational Readiness**
- What makes a runbook useful vs. one that restates the alert message
- Blameless postmortem structure and how follow-up items connect to code changes
- Dashboard design for infrastructure with async boundaries

---

## Learning Goals

This codebase was built to work through the following questions from first principles, not from documentation:

1. What exactly fails when a process crashes between a database commit and a `channel.publish()`?
2. What does `SKIP LOCKED` guarantee, and what does a fencing token add to it?
3. Why must the idempotency INSERT be in the same transaction as the business write?
4. How does the W3C trace context survive an async outbox boundary?
5. What happens to a retry budget when an operator manually requeues a DLQ message?
6. Why does per-message TTL not behave as expected under queue depth?

Each question has a corresponding lesson in the [Lessons Learned](../README.md#lessons-learned) section, and a reliability test that demonstrates the failure mode the lesson is about.
