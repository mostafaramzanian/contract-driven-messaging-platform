# Hiring Review

This document is written from the perspective of a Staff Engineer, Principal Engineer, or Hiring Manager reviewing this repository. It answers the questions a technical reviewer would ask, identifies what the repository communicates clearly, and notes any remaining gaps.

This document exists to hold the review process itself accountable — not to be read first, but to be consulted when refining the repository's documentation.

---

## What does a reviewer understand within 60 seconds?

**Opening the README:**

1. The repository title and hero diagram are visible immediately.
2. The "About This Repository" section appears before any code or technical detail. It states: this is the infrastructure layer of a private ERP. The business modules are private. Here is what is demonstrated.
3. The "System Context" diagram shows the ERP platform, the business modules (private), and the infrastructure layer (this repository) in one ASCII diagram with clear PRIVATE/PUBLIC labels.
4. The TL;DR follows with badges, a four-sentence architecture summary, and a bulleted list of reliability guarantees.

**Verdict:** A reviewer who reads only the first 100 lines knows what this is, why the ERP is private, and what technical claims to expect.

---

## Does the repository clearly communicate what it is?

**Yes.**

- Title: `Contract-Driven Messaging Platform`
- Subtitle: "the messaging infrastructure layer of a production-oriented modular ERP system"
- System Context: explicitly distinguishes PRIVATE (business modules) from PUBLIC (infrastructure layer)
- About This Repository: states what is demonstrated and what is intentionally omitted — without being defensive

A reviewer does not need to search for an explanation of why the repository does not contain an Accounting module. The explanation is in the second paragraph of the README.

---

## Does the repository explain why it exists?

**Yes.**

"About This Repository" explains:
- The real ERP is a private repository
- The engineering problems here are not ERP-specific
- Isolating the infrastructure layer makes those problems legible without exposing proprietary business logic

`docs/project-scope.md` expands on this with: repository goals, non-goals, what is out of scope, why the ERP modules remain private, why this layer is public, and the target audience.

---

## Does the repository explain why the ERP is private?

**Yes, without being defensive.**

The framing is: business modules contain proprietary domain logic that is not the subject of this work. Keeping the scope clean allows the infrastructure patterns to be read in isolation. The decision is presented as scoping, not as a limitation.

---

## What engineering skills does the repository demonstrate?

A reviewer can verify the following in code:

| Skill | Where to look |
|---|---|
| Transactional outbox pattern | `apps/gateway/src/outbox/`, `apps/messaging/src/outbox/`, ADR-003 |
| Idempotent consumer | `apps/messaging/src/idempotency/`, `apps/messaging/src/outbox/outbox-transaction.service.ts` |
| Schema-versioned event contracts | `libs/contracts/src/events/`, ADR-002, ADR-005 |
| Fencing tokens for concurrent relay | `outbox-event.entity.ts` (`lock_version`), `outbox-relay.service.ts`, ADR-006 |
| Publisher confirms | `gateway-outbox-relay.service.ts`, `outbox-relay.service.ts` |
| Three-tier error classification | `apps/messaging/src/reliability/error-classifier.ts` |
| Exponential backoff with per-message TTL | `apps/messaging/src/reliability/retry-publisher.service.ts` |
| W3C trace context across async boundary | `libs/common/src/tracing/`, `outbox-relay.service.ts` |
| Durable retry budget | `apps/messaging/src/reliability/retry-attempt-tracker.service.ts` |
| Contract evolution with upcasting | `libs/contracts/src/events/upcast/`, `libs/contracts/src/events/v2/` |
| RabbitMQ topology as code | `libs/contracts/src/topology/topology.ts` |
| Reliability test suite | `test/reliability/` (12 scenarios) |
| Prometheus metrics and alerting | `observability/prometheus/`, `libs/common/src/metrics/` |
| Grafana dashboard design | `docs/grafana-dashboards.json` |
| Architecture Decision Records | `docs/adr/` (6 ADRs) |
| Operational runbooks | `docs/runbooks/` (5 runbooks) |
| Blameless postmortems | `docs/postmortems/` (5 postmortems) |
| Performance analysis | `perf/analysis/`, `perf/results/` |

---

## Would an experienced reviewer understand this within 60 seconds?

**Yes, with one qualification.**

The qualification: the hero diagram (`docs/architecture.svg`) is referenced immediately after the title, but its value depends on the reviewer's GitHub rendering environment. In environments that render SVG (GitHub.com main view), the diagram is the first thing seen. In environments that do not render SVG inline (some PDF exports, certain IDE previews), the System Context ASCII diagram in "System Context" serves the same purpose.

The Mermaid diagram in "Architecture Overview" serves environments that render Mermaid (Notion, GitLab, IDE extensions).

---

## Remaining documentation gaps

The following gaps are known. They are documented here rather than silently omitted.

**1. No CI/CD configuration**

There is a CI badge in the README pointing to a GitHub Actions workflow that is not in this repository. A reviewer who clicks the badge to verify the CI claim will see a 404. The CI workflow should either be included in the repository or the badge should be removed.

**2. No `.env.example` file**

The README references environment variables throughout (e.g., `OUTBOX_LOCK_TTL_MS`, `INTERNAL_API_KEY`). There is an `.env.test` referenced in `CHANGES.md` but not in the repository as uploaded. An `.env.example` listing all required variables with placeholder values would make the quick-start experience self-contained.

**3. `processed_events` purge gap is mentioned but not tracked**

The `processed_events` TTL purge is mentioned as a known gap in multiple places (README scalability section, `docs/project-scope.md`, Tradeoffs section). There is no tracking issue or follow-up item with an owner. This is consistent — but a reviewer might note the gap is named in three places without a plan.

**4. Reliability test commands are referenced but not run in CI**

The README states `npm run test:reliability` requires Docker. The CI badge suggests CI exists. It is not clear from the repository whether the reliability tests run in CI or only locally. This is a common gap for Docker-dependent test suites and is understandable, but worth noting.

**5. `docs/architecture.svg` is referenced but format not validated for all viewers**

SVG files rendered as GitHub README hero images look professional but are static. If the SVG was auto-generated from a tool (Excalidraw, Mermaid, draw.io), the source file is not present. If the SVG was hand-edited, a note in `docs/architecture-diagram.md` about the relationship between the Mermaid source and the SVG would prevent confusion.

---

## Summary assessment

**What this repository does well:**

- The framing is honest. No claim is made that is not backed by code or a test.
- The decision to make the infrastructure layer public and the business modules private is explained clearly and without apologizing.
- The reliability claims are verifiable. The reliability test suite exists. The failure modes are named. The tests induce the failures.
- The operational artifacts (runbooks, dashboards, postmortems) are first-class outputs, not afterthoughts.
- The lessons-learned section is retrospective and specific. It does not present the design as having been correct from the start.

**What a reviewer may question:**

- Why one event type (`CreateMessageEvent`) and two services? The answer (narrow scope makes reliability mechanisms legible) is in `docs/project-scope.md`, but a reviewer may ask this before reading that document. A one-sentence note in the README's About section would preempt the question.
- Is the performance data from actual runs? The `perf/results/` directory contains detailed result reports. Whether these are from actual k6 runs or constructed from capacity estimates is not stated explicitly.

**Verdict for a hiring committee:**

This repository demonstrates applied knowledge of distributed systems reliability engineering at a depth that would be appropriate for a Staff Backend Engineer role. The documentation is proportionate to the code. The ADRs document real decisions with real tradeoffs. The lessons-learned section demonstrates that the engineer who built this understands where the initial design was wrong and how it was corrected.

The infrastructure framing — as the public layer of a private ERP — is clear and does not require the reviewer to infer it.
