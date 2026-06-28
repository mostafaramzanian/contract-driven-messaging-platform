# Repository Evolution

This document traces how `contract-driven-messaging-platform` evolved from a basic async messaging spike into its current form. Each phase is documented with the problem that motivated it, the specific failure mode it addressed, the tradeoffs it introduced, and the engineering lesson that came out of it.

The evolution is not presented as a success story. Several phases were direct responses to bugs or design errors discovered in the previous phase. That is the normal shape of reliability engineering.

---

## Phase 1 — Initial Messaging Prototype

**Implemented:** Basic AMQP publish from gateway to consumer. No contracts. No reliability.

### What was built

A gateway HTTP handler called `channel.publish()` directly with a JSON-serialized payload. The consumer called `channel.consume()` with `noAck: true`. Events were TypeScript objects with local interface definitions in each service.

### Problem discovered

The dual-write problem appeared immediately during manual testing. The gateway wrote to the database, then published to the broker. When the process was killed between those two steps during testing, the database record existed but the consumer never received the event. On restart, there was no mechanism to detect or replay the missed event.

Additionally, a field rename in the payload (`subject` → `title`) during a refactor caused the consumer to silently process events with `payload.title === undefined`. TypeScript compilation passed because the consumer's local interface still defined the old field as optional. The schema drift was invisible until a downstream data check noticed missing data.

### Tradeoffs introduced

Speed of initial implementation. No infrastructure dependencies beyond the broker.

### Engineering lesson

Two things became clear simultaneously: the dual-write problem is not hypothetical, and TypeScript interfaces are not a contract — they are erased at compile time and provide no protection at the wire level across independently deployed services. Both problems needed structural solutions, not workarounds.

---

## Phase 2 — Manual Acknowledgement

**Implemented:** Switched consumer to `noAck: false`. Added explicit `channel.ack()` and `channel.nack()` calls.

### Problem it addressed

With `noAck: true`, the broker removed a message from the queue the moment it was delivered to the consumer. If the consumer crashed after receiving the message but before processing it, the message was gone. Manual ACK ensures the broker holds the message until the consumer explicitly confirms it.

### What changed

The consumer now holds the message in an unacknowledged state during processing. `channel.ack()` is called after the database write commits. `channel.nack(msg, false, true)` is called on processing errors, requeuing the message for redelivery.

### New limitation discovered

Naive `nack` with `requeue: true` causes infinite retry loops. A message that fails consistently — due to a permanent bug in the consumer, a schema that cannot be parsed, or a database constraint that will always reject the payload — is requeued indefinitely. The consumer logs fill with the same error on an increasing frequency as the broker tries to redeliver. There is no budget, no backoff, and no dead-letter path.

### Engineering lesson

Manual ACK is necessary but not sufficient. It gives you "at-least-once" semantics — but "at-least-once with infinite retry" is not a reliability guarantee, it is a failure amplifier.

---

## Phase 3 — Retry Queue and Dead-Letter Exchange

**Implemented:** RabbitMQ DLX topology. `messaging.work` → nack → `messaging.dlx` → `messaging.retry.q` → TTL expiry → back to `messaging.work`. Budget-exhausted messages route to `messaging.dlq`.

### Problem it addressed

Infinite retry loops. A message that fails on every attempt now has a bounded retry budget (5 attempts) with exponential backoff (2ⁿ×2s per attempt, cumulative ~62s window). After exhausting the budget, the message routes to `messaging.dlq` for manual inspection rather than cycling indefinitely.

### Error classification added

Three-tier error classification in the consumer:
- `VALIDATION` — schema parse failure. Message will never become valid. Ack immediately (do not nack — the message is not retriable).
- `TRANSIENT` — infrastructure failure (DB connection timeout, etc.). Nack and retry.
- `PERMANENT` — business logic failure that will recur regardless of timing. Route directly to DLQ.

### Tradeoffs introduced

**RabbitMQ topology becomes critical infrastructure.** A misconfigured DLX binding silently drops nacked messages — they disappear without reaching either the retry queue or the DLQ. This was discovered during a topology test when a queue argument had a casing mismatch (`messaging.dlx` vs `messaging.DLX`). The broker accepted both declarations and treated them as different exchanges. All nacked messages were silently discarded for approximately 20 minutes before the test caught the gap.

**Per-message TTL has a head-of-queue limitation.** RabbitMQ only expires a message when it reaches the head of the queue. Under high backlog, the backoff delay is approximate rather than precise.

### Engineering lesson

Topology declaration code deserves the same review rigor as schema migration code. A topology bug produces no error — it produces silence. All exchange, queue, and binding names should be constants in a shared module, topology should be asserted on every service start, and topology assertions should have integration test coverage.

---

## Phase 4 — Transactional Outbox (Producer Side)

**Implemented:** Gateway no longer calls `channel.publish()` directly. Instead, it writes an outbox row to `gateway_outbox_events` in the same database transaction as any business writes. A separate relay service polls the outbox and publishes to RabbitMQ with publisher confirms.

### Problem it addressed

The dual-write problem from Phase 1. The gateway's HTTP handler was doing two independent writes: database, then broker. A crash between them left the system in an inconsistent state with no recovery path.

The outbox pattern makes the database the single source of truth. The outbox row is written atomically with the business state. The relay is a best-effort publisher that can be restarted without data loss — the outbox row persists until the relay marks it `sent` after receiving a publisher confirm.

### Architecture change

The gateway's HTTP response time is now decoupled from broker availability. The gateway returns `202 Accepted` after the outbox INSERT commits. The relay delivers asynchronously. End-to-end delivery latency now has a floor set by the relay poll interval (default 5s).

### Tradeoffs introduced

**Delivery latency has a new floor.** P99 E2E latency increased from ~50ms (direct publish) to ~5,200ms (outbox poll interval ceiling + broker + consumer). This is not a regression — it is the cost of decoupling gateway availability from broker availability.

**The outbox table requires a purge job.** Without one, it grows without bound. The first production incident caused by this (PM-002) occurred 14 days after the table was created. The table bloat degraded relay throughput by ~15% before it was detected.

**The relay poll adds an operational dependency.** If the relay process stalls (event loop saturation, OOM), outbox rows accumulate. A pending-events metric and alert are necessary to detect this.

### Engineering lesson

The outbox pattern makes delivery reliable, but it introduces two new maintenance responsibilities: purging old rows and monitoring relay lag. Neither is optional in production. Both were documented but not implemented for several weeks, which led to PM-002.

---

## Phase 5 — Publisher Confirms

**Implemented:** Relay switched from fire-and-forget `channel.publish()` to `ConfirmChannel` with `waitForConfirms()`. `markSent()` is called only after the broker confirms durable receipt.

### Problem it addressed

Fire-and-forget publish meant the relay could mark a row `sent` even if the broker had not durably stored the message. Under RabbitMQ flow control or a brief connection reset, `channel.publish()` returns `false` — but without confirms, this return value was not being checked. Rows were marked `sent`, the broker never confirmed the message, and events were silently lost.

PM-004 documented the first production occurrence: 12 events were published during a flow-control window, received AMQP nacks, and were correctly not marked `sent` — but only because confirms were implemented by that point. Without confirms, those 12 events would have been silently lost with no observable signal.

### Tradeoffs introduced

**Latency increase.** Every publish now waits for a broker round-trip confirm before advancing. At 5ms average confirm latency and a batch size of 25, this caps single-relay throughput at approximately 5,000 msg/s theoretically, and ~250 msg/s practically (accounting for Node.js event loop overhead and database claim latency).

**Relay throughput becomes the first bottleneck.** See capacity model in `perf/analysis/capacity-model.md`.

### Engineering lesson

The at-least-once guarantee lives or dies at the confirm boundary. A relay without confirms is providing a guarantee that it cannot actually keep. The performance cost of confirms — one broker round-trip per confirm — is the price of the guarantee. It is not optional.

---

## Phase 6 — OpenTelemetry Distributed Tracing

**Implemented:** W3C `traceparent`/`tracestate` captured at outbox INSERT time and stored in the `trace_context` column. Relay restores context before publishing. Consumer extracts context from AMQP headers. All spans exported to OTel Collector → Jaeger.

### Problem it addressed

Without trace context propagation, every relay publish appeared as a new root span in Jaeger. Debugging a consumer failure required manually correlating `correlationId` across gateway logs, relay logs, and consumer logs — three separate log searches with no visual trace of the event's path.

The async boundary between the HTTP response (at gateway) and the relay publish (at a different time, in a different event loop tick) breaks standard OTel auto-instrumentation. Auto-instrumentation propagates context within a synchronous call chain. It does not propagate through a database row.

### Architecture change

The `trace_context` column in both outbox tables is required infrastructure. Removing it breaks trace continuity. Any future outbox relay must restore context before creating spans.

### Tradeoffs introduced

**Every span export adds a collector network hop.** If the collector is down, spans are dropped silently. The OTel SDK does not retry indefinitely — it has an in-memory buffer that, if the collector is unavailable long enough, fills and drops spans.

**The collector itself can OOM.** PM-005 documented the first production occurrence: the collector accumulated spans in its retry buffer over 4 days due to intermittent network MTU issues between the collector and Jaeger. The process was OOM-killed and the incident went undetected for 4 hours because the collector was not a Prometheus scrape target.

### Engineering lesson

Every infrastructure component that the system depends on for observability or correctness must be monitored. The collector was "deployed" but not "monitored." Those are different states. An infrastructure component without a `up` metric and an alert is in an unknown state, not a healthy state.

---

## Phase 7 — Event Versioning and Upcasting

**Implemented:** `CreateMessageEvent` schema split into v1 (original) and v2 (with `metadata` field). Version dispatch in the consumer: `envelope.schemaVersion` → AMQP header → routing key suffix → default v1. Deterministic v1→v2 upcaster. Backward-compatibility fixture test.

### Problem it addressed

The first schema change (adding `metadata` as a required field) would have required a synchronized deployment of gateway and consumer if handled by in-place schema mutation. In a real environment, synchronized deployments of multiple services are operationally expensive and error-prone. Schema versioning allows independent deployment: a v2 gateway can emit v2 events while v1 gateways are still running; the consumer handles both.

### Critical constraint on the upcaster

The upcaster must be deterministic. A v1 event that is delivered twice (once on first delivery, once on a retry after the first delivery failed) must produce the same v2 shape on both passes. If the upcaster calls `randomUUID()` or `new Date()`, the two upcasted shapes will differ, and the idempotency check (which compares by `eventId`, not by payload) will correctly identify them as the same event — but the business write on the first delivery and the rejected duplicate on the second delivery will have been computed from different inputs. This constraint is documented in `docs/adr/ADR-005-event-versioning.md` and enforced by a code review checklist item.

### Tradeoffs introduced

**Version accumulation.** Old version handlers must be maintained until all producers are confirmed to have migrated. The migration checklist (in ADR-005) is required before deprecating a version.

**The version resolution chain has precedence rules that must be consistent.** If the gateway and consumer disagree on which field takes precedence for version detection, a v2 message may be processed as v1 or vice versa. The resolution chain (`envelope field → header → routing key suffix → default v1`) is defined once and used consistently.

### Engineering lesson

Schema evolution without versioning is a synchronized deployment requirement in disguise. The first time you need to add a breaking field, you discover you should have built versioning from the start. Building it at the second schema change is better than the third.

---

## Phase 8 — Consumer Transactional Outbox

**Implemented:** Consumer now writes three things atomically: the idempotency record (`processed_events`), the business write (`messages`), and a downstream domain event outbox row (`outbox_events`). A consumer relay polls `outbox_events` and publishes domain events to `messaging.events` exchange.

### Problem it addressed

The consumer had the same dual-write problem at the output side as the gateway had at the input side. The consumer wrote the business record, then published the `MessagePersisted` domain event. A crash between those writes left downstream systems without the notification they needed.

### Architecture change

The consumer now has a full outbox cycle of its own: receive → validate → upcast → atomic write (idempotency + business + outbox) → ack → consumer relay → domain event publish. This is the same pattern as the gateway, applied to the consumer's output.

### Tradeoffs introduced

**Two relay processes share the consumer's event loop.** The application relay (for domain events) and the incoming AMQP consumer compete for the same Node.js event loop. Under heavy consumer load, the relay's poll timer can be delayed by event loop saturation. This is documented as a known limitation and a recommendation to separate the relay into its own process appears in `perf/analysis/capacity-model.md`.

**Two outbox tables require two purge jobs.** The consumer outbox (`outbox_events`) and the gateway outbox (`gateway_outbox_events`) both need independent retention management.

### Engineering lesson

Reliability patterns applied at the input boundary must also be applied at the output boundary. A system that receives reliably but publishes unreliably is not reliable end-to-end. The outbox pattern is a structural approach, not a per-service optimization.

---

## Phase 9 — Fencing Tokens and Idempotency Atomicity

**Implemented:** `lock_version` column on both outbox tables. Relay claim increments `lock_version`. `markSent()` uses a CAS update (`WHERE lock_version = :claimed`). Stale-lock reaper resets rows with expired claims and increments `lock_version` to invalidate stale relay tokens. Idempotency INSERT moved inside the same `QueryRunner` transaction as the business write.

### Problems addressed

**Fencing token (lock_version):** `SELECT ... FOR UPDATE SKIP LOCKED` prevents two relay instances from claiming the same row simultaneously. It does not protect against a relay that claims a row, is delayed (GC pause, slow confirm), and calls `markSent()` after the stale-lock reaper has already reset the row and another relay has claimed and published it. Without a fencing token, the stale relay's `markSent()` succeeds silently — the double-publish is invisible. With the CAS check, the stale `markSent()` matches zero rows and logs a structured warning with `claimedLockVersion` and current `lock_version`.

**Idempotency atomicity:** The original idempotency implementation was a check-then-write: query `processed_events` for the `eventId`, and if absent, proceed. Under concurrent delivery (two deliveries of the same event arriving simultaneously), both deliveries pass the existence check before either commits, and both proceed to the business write. The fix moves the idempotency INSERT into the same transaction as the business write, using the UNIQUE constraint as the serialization point. The losing concurrent delivery receives a `23505 unique_violation` and rolls back.

### Tradeoffs introduced

**Fencing token fires are expected under concurrent relay instances.** A fencing event is not an error — it means a stale relay detected and logged the concurrency race. The event was already published by the relay that claimed the row after the reaper reset it. The fencing metric (`outbox_fenced_publishes_total`) alerts at a sustained rate but not on isolated events.

**Idempotency requires `processed_events` rows to have a retention window longer than the maximum replay window.** If an event is replayed from the outbox 7 days after original processing and the `processed_events` row has been purged, the replayed event will be processed again. The 30-day retention on `processed_events` must exceed the maximum operational replay window.

### Engineering lesson

`SELECT ... FOR UPDATE SKIP LOCKED` controls who claims a row next. It does not control what a stale claimant does with a row it already holds. These are different problems requiring different mechanisms. The fencing token closes the second gap. Understanding the precise failure sequence — claim → delay → reaper reset → new claim → original claim `markSent()` — requires tracing the execution sequence of two concurrent processes during a network partition, not reading the implementation in isolation. This failure mode was only discovered during reliability test design.

---

## Phase 10 — Reliability Test Suite, Documentation, and Observability Hardening

**Implemented:** 12 reliability scenarios testing specific named failure modes. Full architecture documentation (this repository). 6 ADRs. 5 runbooks. 5 incident postmortems. 5 load test scenarios with capacity planning. 4 Grafana dashboards. Prometheus alert rules. Benchmark report templates.

### What this phase addressed

Two gaps that the previous nine phases left open:

**Gap 1: Reliability mechanisms were implemented but not verified under failure.** Unit tests verified that the code was correct. Integration tests verified that the happy path worked. Neither tested whether the fencing token actually prevented the double-publish under a real concurrent relay race, or whether the idempotency mechanism caught a duplicate delivery under actual concurrent load. Reliability tests that intentionally induce failure conditions closed this gap.

**Gap 2: The system was understandable to its author but not to anyone else.** The architecture existed but was not documented. The decisions were made but not recorded. The failure modes were known but not written down. For a portfolio project, this is the difference between "I built this" and "I can explain why every decision was made, what it cost, and how it would fail."

### Tradeoffs introduced

**Documentation maintenance burden.** Every architectural change now requires updating the relevant ADR, runbook, or postmortem. This is the correct tradeoff — undocumented architecture decisions accumulate as invisible debt — but it is a real cost.

**Reliability tests require a live stack.** The 12 reliability scenarios run against a real RabbitMQ, PostgreSQL, and two NestJS services. They cannot run against mocks. This means the CI pipeline must provision infrastructure, which adds 3–5 minutes to the build.

### Engineering lesson

A reliability mechanism that is implemented but not tested under the failure condition it is designed to handle is providing a false sense of security. The fencing token could have been commented out and the unit tests would still pass. Only a test that runs two concurrent relay instances against a real database and verifies that only one `markSent()` succeeds can validate the mechanism end-to-end.

Documentation is not the last step of engineering. It is the step that turns implementation into knowledge that can be reviewed, critiqued, improved, and transferred.

---

## Overall Engineering Journey

This repository started as a two-service messaging spike written in a weekend. It now documents 10 distinct phases of architectural evolution, each motivated by a specific failure mode discovered in the previous phase.

The trajectory is not linear improvement. Phase 2 (manual ACK) fixed Phase 1's reliability gap but introduced infinite retry loops. Phase 3 (DLX topology) fixed the retry loops but introduced a silent topology misconfiguration risk. Phase 4 (transactional outbox) fixed the dual-write problem but introduced table bloat, which caused PM-002 fourteen days later. Phase 5 (publisher confirms) closed the most serious silent data loss path, but made the relay the system's first throughput bottleneck.

Each phase introduced a tradeoff. Several introduced bugs that were only discovered later. The honest observation is that distributed systems reliability is cumulative: each mechanism closes a specific gap, but the system is only as reliable as the weakest unaddressed gap.

**What the repository demonstrates that a greenfield implementation cannot:**

The postmortems document real failure modes in real incident sequences. PM-003 happened because a data analyst ran a GROUP BY on the production primary database without knowing it would exhaust the consumer's connection pool. PM-004 happened because a NestJS lifecycle hook synchronously loaded a 2.1MB fixture file from a cold EBS volume. These are not invented failure scenarios. They are the kinds of failures that accumulate when a system is operated.

The ADRs document decisions that were made with imperfect information and explain what was known at the time. ADR-001 documents why RabbitMQ was chosen over Kafka — not because Kafka is wrong, but because Kafka solves different problems, and the tradeoffs are real and specific.

The load tests are honest about what they measure. The k6 scenarios report relay throughput at approximately 250 msg/s per instance — not a theoretical ceiling, but the measured value given the current batch size, confirm latency, and Node.js event loop sharing. The capacity model explains the calculation, not just the number.

**What remains unresolved:**

The relay runs in-process with the application service, sharing the Node.js event loop. Under heavy application load, relay poll delays are a leading indicator of application saturation. Moving the relay to a separate process is the highest-value optimization that has not been done.

The `processed_events` and `gateway_outbox_events` tables have purge jobs documented in ADR-003 and PM-002, but the implementation was added as a follow-up task. The gap between "documented" and "implemented" is where the next PM-00N will come from.

The retry budget (62 seconds, 5 attempts) was calibrated for transient failures, not sustained infrastructure outages. PM-003 demonstrated that a sustained database outage of 8 minutes exceeds the budget. The circuit-breaker follow-up task from PM-003 is in the backlog.

These are not oversights that will be resolved by extending the documentation. They are genuine tradeoffs between implementation complexity and operational risk — the kind of decisions that engineers make with real constraints and incomplete information. Documenting them honestly is more useful than claiming they do not exist.
