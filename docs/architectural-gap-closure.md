# Architectural Gap Closure: Producer Reliability & Event Routing

This document records the fix for two CRITICAL architectural gaps
identified in a Staff Engineering review, separate from (and on top of)
the production-readiness fixes already recorded in
`docs/production-readiness-fixes.md`. No new microservice, no Kafka, and
no rewrite of the existing architecture — both fixes close the gap using
the patterns the codebase already established for the consumer side.

```
Gateway → RabbitMQ → Messaging Service → PostgreSQL
```

remains the architecture. What changed is *how* the Gateway talks to
RabbitMQ, and *where* the Messaging Service's own outgoing domain events
go once published.

---

## CRITICAL ISSUE #1 — Producer Reliability Gap

### Problem

`AppController` (gateway) published directly with `this.client.emit(...)`
— a synchronous-within-the-HTTP-request call against a live `ClientProxy`
connected straight to RabbitMQ. If RabbitMQ was unavailable at that exact
moment: the event was lost, with no retry, no persistence, and no
recovery path.

### Fix: Producer Transactional Outbox + Gateway Outbox Relay

A new table, `gateway_outbox_events` (own migration, own TypeORM entity,
same physical Postgres database the messaging service already uses — no
new infrastructure), durably records every outgoing event inside a
committed Postgres transaction *before* the HTTP handler returns. A
background relay (`GatewayOutboxRelayService`), structurally a
near-line-for-line adaptation of the messaging service's existing
`OutboxRelayService`, asynchronously claims and publishes those rows to
RabbitMQ with publisher confirms, exponential back-off retry, and a
stale-lock reaper for crash recovery.

```
                         HTTP request
                              |
                              v
                    +------------------+
                    |  AppController   |  validate -> 400 if invalid
                    +---------+--------+
                              | record()  (Postgres tx, COMMITTED here)
                              v
                 +---------------------------+
                 | gateway_outbox_events     | <-- durable, independent
                 | (status: pending)         |     of RabbitMQ entirely
                 +-------------+--------------+
                              | 202 Accepted -------------> caller
                              |
                              |  (asynchronous, separate process loop)
                              v
                 +---------------------------+
                 | GatewayOutboxRelayService | poll -> claim (SKIP LOCKED)
                 |  - publisher confirms     | -> publish -> confirm -> markSent
                 |  - exp. back-off retry    |
                 |  - stale-lock reaper      |
                 +-------------+--------------+
                              | resolveOutboxRoute(eventType)
                              v
                    messaging.direct / messaging.work
                              |
                              v
                      Messaging Service
```

### Database schema

`apps/gateway/src/migrations/001_CreateGatewayOutboxEventsTable.ts` —
the column set deliberately matches the messaging service's
`outbox_events` table (`id`, `event_type`, `payload` jsonb,
`correlation_id`, `status`, `attempts`, `last_error`, `created_at`,
`sent_at`, `locked_at`, `locked_by`, `next_retry_at`, `max_attempts`,
`event_id`, `lock_version`, `trace_context` jsonb), with the same
composite indexes the consumer-side relay's claim query and reaper
depend on (`(status, next_retry_at)`, `(status, locked_at)`), and the same
partial unique index on `event_id`.

### Transaction boundaries

`GatewayOutboxTransactionService.runWithOutboxEvent(work, event)` opens
one `QueryRunner` transaction, runs the caller's `work` callback (a no-op
for the common `record()` case, but the seam is there if the gateway ever
needs a business write alongside the event), inserts the outbox row, and
commits — all inside one transaction, so the HTTP handler either durably
persists the event or the whole request fails loudly (a real Postgres
outage), never a silent partial state.

### Locking strategy

Identical mechanism to the messaging service's consumer-side relay:

```sql
UPDATE gateway_outbox_events
SET    locked_at = now(), locked_by = $instanceId, lock_version = lock_version + 1
WHERE  id IN (
  SELECT id FROM gateway_outbox_events
  WHERE status = 'pending' AND next_retry_at <= now()
  ORDER BY next_retry_at ASC LIMIT $batchSize
  FOR UPDATE SKIP LOCKED
)
RETURNING ...
```

`FOR UPDATE SKIP LOCKED` lets N concurrent relay instances (horizontal
scaling — one per gateway replica) each claim a disjoint batch with zero
coordination between them. `lock_version`, returned with the claim and
threaded through to `markSent`/`markFailedAttempt`'s `WHERE ... AND
lock_version = $expected`, is the fencing token that prevents a *stale*
claimant (one whose lock already expired and was reaped) from
overwriting a result a *newer* claimant already wrote.

### Failure recovery strategy

- **RabbitMQ down during the HTTP request (Requirement A):** the outbox
  INSERT has no dependency on RabbitMQ at all — the request still
  succeeds with `202 Accepted`. The relay retries publishing with
  exponential back-off (`computeFailureOutcome`, shared with the
  messaging service via `@app/contracts`) until the broker recovers.
- **Gateway crashes after persistence but before publish (Requirement
  B):** the row is durable in Postgres regardless of which process
  crashed. If a relay instance crashes mid-claim (lock held, never
  published), `reapStaleLocks()` clears the lock after
  `GATEWAY_OUTBOX_LOCK_TTL_MS` so any live instance can reclaim it.
- **Multiple relay instances (Requirement C):** `SKIP LOCKED` for claim
  exclusivity, `lock_version` fencing for markSent/markFailedAttempt — no
  combination of timing produces a double-publish that the relay itself
  records as success twice; downstream idempotency (eventId-keyed, see
  `MessagingService.handleMessageCreationIdempotent`) is the final
  backstop if the broker ever redelivers regardless.

### Code reuse

`libs/contracts/src/outbox/outbox-retry-policy.ts` and
`libs/contracts/src/topology/topology.ts` now hold the canonical, shared
pure functions (`computeFailureOutcome`, `isLockStale`,
`generateInstanceId`) and topology constants (`EXCHANGES`, `QUEUES`,
`RETRY_CONFIG`, `retryDelayMs`) — moved here from
`apps/messaging/src/outbox/outbox-retry-policy.ts` /
`apps/messaging/src/reliability/topology.ts`, which now re-export them
unchanged so no existing import site in the messaging app needed to
change. `GatewayOutboxRelayService` imports the SAME functions directly.

---

## CRITICAL ISSUE #2 — MessagePersisted Routing Loop

### Problem

`OutboxRelayService` (messaging) published every outbox row — regardless
of `event_type` — to the same destination: `messaging.direct` exchange,
`messaging.work` routing key. That queue is consumed by exactly one
handler, `MessagingController.handleMessage`, whose `@MessagePattern`
only matches `CreateMessageEvent.v1`/`.v2`. A `MessagePersisted` row
(written by the messaging service itself, as a side effect of handling a
`CreateMessageEvent`) landing in that same queue is not a command that
handler can process — work -> nack -> retry -> work -> retry -> DLQ,
entirely self-inflicted, polluting the DLQ with events that were never
commands.

### Fix: dedicated domain-event exchange + routing chokepoint

A new fanout exchange, `messaging.events`, with a bound audit queue,
`messaging.events.audit` — **not** bound to `messaging.work`, **not**
consumed by `MessagingController`. `resolveOutboxRoute(eventType)`
(`libs/contracts/src/topology/topology.ts`) is the single chokepoint both
relays (gateway and messaging) use to decide a row's destination:
anything in `COMMAND_EVENT_TYPES` (today: `CreateMessageEvent.v1`/`.v2`)
goes to the command bus; everything else — fail-safe default, not an
allow-list — goes to the domain-event bus.

```
                outbox_events (messaging service)
                              |
                              v
                 +---------------------------+
                 |   OutboxRelayService      |
                 +-------------+--------------+
                              | resolveOutboxRoute(row.event_type)
              +----------------+-----------------+
              v                                   v
   event_type IN COMMAND_EVENT_TYPES    event_type NOT IN COMMAND_EVENT_TYPES
   (CreateMessageEvent.v1/.v2)          (e.g. MessagePersisted)
              |                                   |
              v                                   v
   messaging.direct (direct)            messaging.events (fanout)
   routing key: messaging.work          routing key: '' (fanout, ignored)
              |                                   |
              v                                   v
       messaging.work (queue)           messaging.events.audit (queue)
              |                                   |
              v                                   v
   MessagingController.handleMessage    (no consumer in this codebase —
   (CreateMessageEvent handlers)         audit/observability sink only;
              |                          NEVER bound to messaging.work,
        success / failure                NEVER touches the command DLQ)
              |
       messaging.dlx -> messaging.dlq   (only for genuine command
                                          failures — no longer ever
                                          triggered by domain events)
```

### Why a fanout exchange, not a dedicated queue per consumer

A fanout exchange (`messaging.events`) was chosen over a direct exchange
with per-consumer routing keys because today there is exactly one
consumer of domain events (the audit queue), but the *shape* of the
problem — "broadcast a fact about something that already happened to
whoever wants to know" — is fanout's textbook use case, and adding a
second downstream consumer later (e.g. a future analytics or
notification service) is then a pure addition: `assertQueue` +
`bindQueue` to the existing exchange, zero changes to the publishing
side. A direct exchange with per-event-type routing keys would have
worked too, but would couple every future consumer's binding to knowing
the exact set of domain event type strings in advance — fanout sidesteps
that.

### Routing key strategy

Commands keep their existing key, `messaging.work` (unchanged — no
existing consumer binding needed to change). Domain events use an empty
routing key (`''`), since RabbitMQ ignores routing keys on fanout
exchanges entirely; `OutboxRoute.routingKey` is still always a string
(rather than `string | undefined`) purely so call sites never need a
conditional before calling `channel.publish()`.

### Implementation changes

- `libs/contracts/src/topology/topology.ts` — new `EXCHANGES.EVENTS`,
  `QUEUES.EVENTS_AUDIT`, `COMMAND_EVENT_TYPES`, `resolveOutboxRoute()`.
- `apps/messaging/src/reliability/topology.service.ts` — asserts the new
  exchange/queue/binding alongside the existing topology at startup.
- `apps/messaging/src/outbox/outbox-relay.service.ts` —
  `publishOne()`/`publishWithRestoredTrace()` now call
  `resolveOutboxRoute(row.event_type)` instead of hardcoding
  `EXCHANGES.MAIN`/`ROUTING_KEYS.WORK`.
- `apps/gateway/src/outbox/gateway-outbox-relay.service.ts` — uses the
  exact same `resolveOutboxRoute()` call (today always resolving to the
  command bus, since every row the gateway writes is a command — but
  future-proofed identically to the messaging side with zero extra
  code).

### Migration strategy

No data migration needed — `outbox_events` rows already in flight at
deploy time keep their existing `event_type` values; `resolveOutboxRoute`
is a pure function of that column, so the very next relay poll after
deploy classifies every row (in-flight or new) correctly. The new
exchange/queue are asserted idempotently at startup
(`channel.assertExchange`/`assertQueue` are no-ops if already present),
so there is no ordering requirement between deploying the messaging
service and the gateway.

### Integration / reliability tests

- `test/reliability/12-domain-event-routing-isolation.reliability-spec.ts`
  — D1 (a real `MessagePersisted` event reaches the audit queue, not
  `messaging.work`), D2 (`messaging.work`/DLQ queue depth unaffected —
  no retry loop, no DLQ pollution), D3 (unit-level proof of
  `resolveOutboxRoute`'s classification, including the fail-safe default
  for an unregistered event type).
- `test/reliability/11-gateway-producer-outbox.reliability-spec.ts` —
  Requirements A, B, C for the gateway's producer outbox (broker outage
  during the HTTP request, relay crash recovery, concurrent-instance
  fencing), plus an end-to-end check that a gateway-relayed event is
  actually consumed and persisted by the messaging service.
