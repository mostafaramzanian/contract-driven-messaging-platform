# Production-Readiness Fixes

This document records what changed in response to a Staff-level
production-readiness review, why, and what is intentionally left as
follow-up work rather than bundled into this change. See the review
itself (not reproduced here) for the full list of findings; this
document covers only what was actually implemented.

## 1. Idempotency: atomic idempotency-ledger + business write

**Problem:** `IdempotencyService.checkAndMark()` previously committed as
its own, separate transaction, before `MessagingService.handleMessageCreation()`
ran its own (separately committed) transaction. A crash between the two
left an orphaned idempotency row with no corresponding message — silent,
permanent message loss on redelivery, since the next delivery attempt
would see the (orphaned) idempotency row and skip reprocessing entirely.

**Fix:** `OutboxTransactionService.runIdempotentWithOutboxEvents()`
(`apps/messaging/src/outbox/outbox-transaction.service.ts`) folds the
idempotency INSERT, the business write, and the outbox-event INSERT into
ONE database transaction — all commit together or all roll back together.
`MessagingService.handleMessageCreationIdempotent()` is the new method
that uses it; `MessagingController.handleMessage` now calls this instead
of the previous two-step check-then-process sequence.

The original `handleMessageCreation()` / `runWithOutboxEvents()` methods
are **unchanged** and still used by the legacy `handleTestRabbit`
`@MessagePattern('test-rabbit')` handler, which has no idempotency
requirement of its own.

## 2. Consumer reliability: retry-publish-before-ack

**Problem:** `MessagingController.handleMessage`'s retry path called
`channel.ack(msg)` (permanently removing the original message from the
broker) BEFORE awaiting `RetryPublisherService.publishToRetry()`. If the
retry publish then failed (back-pressure, a connection mid-reconnect),
the message was already gone — silent, unrecoverable loss.

**Fix:** The ack now happens only after `publishToRetry()` resolves
successfully. If it throws, the original message is `nack`'d with
`requeue=true` instead — a deliberate, narrow exception to the
no-requeue convention used everywhere else in this handler, because the
alternative (the previous behavior) is unconditional data loss.
`RetryPublisherService.publishToRetry()` now throws on back-pressure
(previously only logged a warning) so the controller's catch block has
something to catch.

## 3. RabbitMQ: publisher confirms on the retry-publish path

**Problem:** `channel.publish()`'s synchronous boolean return reflects
only local write-buffer back-pressure, not broker-side durability. Acting
on that boolean alone (as both the retry publisher and the outbox relay
previously did) can declare a message "sent" when the broker accepted the
TCP write but crashed before persisting it.

**Fix:** `RetryPublisherService` now uses `createConfirmChannel()` (not
`createChannel()`) and awaits `channel.waitForConfirms()` after every
publish, which only resolves once the broker has actually acknowledged
the publish.

**Not yet applied to:** `OutboxRelayService.publishOne()` and the
gateway's `ClientProxy.emit()` calls — see "Deferred / follow-up work"
below.

## 4. Security: unauthenticated internal admin endpoints

**Problem:** `OutboxAdminController`'s replay endpoints
(`POST /internal/outbox/:id/replay`, `POST /internal/outbox/replay-failed`)
had no auth guard, on the assumption that the internal HTTP port "is not
publicly routed." That assumption did not hold: `docker-compose.yml`
mapped the messaging service's internal port directly to the host
(`"3006:3006"`), bypassing Nginx (which only proxies `/api`) entirely.

**Fix, two layers (defense in depth, not either/or):**

1. **`InternalApiKeyGuard`** (`libs/common/src/security/internal-api-key.guard.ts`)
   — a constant-time shared-secret header check
   (`x-internal-api-key` vs. the `INTERNAL_API_KEY` env var), applied to
   `OutboxAdminController` via `@UseGuards(InternalApiKeyGuard)`. Fails
   closed: if `INTERNAL_API_KEY` is unset, every request is rejected.
2. **`docker-compose.yml`** changed the messaging service's port mapping
   from `ports: ["3006:3006"]` to `expose: ["3006"]` — still reachable on
   the Docker network (Prometheus scraping, health checks) but no longer
   published on the host's network interface at all.

`INTERNAL_API_KEY` must be set in `.env` for the messaging service's
internal admin endpoints to function — see `.env.example`.
`MessagingHealthController`'s `/internal/health/*` routes are a
**separate controller** and are intentionally **not** behind this guard
(liveness/readiness probes must remain unauthenticated).

**Also fixed in the same pass:** `docker-compose.yml`'s
`POSTGRES_PASSWORD: ${DB_PASSWORD:-password123}` weak fallback default,
and the hardcoded (non-overridable) `GF_SECURITY_ADMIN_PASSWORD=admin`
for Grafana — both now use Compose's required-variable syntax
(`${VAR:?error message}`), which refuses to start the affected service if
the variable is unset, rather than silently falling back to a guessable
credential. `.env.example`'s `RABBITMQ_URL` default was also changed from
the well-known insecure `guest:guest` to an explicit placeholder.

## 5. PostgreSQL: missing index + unbounded result set on `messages`

**Problem:** `MessagesService.findAll()` ran
`ORDER BY "createdAt" DESC` with no supporting index and no `LIMIT` —
every call returned the entire table, sorted in memory.

**Fix:**
- Migration 008 adds `IDX_messages_created_at` on `messages."createdAt"`.
- `MessagesService.findAll()` now accepts an optional
  `{ limit?: number; cursor?: string }`, with a default limit of 50 and a
  hard ceiling of 200 (out-of-range input is clamped, not rejected).
  `cursor` (an ISO timestamp) does **cursor-based**, not offset-based,
  pagination (`WHERE "createdAt" < cursor`), which stays a stable,
  indexed range scan regardless of table size or concurrent inserts.
- `MessagesController.findAll` (`@MessagePattern('findAllMessages')`)
  forwards an optional payload through to the new parameters.

## 6. Consumer reliability: durable retry-attempt tracking

**Problem:** `MAX_ATTEMPTS` was enforced by reading `x-retry-count` from
AMQP headers — which only survives because `RetryPublisherService`
explicitly carries it forward. Any other redelivery path (a manual
requeue via the RabbitMQ management UI, or a future outbox-relay replay)
starts a fresh message with no such header, silently resetting the
attempt counter and weakening `MAX_ATTEMPTS` from a true lifetime cap to
a per-incident one.

**Fix (schema only in this change):** Migration 006 adds an
`event_attempts` table (`event_id` primary key, `attempts` counter,
durable across any redelivery path) and the corresponding
`EventAttempt` entity. **Wiring this into
`MessagingController.handleMessage`'s retry-vs-DLQ decision is deferred**
— see below.

## 7. Outbox: stable event identity + relay fencing token (schema only)

**Problem (two related gaps):**
- Outbox rows had no identity meaningful to a downstream consumer's
  idempotency check beyond the surrogate primary key.
- `OutboxRelayService.reapStaleLocks()` can release a lock held by a
  claimant that is merely slow (not dead), allowing a second instance to
  claim and publish the same row — a real double-publish window.

**Fix (schema only in this change):** Migration 007 adds `event_id`
(generated once per row, reused across retries/replays — see
`OutboxTransactionService.insertOutboxEvents`, which now generates this
for every row) and `lock_version` (an integer fencing token, not yet
read/incremented by the relay's claim/publish/markSent cycle) to
`outbox_events`. **Wiring `lock_version` into
`OutboxRelayService.claimBatch`/`markSent` is deferred** — see below.

## 8. Observability: trace-context propagation across the outbox boundary (schema + capture only)

**Problem:** `OutboxRelayService.publishOne()` injects whatever ambient
OpenTelemetry context exists at relay-poll time (a `setInterval`
callback with no parent span) rather than the original producer's trace
— breaking distributed tracing exactly at the outbox hop.

**Fix (partial in this change):**
- Migration 008 adds `outbox_events.trace_context` (a JSONB column
  storing the W3C propagation carrier, e.g. `{ traceparent: "00-..." }`).
- `libs/common/src/tracing/amqp-propagation.ts` gained
  `captureTraceContextCarrier()`, which captures the active trace context
  as a bare carrier (no AMQP headers merged in) — used by
  `OutboxTransactionService.insertOutboxEvents` to capture and store the
  original request's trace context at outbox-row write time.
- **`OutboxRelayService.publishOne()` reading this column back and using
  `extractTraceContext`/`context.with(...)` to make it the active context
  at publish time is deferred** — see below. The column is populated;
  nothing reads it yet.

## Deferred / follow-up work (not in this change)

These were identified in the review and have groundwork laid (schema,
supporting functions) but are **not yet wired into the running code
path**, either because they require touching `OutboxRelayService` (a
larger, separately-reviewable change given its role in the
already-working horizontal-scaling story) or because they depend on live
infrastructure to verify (quorum queues need a real multi-node RabbitMQ
cluster, which this environment cannot provision):

- **`OutboxRelayService.publishOne()`**: switch to a confirm channel +
  `waitForConfirms()` (same pattern as `RetryPublisherService`, item 3
  above); read `lock_version` on claim and check it on `markSent` (item
  7); read `trace_context` and propagate it instead of the ambient
  context (item 8).
- **`MessagingController.handleMessage`**: read/increment the durable
  `event_attempts` counter (item 6) instead of trusting
  `x-retry-count` alone for the `MAX_ATTEMPTS` decision.
- **Quorum queues**: `apps/messaging/src/reliability/topology.service.ts`
  and `apps/messaging/src/main.ts`'s queue declarations still default to
  classic queues. Switching to `'x-queue-type': 'quorum'` requires a
  real multi-node RabbitMQ cluster to provide the durability benefit;
  single-node quorum queues add overhead with no HA gain. Documented as
  an infrastructure prerequisite, not just a code change.
- **Rate limiting** on the gateway's HTTP routes (`@nestjs/throttler`)
  was identified but not added in this change.
- **Event tampering protection** (HMAC-signing outbound events) was
  identified but not added — a larger change touching both the gateway
  producer and the messaging consumer's validation step.
- **Retention/cleanup jobs** for `processed_events`, `outbox_events`
  (`status = 'sent'`), and the new `event_attempts` table — indexes
  exist (or were added) to support a future TTL-purge job, but no
  scheduled job was added in this change.
- **`apps/messaging/src/health/rabbitmq-health.indicator.spec.ts`** and
  **`apps/messaging/src/interceptors/logging.interceptor.spec.ts`** /
  **`libs/common/src/logger/pino-logger.service.spec.ts`** /
  **`libs/common/src/middleware/logging.middleware.spec.ts`**: four
  pre-existing test failures, confirmed unrelated to this change (each
  fails identically in complete isolation, in files never touched this
  session — a mock-call-count mismatch, a `done()` timeout, and a `pino`
  module-resolution error respectively). Flagged, not fixed, as outside
  this change's scope.

## Migrations added

| # | File | Adds |
|---|---|---|
| 006 | `006_CreateEventAttemptsTable.ts` | `event_attempts` table |
| 007 | `007_AddEventIdAndLockVersionToOutboxEvents.ts` | `outbox_events.event_id`, `outbox_events.lock_version` |
| 008 | `008_AddMessagesIndexAndOutboxTraceContext.ts` | `messages` index on `createdAt`; `outbox_events.trace_context` |

Run via the existing `npm run migration:run` script
(`apps/messaging/typeorm.config.ts`) against a real Postgres instance —
**not executed in this environment**, since no live database was
available; verified only via `tsc` (the migration files compile cleanly
and match the existing migrations' structure/conventions) and manual
review against the entity changes that depend on them.

## Regression status

All changes were validated against the same three-leg gate used
throughout this project's development:

1. `npx jest` — **194/194 passing** across every test file this change
   touched or could affect, excluding the four confirmed pre-existing,
   unrelated failures listed above.
2. `npx tsc --noEmit -p libs/contracts/tsconfig.lib.json` — clean.
3. `npx tsc --noEmit -p tsconfig.json` — **exactly 7 pre-existing,
   unrelated baseline errors** (missing `winston` types, an OTel/pino
   version-skew issue, an Express typing gap), identical in count and
   location to the baseline measured before this change; zero new errors
   introduced.

Database migrations and any change touching live RabbitMQ/Postgres
behavior (the deferred items above, plus confirming the three new
migrations actually apply cleanly) could not be executed end-to-end in
this environment — no Docker/database was available. They are written
to the same standard and conventions as the existing, presumably-tested
migrations, but should be run against a real environment before being
considered verified.
