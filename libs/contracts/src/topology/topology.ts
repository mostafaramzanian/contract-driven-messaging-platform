/**
 * Shared RabbitMQ Topology + Outbox Routing Contract
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * Single source of truth for exchange/queue/routing-key names and the
 * outbox→broker routing decision, shared by BOTH the gateway's producer
 * outbox relay and the messaging service's consumer-side outbox relay.
 *
 * Previously this lived only inside `apps/messaging/src/reliability/topology.ts`
 * and was never importable from the gateway app (no NestJS app imports across
 * `apps/*` — only `libs/*` is shared). That file now re-exports everything
 * from here unchanged, so no call site in the messaging app needed to change
 * its import path.
 *
 * ┌──────────────────────────────────────────────────────────────────────┐
 * │                      EXCHANGE TOPOLOGY                               │
 * │                                                                      │
 * │  messaging.direct (direct)            — COMMAND bus                  │
 * │    └─ routing-key: messaging.work  ──► messaging.work (queue)        │
 * │    └─ routing-key: messaging.retry ──► messaging.retry.q (queue)     │
 * │                                              │ TTL expires           │
 * │  messaging.dlx (fanout, dead-letter exch.)   ▼                      │
 * │    └─ ──────────────────────────────► messaging.work (requeued)     │
 * │                                                                      │
 * │  messaging.dlq.exchange (direct)                                     │
 * │    └─ routing-key: messaging.dead ──► messaging.dlq (queue)         │
 * │                                                                      │
 * │  messaging.events (fanout)            — DOMAIN EVENT bus (NEW)       │
 * │    └─ ──────────────────────────────► messaging.events.audit (queue) │
 * │       Carries events ABOUT something that already happened           │
 * │       (e.g. MessagePersisted). NEVER bound to messaging.work, NEVER  │
 * │       consumed by the CreateMessageEvent handler — see               │
 * │       `resolveOutboxRoute` below for why this separation exists.     │
 * │                                                                      │
 * │  event-lifecycle (fanout) [existing, unchanged]                      │
 * └──────────────────────────────────────────────────────────────────────┘
 *
 * ## Why a separate domain-event exchange (Architectural Gap #2)
 *
 * `OutboxRelayService` (both gateway and messaging) is a generic poller: it
 * does not know or care *what* an outbox row's payload means, only that it
 * needs to reach the broker. Before this fix, every relay published every
 * row to the SAME destination (`messaging.direct` / `messaging.work`) —
 * regardless of whether the row was a COMMAND ("please create this
 * message", produced by the gateway) or a DOMAIN EVENT ("this message was
 * just persisted", produced by the messaging service as a side-effect of
 * handling a command).
 *
 * `messaging.work` is consumed by exactly one handler:
 * `MessagingController.handleMessage`, registered against
 * `CreateMessageEvent.v1`/`.v2`. A `MessagePersisted` row landing in that
 * same queue is not a `CreateMessageEvent` of either version — it fails
 * contract validation (Step 2 of `handleMessage`), or, depending on the
 * NestJS RMQ deserializer, never finds a matching `@MessagePattern` at all.
 * Either way the broker sees a delivery that is not cleanly acked, and the
 * message is requeued/retried by the SAME queue's DLX wiring that exists
 * for genuine command failures — `work → nack → retry → work → retry →
 * DLQ`, entirely self-inflicted, polluting the DLQ with events that were
 * never supposed to be commands in the first place.
 *
 * `resolveOutboxRoute()` below is the fix: it classifies an outbox row's
 * `eventType` as either a COMMAND (anything in `COMMAND_EVENT_TYPES` —
 * today, `CreateMessageEvent.v1`/`.v2`) or a DOMAIN EVENT (everything else),
 * and returns the correct exchange/routing-key pair. Commands keep going to
 * `messaging.direct` / `messaging.work`, exactly as before. Domain events go
 * to the new `messaging.events` fanout exchange, which has no binding to
 * `messaging.work` and is never read by `MessagingController` — there is no
 * code path by which a domain event can re-enter the command queue.
 *
 * Message headers used for retry tracking:
 *   x-retry-count    : number  (incremented on each retry hop)
 *   x-first-error    : string  (original error message, set once)
 *   x-error-class    : string  (VALIDATION | TRANSIENT | PERMANENT)
 *   x-failed-at      : string  (ISO timestamp of first failure)
 */

export const EXCHANGES = {
  /** Primary direct exchange — routes COMMANDS to the work queue and retry queue */
  MAIN: 'messaging.direct',
  /** Dead-letter exchange attached to messaging.work — catches nack/expired */
  DLX: 'messaging.dlx',
  /** Exchange that dead-letter routes terminate at (DLQ consumer reads this) */
  DLQ: 'messaging.dlq.exchange',
  /**
   * Domain-event bus (fanout). Carries facts about something that already
   * happened (e.g. `MessagePersisted`) for any interested downstream
   * consumer/audit trail. NEVER bound to `messaging.work`. This is the
   * destination `resolveOutboxRoute()` returns for any `eventType` that is
   * not in `COMMAND_EVENT_TYPES` — see this module's top-level doc comment.
   */
  EVENTS: 'messaging.events',
} as const;

export const QUEUES = {
  /** Main work queue — where commands are consumed (CreateMessageEvent handlers) */
  WORK: 'messaging.work',
  /** Retry delay queue — messages sit here for TTL then re-enter WORK */
  RETRY: 'messaging.retry.q',
  /** Dead-letter queue — poison messages land here for inspection */
  DLQ: 'messaging.dlq',
  /**
   * Audit queue bound to the domain-event fanout exchange. Not consumed by
   * any command handler — purely an observability/audit sink so domain
   * events are inspectable without being able to pollute the command DLQ.
   * Deliberately has NO dead-letter-exchange argument: a poison domain
   * event can fail here without ever generating DLQ traffic on the command
   * side, by construction (different exchange, different queue, no shared
   * dead-lettering path).
   */
  EVENTS_AUDIT: 'messaging.events.audit',
} as const;

export const ROUTING_KEYS = {
  WORK: 'messaging.work',
  RETRY: 'messaging.retry',
  DEAD: 'messaging.dead',
} as const;

export const RETRY_CONFIG = {
  /** Maximum number of delivery attempts before routing to DLQ */
  MAX_ATTEMPTS: 5,
  /**
   * Base delay in ms for exponential back-off.
   * attempt 1 → 2s, 2 → 4s, 3 → 8s, 4 → 16s, 5 → DLQ
   */
  BASE_DELAY_MS: 2_000,
  MAX_DELAY_MS: 30_000,
} as const;

/** Compute per-attempt TTL (capped at MAX_DELAY_MS). */
export function retryDelayMs(attempt: number): number {
  return Math.min(
    RETRY_CONFIG.BASE_DELAY_MS * Math.pow(2, attempt - 1),
    RETRY_CONFIG.MAX_DELAY_MS,
  );
}

// ── Command vs domain-event classification (Architectural Gap #2) ─────────

/**
 * The full set of `eventType` strings that represent COMMANDS — events
 * that instruct the messaging service to do something, consumed by
 * `MessagingController.handleMessage`'s `@MessagePattern`.
 *
 * Deliberately a `Set`, not an array: every relay's hot path calls
 * `resolveOutboxRoute()` once per published row, and this needs O(1)
 * membership testing, not O(n) `Array.includes`.
 *
 * Add a new wire-level command pattern (e.g. a future `CreateMessageEvent.v3`
 * or an entirely new command type) here — and ONLY here — to route it to
 * the command bus. Anything NOT listed here is treated as a domain event by
 * default (fail-safe: an unrecognized type can never accidentally re-enter
 * the command queue and create a retry loop).
 */
export const COMMAND_EVENT_TYPES: ReadonlySet<string> = new Set([
  'CreateMessageEvent.v1',
  'CreateMessageEvent.v2',
]);

export interface OutboxRoute {
  exchange: string;
  /**
   * Routing key to publish with. Empty string for fanout exchanges
   * (`EXCHANGES.EVENTS`), where RabbitMQ ignores the routing key entirely —
   * present mainly so callers always have a string to pass to
   * `channel.publish()` without a conditional at the call site.
   */
  routingKey: string;
  /** `'command'` → command bus (messaging.work); `'domain-event'` → event bus. */
  kind: 'command' | 'domain-event';
}

/**
 * Decide where an outbox row should be published, based on its `eventType`.
 *
 * This is the single chokepoint that closes Architectural Gap #2. Both
 * `OutboxRelayService` (messaging, consumer-side outbox) and
 * `GatewayOutboxRelayService` (gateway, producer-side outbox) call this
 * instead of hardcoding `EXCHANGES.MAIN` / `ROUTING_KEYS.WORK` — so a
 * domain event written to either outbox table can never be routed onto the
 * command queue, regardless of which service produced it.
 *
 * @param eventType  The outbox row's `event_type` column value.
 */
export function resolveOutboxRoute(eventType: string): OutboxRoute {
  if (COMMAND_EVENT_TYPES.has(eventType)) {
    return {
      exchange: EXCHANGES.MAIN,
      routingKey: ROUTING_KEYS.WORK,
      kind: 'command',
    };
  }
  return { exchange: EXCHANGES.EVENTS, routingKey: '', kind: 'domain-event' };
}
