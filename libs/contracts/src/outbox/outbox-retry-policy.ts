import { hostname } from 'os';
import { randomBytes } from 'crypto';
import { retryDelayMs, RETRY_CONFIG } from '../topology/topology';

/**
 * Shared OutboxRetryPolicy — pure functions for outbox failure-handling
 * logic, used identically by `OutboxRelayService` (messaging, consumer-side
 * outbox) and `GatewayOutboxRelayService` (gateway, producer-side outbox).
 *
 * Moved here (from `apps/messaging/src/outbox/outbox-retry-policy.ts`) so
 * the gateway's producer outbox can reuse the EXACT SAME back-off curve and
 * instance-id generation, rather than re-implementing or subtly diverging
 * from it — see CRITICAL ISSUE #1's requirement to "reuse existing outbox
 * patterns where possible". `apps/messaging/src/outbox/outbox-retry-policy.ts`
 * now re-exports everything from here unchanged, so no messaging-app call
 * site needed to change its import path.
 *
 * All functions are side-effect free and accept an optional `now` Date so
 * tests can inject a deterministic clock without patching globals.
 */

// ── Types ──────────────────────────────────────────────────────────────────

export type FailureOutcome =
  | { action: 'retry'; nextRetryAt: Date; newAttempts: number }
  | { action: 'dead-letter'; newAttempts: number };

// ── computeFailureOutcome ─────────────────────────────────────────────────

/**
 * Decide what to do after a single publish failure.
 *
 * @param attemptsBefore  Number of attempts already recorded BEFORE this failure.
 * @param maxAttempts     The per-row retry budget (from outbox_events.max_attempts).
 * @param now             Reference time for computing next_retry_at.  Defaults to new Date().
 *
 * @returns FailureOutcome — either `retry` (with the next eligible time and
 *          incremented attempt count) or `dead-letter` (budget exhausted).
 *
 * The delay schedule reuses `retryDelayMs` from topology.ts so the outbox
 * back-off curve matches the RabbitMQ retry curve:
 *   attempt 1 → 2 s, 2 → 4 s, 3 → 8 s, 4 → 16 s, 5 → dead-letter
 */
export function computeFailureOutcome(
  attemptsBefore: number,
  maxAttempts: number = RETRY_CONFIG.MAX_ATTEMPTS,
  now: Date = new Date(),
): FailureOutcome {
  const newAttempts = attemptsBefore + 1;

  if (newAttempts >= maxAttempts) {
    return { action: 'dead-letter', newAttempts };
  }

  // attempt N+1 uses slot N+1 in the delay curve (1-indexed)
  const delayMs = retryDelayMs(newAttempts);
  const nextRetryAt = new Date(now.getTime() + delayMs);

  return { action: 'retry', nextRetryAt, newAttempts };
}

// ── isLockStale ────────────────────────────────────────────────────────────

/**
 * Returns true when a lock is older than `ttlMs` milliseconds.
 *
 * A stale lock means the relay instance that claimed the row died (or was
 * restarted) without completing the publish.  The reaper calls this before
 * releasing the lock so another instance can reclaim the row.
 *
 * @param lockedAt  When the lock was acquired.
 * @param ttlMs     Lock time-to-live in milliseconds.
 * @param now       Reference time.  Defaults to new Date().
 */
export function isLockStale(
  lockedAt: Date,
  ttlMs: number,
  now: Date = new Date(),
): boolean {
  return now.getTime() - lockedAt.getTime() > ttlMs;
}

// ── generateInstanceId ────────────────────────────────────────────────────

/**
 * Generate a unique identifier for a relay instance.
 *
 * Format: `<hostname>:<pid>:<8-hex-chars>`
 *
 * The random suffix ensures uniqueness even when multiple relay instances
 * start on the same host in the same process (rare, but possible in tests).
 *
 * NOTE: instanceId is for OBSERVABILITY only — it appears in `locked_by`
 * so operators can correlate a lock to an instance in logs/metrics.
 * Correctness (no double-publish) is guaranteed by Postgres SKIP LOCKED,
 * not by this value.
 */
export function generateInstanceId(): string {
  return `${hostname()}:${process.pid}:${randomBytes(4).toString('hex')}`;
}
