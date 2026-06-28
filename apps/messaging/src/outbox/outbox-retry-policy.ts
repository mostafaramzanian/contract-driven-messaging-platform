/**
 * OutboxRetryPolicy — re-export shim
 *
 * The canonical pure functions now live in `@app/contracts` (see
 * `libs/contracts/src/outbox/outbox-retry-policy.ts`) so the gateway's
 * producer outbox relay (`GatewayOutboxRelayService`) can reuse the exact
 * same fencing/back-off logic instead of re-implementing it — see CRITICAL
 * ISSUE #1's requirement to "reuse existing outbox patterns where possible".
 *
 * Kept, unchanged in its exported surface, so every existing import of
 * `from './outbox-retry-policy'` elsewhere in this app continues to
 * resolve without modification.
 */
export {
  computeFailureOutcome,
  isLockStale,
  generateInstanceId,
  type FailureOutcome,
} from '@app/contracts';
