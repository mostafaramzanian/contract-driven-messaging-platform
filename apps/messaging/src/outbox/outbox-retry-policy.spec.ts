import {
  computeFailureOutcome,
  isLockStale,
  generateInstanceId,
} from './outbox-retry-policy';
import { RETRY_CONFIG, retryDelayMs } from '../reliability/topology';

describe('outbox-retry-policy', () => {
  // Fixed clock for deterministic assertions
  const NOW = new Date('2024-01-15T10:00:00.000Z');

  // ── computeFailureOutcome ───────────────────────────────────────────────

  describe('computeFailureOutcome', () => {
    it('returns retry with correct delay for attempt 1 (0 before → 1 after)', () => {
      const result = computeFailureOutcome(0, 5, NOW);

      expect(result.action).toBe('retry');
      if (result.action !== 'retry') return; // type narrowing

      const expectedDelayMs = retryDelayMs(1); // 2 000 ms
      const expectedNextRetry = new Date(NOW.getTime() + expectedDelayMs);

      expect(result.newAttempts).toBe(1);
      expect(result.nextRetryAt).toEqual(expectedNextRetry);
    });

    it('returns retry with exponentially growing delay for attempt 2', () => {
      const result = computeFailureOutcome(1, 5, NOW);

      expect(result.action).toBe('retry');
      if (result.action !== 'retry') return;

      const expectedDelayMs = retryDelayMs(2); // 4 000 ms
      expect(result.newAttempts).toBe(2);
      expect(result.nextRetryAt).toEqual(
        new Date(NOW.getTime() + expectedDelayMs),
      );
    });

    it('returns retry for attempt 3 (2 before)', () => {
      const result = computeFailureOutcome(2, 5, NOW);
      expect(result.action).toBe('retry');
      if (result.action !== 'retry') return;
      expect(result.newAttempts).toBe(3);
      expect(result.nextRetryAt).toEqual(
        new Date(NOW.getTime() + retryDelayMs(3)),
      );
    });

    it('returns dead-letter when newAttempts equals maxAttempts', () => {
      // 4 before → 5 after = maxAttempts (5) → dead-letter
      const result = computeFailureOutcome(4, 5, NOW);

      expect(result.action).toBe('dead-letter');
      expect(result.newAttempts).toBe(5);
    });

    it('returns dead-letter when newAttempts exceeds maxAttempts', () => {
      // shouldn't normally happen but must handle gracefully
      const result = computeFailureOutcome(6, 5, NOW);
      expect(result.action).toBe('dead-letter');
      expect(result.newAttempts).toBe(7);
    });

    it('uses RETRY_CONFIG.MAX_ATTEMPTS when maxAttempts is omitted', () => {
      // With default maxAttempts (5), 4 before → dead-letter
      const result = computeFailureOutcome(4, undefined, NOW);
      expect(result.action).toBe('dead-letter');
    });

    it('respects a custom maxAttempts budget of 3', () => {
      // 1 before → 2 after < 3 → retry
      const retryResult = computeFailureOutcome(1, 3, NOW);
      expect(retryResult.action).toBe('retry');

      // 2 before → 3 after = 3 → dead-letter
      const dlResult = computeFailureOutcome(2, 3, NOW);
      expect(dlResult.action).toBe('dead-letter');
    });

    it('uses current time when now is omitted', () => {
      const before = Date.now();
      const result = computeFailureOutcome(0, 5);
      const after = Date.now();

      expect(result.action).toBe('retry');
      if (result.action !== 'retry') return;

      expect(result.nextRetryAt.getTime()).toBeGreaterThanOrEqual(
        before + retryDelayMs(1),
      );
      expect(result.nextRetryAt.getTime()).toBeLessThanOrEqual(
        after + retryDelayMs(1),
      );
    });
  });

  // ── isLockStale ─────────────────────────────────────────────────────────

  describe('isLockStale', () => {
    it('returns false when lock is younger than TTL', () => {
      const lockedAt = new Date(NOW.getTime() - 30_000); // 30s ago
      expect(isLockStale(lockedAt, 60_000, NOW)).toBe(false);
    });

    it('returns true when lock is older than TTL', () => {
      const lockedAt = new Date(NOW.getTime() - 90_000); // 90s ago
      expect(isLockStale(lockedAt, 60_000, NOW)).toBe(true);
    });

    it('returns false at the exact TTL boundary', () => {
      const lockedAt = new Date(NOW.getTime() - 60_000); // exactly 60s
      // strictly greater than, so exactly at TTL is NOT stale
      expect(isLockStale(lockedAt, 60_000, NOW)).toBe(false);
    });

    it('returns true when lock is 1ms beyond TTL', () => {
      const lockedAt = new Date(NOW.getTime() - 60_001);
      expect(isLockStale(lockedAt, 60_000, NOW)).toBe(true);
    });
  });

  // ── generateInstanceId ──────────────────────────────────────────────────

  describe('generateInstanceId', () => {
    it('returns a string in hostname:pid:hex format', () => {
      const id = generateInstanceId();
      // format: <word chars and dots>:<digits>:<8 hex chars>
      expect(id).toMatch(/^[^:]+:\d+:[0-9a-f]{8}$/);
    });

    it('produces unique ids on each call', () => {
      const ids = new Set(
        Array.from({ length: 20 }, () => generateInstanceId()),
      );
      // With 4 random bytes the collision probability is negligible; expect
      // all 20 to be unique.
      expect(ids.size).toBe(20);
    });
  });
});
