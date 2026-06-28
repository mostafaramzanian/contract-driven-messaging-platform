import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import type { Repository } from 'typeorm';
import { EventAttempt } from '../entities/event-attempt.entity';

/**
 * RetryAttemptTrackerService
 *
 * Durable, per-`eventId` delivery-attempt counter, backed by the
 * `event_attempts` table (migration 006). Replaces — for the purpose of
 * the `MAX_ATTEMPTS` retry-vs-DLQ decision — exclusive reliance on the
 * `x-retry-count` AMQP header.
 *
 * ## The gap this closes
 *
 * `x-retry-count` is set and carried forward ONLY by
 * `RetryPublisherService.publishToRetry()`. Every other way a message
 * can re-arrive at `MessagingController.handleMessage` starts that
 * counter at zero:
 *
 *  - An operator manually requeues a DLQ'd message via the RabbitMQ
 *    management UI (no header carried forward at all).
 *  - `OutboxAdminService.replayById()`/`replayAllFailed()` resets an
 *    outbox row's `status` back to `pending`, and the relay republishes
 *    it as a brand-new AMQP message with fresh headers.
 *  - The messaging consumer process itself restarts mid-redelivery —
 *    this doesn't reset anything by itself (the header lives on the
 *    AMQP message, not the consumer process), but it's the scenario
 *    that makes "is the cap actually durable" matter operationally:
 *    an operator restarting a stuck consumer should not also be
 *    silently resetting every in-flight message's retry budget.
 *
 * Each of these silently resets `RETRY_CONFIG.MAX_ATTEMPTS` from a true
 * lifetime cap on the logical event to a per-incident one — a message
 * that has already permanently failed 5 times can be manually replayed
 * and get another full 5 attempts, indefinitely, with no caller ever
 * noticing the cap was supposed to be a hard ceiling.
 *
 * ## Design
 *
 * `recordAttempt(eventId)` performs an atomic
 * `INSERT ... ON CONFLICT (event_id) DO UPDATE SET attempts = attempts + 1`
 * and returns the resulting count in the SAME round-trip — no
 * read-then-write race window, the same reasoning `IdempotencyService`
 * already applies to `processed_events`' UNIQUE-constraint INSERT.
 *
 * This durable count is consulted ALONGSIDE (not instead of) the
 * existing `x-retry-count` header in `MessagingController.handleMessage`:
 * the durable count is authoritative for the MAX_ATTEMPTS decision; the
 * header remains useful purely as a fast, no-DB-round-trip signal for
 * logging/metrics labels and is still carried forward by
 * `RetryPublisherService` for that purpose.
 */
@Injectable()
export class RetryAttemptTrackerService {
  private readonly logger = new Logger(RetryAttemptTrackerService.name);

  constructor(
    @InjectRepository(EventAttempt)
    private readonly repo: Repository<EventAttempt>,
  ) {}

  /**
   * Atomically increment and return the durable attempt count for
   * `eventId`. The FIRST call for a given `eventId` returns `1` (not
   * `0`) — "recording an attempt" means "this delivery is happening
   * right now", so the count returned always includes the attempt that
   * triggered this call.
   */
  async recordAttempt(eventId: string): Promise<number> {
    // INSERT ... ON CONFLICT DO UPDATE, single round-trip, race-safe
    // under concurrent deliveries of the same eventId (e.g. a duplicate
    // delivered to two different consumer instances at once) the same
    // way IdempotencyService's UNIQUE-constraint INSERT is.
    const rows: { attempts: number }[] = await this.repo.manager.query(
      `INSERT INTO event_attempts (event_id, attempts, updated_at)
       VALUES ($1, 1, now())
       ON CONFLICT (event_id)
       DO UPDATE SET attempts = event_attempts.attempts + 1, updated_at = now()
       RETURNING attempts`,
      [eventId],
    );

    const attempts = rows[0]?.attempts ?? 1;

    this.logger.debug(
      `Recorded delivery attempt ${attempts} for eventId=${eventId}`,
      RetryAttemptTrackerService.name,
    );

    return attempts;
  }

  /**
   * Read the current durable attempt count without incrementing it.
   * Returns 0 for an eventId with no recorded attempts yet (distinct
   * from `recordAttempt`'s return value, which is always >= 1 because
   * calling it IS an attempt).
   */
  async getAttemptCount(eventId: string): Promise<number> {
    const row = await this.repo.findOne({ where: { eventId } });
    return row?.attempts ?? 0;
  }

  /**
   * Remove the durable attempt record for `eventId`.
   *
   * Called once a logical event reaches a TERMINAL outcome (successfully
   * persisted, or permanently dead-lettered) — at that point the retry
   * budget no longer needs tracking, and clearing the row prevents
   * `event_attempts` from growing unbounded forever (same retention
   * concern as `processed_events`, see `IDX_event_attempts_updated_at`
   * in migration 006, added specifically to support this and any future
   * scheduled TTL-purge job for rows that are cleared late or never).
   *
   * Safe to call even if no row exists (no-op).
   */
  async clearAttempts(eventId: string): Promise<void> {
    await this.repo.delete({ eventId });
  }
}
