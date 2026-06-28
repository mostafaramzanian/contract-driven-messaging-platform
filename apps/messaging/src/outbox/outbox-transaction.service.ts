import { Injectable, Logger } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import type { DataSource, EntityManager } from 'typeorm';
import { randomUUID } from 'node:crypto';
import { captureTraceContextCarrier } from '@app/common';
import { OutboxEvent } from '../entities/outbox-event.entity';
import { ProcessedEvent } from '../entities/processed-event.entity';

export interface OutboxEventInput {
  eventType: string;
  payload: unknown;
  correlationId?: string;
  /** Override per-row retry budget. Defaults to entity default (5). */
  maxAttempts?: number;
}

export interface IdempotencyInput {
  eventId: string;
  eventType: string;
  correlationId?: string;
}

export type IdempotentRunResult<T> =
  | { duplicate: true }
  | { duplicate: false; result: T };

/**
 * OutboxTransactionService
 *
 * Provides `runWithOutboxEvents()` — a thin transactional envelope that
 * guarantees a business write and one or more outbox-event rows are
 * committed atomically to Postgres — and `runIdempotentWithOutboxEvents()`,
 * which additionally folds the idempotency-ledger INSERT into the same
 * transaction (see that method's doc comment for why this exists).
 *
 * ## Why this matters
 *
 * Without `runWithOutboxEvents`, a crash between `messageRepository.save()`
 * and `outboxRepo.save()` leaves no outbox row and the downstream consumer
 * never receives the event.  With this service, both writes succeed or
 * both are rolled back — the classic "dual write" problem is eliminated.
 *
 * ## Usage
 *
 * ```typescript
 * const saved = await this.outboxTransactionService.runWithOutboxEvents(
 *   async (em) => {
 *     const msg = em.create(Message, { title, content, sender });
 *     return em.save(Message, msg);
 *   },
 *   [{ eventType: 'MessagePersisted', payload: { messageId: 1 }, correlationId }],
 * );
 * ```
 *
 * The `work` callback MUST use the provided `EntityManager` (not an
 * injected repository) so that all writes share the same QueryRunner
 * transaction.
 */
@Injectable()
export class OutboxTransactionService {
  private readonly logger = new Logger(OutboxTransactionService.name);

  constructor(
    @InjectDataSource()
    private readonly dataSource: DataSource,
  ) {}

  /**
   * Execute `work` and atomically insert `events` in the same DB
   * transaction.  Returns the value returned by `work`.
   *
   * @param work    Callback that performs business writes via the
   *                transaction's EntityManager.
   * @param events  One or more outbox-event descriptors to persist.
   */
  async runWithOutboxEvents<T>(
    work: (em: EntityManager) => Promise<T>,
    events: OutboxEventInput[],
  ): Promise<T> {
    const qr = this.dataSource.createQueryRunner();
    await qr.connect();
    await qr.startTransaction();

    try {
      // ── 1. Execute caller's business writes ───────────────────────────
      const result = await work(qr.manager);

      // ── 2. Insert outbox events in the same transaction ───────────────
      await this.insertOutboxEvents(qr.manager, events);

      // ── 3. Commit ─────────────────────────────────────────────────────
      await qr.commitTransaction();
      return result;
    } catch (err: unknown) {
      await qr.rollbackTransaction();
      this.logger.error(
        'Transaction rolled back',
        err instanceof Error ? err.stack : String(err),
        OutboxTransactionService.name,
      );
      throw err;
    } finally {
      await qr.release();
    }
  }

  /**
   * Like `runWithOutboxEvents`, but additionally inserts the idempotency
   * ledger row (`ProcessedEvent`) inside the SAME transaction as the
   * business write and the outbox events.
   *
   * ## Why this exists — the bug it closes
   *
   * Previously, `IdempotencyService.checkAndMark()` ran as its own,
   * separate, already-committed transaction, BEFORE the controller called
   * into business processing (which itself was correctly transactional,
   * via `runWithOutboxEvents`, but as a SEPARATE transaction). That left a
   * real crash window: if the process died between the idempotency INSERT
   * committing and the business write committing, the idempotency row
   * existed but the business write did not. On redelivery, the duplicate
   * check would find the (orphaned) idempotency row, report
   * `isDuplicate: true`, and the controller would ack and skip — silently
   * and permanently losing a message that was never actually persisted.
   *
   * This method closes that window the same way `runWithOutboxEvents`
   * already closed the analogous message+outbox-event dual-write gap:
   * one transaction, all or nothing.
   *
   * ## Duplicate detection
   *
   * Same mechanism as `IdempotencyService.checkAndMark` — INSERT first,
   * rely on the UNIQUE constraint on `event_id` to detect a concurrent or
   * redelivered duplicate atomically (no separate SELECT-then-INSERT
   * TOCTOU window). The difference is that the duplicate check, the
   * business write, and the outbox events now share one QueryRunner: if
   * the idempotency INSERT hits the unique-violation branch, the
   * transaction is rolled back (nothing to roll back, in practice, since
   * `work` hasn't run yet) and `{ duplicate: true }` is returned without
   * ever invoking `work`.
   */
  async runIdempotentWithOutboxEvents<T>(
    idempotency: IdempotencyInput,
    work: (em: EntityManager) => Promise<T>,
    events: OutboxEventInput[],
  ): Promise<IdempotentRunResult<T>> {
    const qr = this.dataSource.createQueryRunner();
    await qr.connect();
    await qr.startTransaction();

    try {
      // ── 1. Idempotency INSERT first, inside the transaction ────────────
      try {
        await qr.manager.save(
          qr.manager.create(ProcessedEvent, {
            eventId: idempotency.eventId,
            eventType: idempotency.eventType,
            correlationId: idempotency.correlationId,
          }),
        );
      } catch (err: unknown) {
        if (this.isUniqueViolation(err)) {
          await qr.rollbackTransaction();
          this.logger.warn(
            `Duplicate event detected inside transactional idempotency check ` +
              `(eventId=${idempotency.eventId}, type=${idempotency.eventType}) — skipping`,
            OutboxTransactionService.name,
          );
          return { duplicate: true };
        }
        throw err;
      }

      // ── 2. Business write, same transaction ─────────────────────────────
      const result = await work(qr.manager);

      // ── 3. Outbox events, same transaction ───────────────────────────────
      await this.insertOutboxEvents(qr.manager, events);

      // ── 4. Commit — idempotency row, business write, and outbox events
      //       all succeed together or all roll back together. ─────────────
      await qr.commitTransaction();
      return { duplicate: false, result };
    } catch (err: unknown) {
      await qr.rollbackTransaction();
      this.logger.error(
        'Idempotent transaction rolled back',
        err instanceof Error ? err.stack : String(err),
        OutboxTransactionService.name,
      );
      throw err;
    } finally {
      await qr.release();
    }
  }

  // ── Private helpers ──────────────────────────────────────────────────

  private async insertOutboxEvents(
    em: EntityManager,
    events: OutboxEventInput[],
  ): Promise<void> {
    const now = new Date();
    // captureTraceContextCarrier() reads context.active() at the moment
    // this row is written — i.e. still inside the original caller's
    // span (the HTTP request that triggered this transaction) — so the
    // relay can later propagate the ORIGINAL trace, not its own ambient
    // one. See OutboxRelayService.publishOne and migration 008.
    const traceContext = captureTraceContextCarrier();

    for (const ev of events) {
      const row = em.create(OutboxEvent, {
        // Generated once, here, and never regenerated on retry/replay —
        // see OutboxEvent.eventId's doc comment and migration 007.
        eventId: randomUUID(),
        eventType: ev.eventType,
        payload: ev.payload,
        correlationId: ev.correlationId,
        status: 'pending',
        attempts: 0,
        nextRetryAt: now,
        maxAttempts: ev.maxAttempts ?? 5,
        traceContext:
          Object.keys(traceContext).length > 0 ? traceContext : null,
      });
      await em.save(OutboxEvent, row);
    }
  }

  /**
   * Extract PostgreSQL error code from various TypeORM error shapes.
   * Same logic as `IdempotencyService.extractPgCode` — duplicated rather
   * than shared because the two services are intentionally decoupled
   * (this one operates on a QueryRunner's EntityManager, not a Repository)
   * and the helper is a few lines of pure logic, not worth a shared
   * abstraction across module boundaries for this.
   */
  private isUniqueViolation(err: unknown): boolean {
    if (typeof err !== 'object' || err === null) return false;
    const errObj = err as Record<string, unknown>;
    if (errObj['code'] === '23505') return true;
    const driver = errObj['driverError'];
    if (typeof driver === 'object' && driver !== null) {
      return (driver as Record<string, unknown>)['code'] === '23505';
    }
    return false;
  }
}
