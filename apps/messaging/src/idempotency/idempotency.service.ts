import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import type { Repository } from 'typeorm';
import { ProcessedEvent } from '../entities/processed-event.entity';

/** Payload required to register a successfully processed event. */
export interface MarkProcessedOptions {
  eventId: string;
  eventType: string;
  correlationId?: string;
  /** Compact summary of the produced output, stored for cached responses. */
  result?: unknown;
}

/** What the caller receives when a duplicate is detected. */
export interface DuplicateRecord {
  isDuplicate: true;
  processedAt: Date;
  result?: unknown;
}

/** What the caller receives when the event is brand-new and was recorded. */
export interface RecordedEvent {
  isDuplicate: false;
  processedAt: Date;
}

export type IdempotencyResult = DuplicateRecord | RecordedEvent;

/**
 * IdempotencyService
 *
 * Implements exactly-once processing semantics backed by the
 * `processed_events` table.
 *
 * ## Contract
 *
 * Call `checkAndMark` **before** performing any side effects.  It
 * atomically checks whether the event was already processed and, if not,
 * inserts the record:
 *
 * ```
 * const idempotency = await this.idempotencyService.checkAndMark({
 *   eventId: event.eventId,
 *   eventType: CreateMessageEvent.name,
 *   correlationId: event.correlationId,
 * });
 *
 * if (idempotency.isDuplicate) {
 *   // Return cached result, ack the AMQP message, done.
 *   return idempotency.result;
 * }
 *
 * // … perform side effects …
 * ```
 *
 * ## Atomicity
 *
 * We rely on PostgreSQL's UNIQUE constraint on `event_id` as the
 * serialisation point.  Two concurrent deliveries of the same event will
 * race at the INSERT; exactly one wins, the other catches error code
 * 23505 (unique_violation) and returns `{ isDuplicate: true }`.  This is
 * safer than a SELECT-then-INSERT because there is no gap between the
 * read and write.
 *
 * ## Retry safety
 *
 * AMQP at-least-once delivery means a message may be redelivered after a
 * consumer crash.  If the crash happened *after* `processed_events` was
 * written but *before* the AMQP ack was sent, the redelivery will be
 * correctly identified as a duplicate and the side effects will be
 * skipped.
 */
@Injectable()
export class IdempotencyService {
  private readonly logger = new Logger(IdempotencyService.name);

  constructor(
    @InjectRepository(ProcessedEvent)
    private readonly repo: Repository<ProcessedEvent>,
  ) {}

  /**
   * Atomically check whether eventId was already processed, and if not,
   * record it as processed.
   *
   * @returns `{ isDuplicate: true, ... }` when the event was seen before,
   *          `{ isDuplicate: false, ... }` when this is the first delivery.
   */
  async checkAndMark(
    options: MarkProcessedOptions,
  ): Promise<IdempotencyResult> {
    const { eventId, eventType, correlationId, result } = options;

    // ── Optimistic path: attempt INSERT first ──────────────────────────
    // We skip a pre-flight SELECT because SELECT-then-INSERT has a TOCTOU
    // race under concurrent deliveries.  The UNIQUE constraint catches
    // duplicates atomically.
    try {
      const record = this.repo.create({
        eventId,
        eventType,
        correlationId,
        result,
      });
      const saved = await this.repo.save(record);

      this.logger.debug(
        `Idempotency record created (eventId=${eventId}, type=${eventType})`,
        IdempotencyService.name,
      );

      return { isDuplicate: false, processedAt: saved.processedAt };
    } catch (err: unknown) {
      // ── Duplicate branch: PostgreSQL unique violation ──────────────────
      // TypeORM wraps the pg error; the original driverError carries the
      // code.  We access it defensively to handle both TypeORM v0.2/v0.3
      // error shapes.
      const code = this.extractPgCode(err);

      if (code === '23505') {
        // Fetch the original record so we can return cached result
        const existing = await this.repo.findOne({ where: { eventId } });

        this.logger.warn(
          `Duplicate event detected — skipping processing ` +
            `(eventId=${eventId}, type=${eventType}, ` +
            `originalProcessedAt=${existing?.processedAt?.toISOString() ?? 'unknown'})`,
          IdempotencyService.name,
        );

        return {
          isDuplicate: true,
          processedAt: existing?.processedAt ?? new Date(),
          result: existing?.result,
        };
      }

      // Any other DB error is a genuine fault — propagate it
      throw err;
    }
  }

  /**
   * Pure lookup — does not insert anything.  Used when you need to check
   * idempotency without committing to a new record (e.g. read-only health
   * probes or test assertions).
   */
  async findByEventId(eventId: string): Promise<ProcessedEvent | null> {
    return this.repo.findOne({ where: { eventId } });
  }

  // ── Private helpers ──────────────────────────────────────────────────

  /**
   * Extract PostgreSQL error code from various TypeORM error shapes.
   * TypeORM wraps the native pg error; the code can appear as:
   *  - err.code            (TypeORM v0.3+ wraps pg error directly)
   *  - err.driverError.code (older TypeORM / pg-pool shapes)
   */
  private extractPgCode(err: unknown): string | undefined {
    if (typeof err !== 'object' || err === null) return undefined;

    const errObj = err as Record<string, unknown>;

    if (typeof errObj['code'] === 'string') return errObj['code'];

    const driver = errObj['driverError'];
    if (typeof driver === 'object' && driver !== null) {
      const driverObj = driver as Record<string, unknown>;
      if (typeof driverObj['code'] === 'string') return driverObj['code'];
    }

    return undefined;
  }
}
