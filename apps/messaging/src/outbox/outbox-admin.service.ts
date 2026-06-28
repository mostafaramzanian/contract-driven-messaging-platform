import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import type { DataSource } from 'typeorm';

export interface ReplayResult {
  /** Number of rows reset to pending. */
  replayed: number;
  /** IDs that were reset. */
  ids: number[];
}

/**
 * OutboxAdminService
 *
 * Operational surface for recovering `failed` outbox rows without manual
 * SQL surgery.  It is intentionally a thin service — all heavy lifting
 * (claim, publish, back-off) remains in `OutboxRelayService`.
 *
 * ## Why a separate service?
 *
 * `OutboxRelayService` handles the continuous polling / publish loop.
 * Admin / recovery is a distinct concern (triggered on demand by an
 * operator, not a timer) and warrants its own class to keep single
 * responsibility intact.  The separation also makes both services easier
 * to test in isolation.
 *
 * ## Decision: reset attempts to 0
 *
 * When an operator triggers a replay they explicitly grant a fresh retry
 * budget.  Preserving the old attempt count would cause the row to dead-
 * letter again on the very first failure because the counter is already at
 * `max_attempts`.  Resetting to 0 gives the row a full budget.  The
 * original failure evidence is preserved in `last_error` and `created_at`
 * and is visible in application logs / Postgres for audit purposes.
 *
 * ## How operators invoke this
 *
 *  1. **HTTP** (preferred) — `OutboxAdminController` exposes:
 *       POST /internal/outbox/:id/replay    — single event
 *       POST /internal/outbox/replay-failed — bulk (all failed, or
 *                                             filtered by event_type)
 *     Both endpoints are on the messaging service's internal HTTP server
 *     (port 3006, same as `/internal/health/*`).  They are NOT routed
 *     through Nginx and are not reachable from the public internet.
 *
 *  2. **One-off script** — for environments without access to the internal
 *     port, inject `OutboxAdminService` into a short-lived script module
 *     and call the methods directly.
 */
@Injectable()
export class OutboxAdminService {
  private readonly logger = new Logger(OutboxAdminService.name);

  constructor(
    @InjectDataSource()
    private readonly dataSource: DataSource,
  ) {}

  /**
   * Reset a single `failed` outbox row to `pending` so it re-enters the
   * normal relay cycle on the next poll.
   *
   * @throws NotFoundException when the row does not exist or is not `failed`.
   */
  async replayById(id: number): Promise<ReplayResult> {
    const rows: Array<{ id: number }> = await this.dataSource.query(
      `
      UPDATE outbox_events
      SET status        = 'pending',
          attempts      = 0,
          next_retry_at = now(),
          locked_at     = NULL,
          locked_by     = NULL
      WHERE id     = $1
        AND status = 'failed'
      RETURNING id
      `,
      [id],
    );

    if (rows.length === 0) {
      throw new NotFoundException(
        `Outbox event id=${id} not found or is not in 'failed' status`,
      );
    }

    this.logger.log(`Replayed outbox event id=${id} (reset to pending)`);

    return { replayed: 1, ids: [id] };
  }

  /**
   * Reset ALL `failed` outbox rows (optionally scoped to one event type)
   * back to `pending` so they re-enter the relay cycle.
   *
   * @param options.eventType  Optional filter — only replay events of this
   *                           type (e.g. 'MessagePersisted').
   * @returns Summary of how many rows were reset and their IDs.
   */
  async replayAllFailed(
    options: { eventType?: string } = {},
  ): Promise<ReplayResult> {
    const { eventType } = options;

    let rows: Array<{ id: number }>;

    if (eventType) {
      rows = await this.dataSource.query(
        `
        UPDATE outbox_events
        SET status        = 'pending',
            attempts      = 0,
            next_retry_at = now(),
            locked_at     = NULL,
            locked_by     = NULL
        WHERE status     = 'failed'
          AND event_type = $1
        RETURNING id
        `,
        [eventType],
      );
    } else {
      rows = await this.dataSource.query(
        `
        UPDATE outbox_events
        SET status        = 'pending',
            attempts      = 0,
            next_retry_at = now(),
            locked_at     = NULL,
            locked_by     = NULL
        WHERE status = 'failed'
        RETURNING id
        `,
      );
    }

    const ids = rows.map((r) => r.id);

    this.logger.log(
      `Replayed ${ids.length} failed outbox event(s) ` +
        (eventType ? `(eventType=${eventType}) ` : '') +
        `[ids: ${ids.join(', ') || 'none'}]`,
    );

    return { replayed: ids.length, ids };
  }
}
