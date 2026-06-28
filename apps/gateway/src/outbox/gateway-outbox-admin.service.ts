import { Injectable, Logger } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import type { DataSource } from 'typeorm';

export interface GatewayOutboxRowSummary {
  id: number;
  eventType: string;
  status: string;
  attempts: number;
  maxAttempts: number;
  lastError: string | null;
  correlationId: string | null;
  eventId: string | null;
}

/**
 * GatewayOutboxAdminService
 *
 * Operator recovery surface for the gateway's producer-side outbox —
 * mirrors `OutboxAdminService` in the messaging app field-for-field.
 * Lets an operator inspect and replay rows that exhausted their retry
 * budget (`status = 'failed'`) without touching the database by hand.
 */
@Injectable()
export class GatewayOutboxAdminService {
  private readonly logger = new Logger(GatewayOutboxAdminService.name);

  constructor(
    @InjectDataSource()
    private readonly dataSource: DataSource,
  ) {}

  async listFailed(): Promise<GatewayOutboxRowSummary[]> {
    interface Row {
      id: number;
      event_type: string;
      status: string;
      attempts: number;
      max_attempts: number;
      last_error: string | null;
      correlation_id: string | null;
      event_id: string | null;
    }

    const rows: Row[] = await this.dataSource.query(
      `SELECT id, event_type, status, attempts, max_attempts, last_error, correlation_id, event_id
       FROM gateway_outbox_events
       WHERE status = 'failed'
       ORDER BY id ASC`,
    );

    return rows.map((r) => ({
      id: r.id,
      eventType: r.event_type,
      status: r.status,
      attempts: r.attempts,
      maxAttempts: r.max_attempts,
      lastError: r.last_error ?? null,
      correlationId: r.correlation_id ?? null,
      eventId: r.event_id ?? null,
    }));
  }

  /**
   * Reset a single failed row back to `pending` so it re-enters the
   * normal claim/publish cycle on the next relay poll. `event_id` and
   * `correlation_id` are left untouched — this is a replay of the SAME
   * logical event, not a new one.
   */
  async replayById(id: number): Promise<{ replayed: boolean }> {
    const result: { id: number }[] = await this.dataSource.query(
      `UPDATE gateway_outbox_events
       SET status        = 'pending',
           attempts      = 0,
           next_retry_at = now(),
           locked_at     = NULL,
           locked_by     = NULL
       WHERE id = $1 AND status = 'failed'
       RETURNING id`,
      [id],
    );

    const replayed = result.length > 0;
    this.logger.log(
      `replayById(${id}) -> ${replayed ? 'reset to pending' : 'no matching failed row'}`,
    );
    return { replayed };
  }

  /**
   * Reset every `failed` row (optionally filtered by `eventType`) back to
   * `pending`. Mirrors `OutboxAdminService.replayAllFailed`.
   */
  async replayAllFailed(filter?: {
    eventType?: string;
  }): Promise<{ replayedCount: number }> {
    const params: unknown[] = [];
    let where = `status = 'failed'`;
    if (filter?.eventType) {
      params.push(filter.eventType);
      where += ` AND event_type = $${params.length}`;
    }

    const result: { id: number }[] = await this.dataSource.query(
      `UPDATE gateway_outbox_events
       SET status        = 'pending',
           attempts      = 0,
           next_retry_at = now(),
           locked_at     = NULL,
           locked_by     = NULL
       WHERE ${where}
       RETURNING id`,
      params,
    );

    this.logger.log(
      `replayAllFailed(${JSON.stringify(filter ?? {})}) -> ${result.length} row(s)`,
    );
    return { replayedCount: result.length };
  }
}
