/**
 * pg-client.ts
 *
 * Thin wrapper around the `pg` client used exclusively by reliability tests.
 * Provides helpers for inspecting and surgically mutating the database to
 * simulate crash scenarios, state validation, and recovery assertions.
 *
 * All methods are intentionally low-level (raw SQL) to avoid the ORM layer
 * that the application itself uses — this keeps tests honest and removes any
 * risk that TypeORM caching or entity mapping masks a real database state.
 */

import { Client } from 'pg';

export const DB_CONFIG = {
  host: process.env.TEST_DB_HOST ?? '127.0.0.1',
  port: Number.parseInt(process.env.TEST_DB_PORT ?? '5432', 10),
  user: process.env.TEST_DB_USERNAME ?? 'admin',
  password: process.env.TEST_DB_PASSWORD ?? 'test_password',
  database: process.env.TEST_DB_NAME ?? 'showcase_test_db',
  connectionTimeoutMillis: 30_000,
};

export interface OutboxRow {
  id: number;
  event_type: string;
  payload: unknown;
  correlation_id: string | null;
  status: 'pending' | 'sent' | 'failed';
  attempts: number;
  max_attempts: number;
  last_error: string | null;
  locked_at: Date | null;
  locked_by: string | null;
  lock_version: number;
  next_retry_at: Date;
  sent_at: Date | null;
  event_id: string | null;
}

export interface ProcessedEventRow {
  id: number;
  event_id: string;
  event_type: string;
  correlation_id: string | null;
  processed_at: Date;
}

export interface EventAttemptRow {
  id: number;
  event_id: string;
  attempts: number;
  last_attempted_at: Date;
}

export class PgTestClient {
  private client: Client;

  constructor() {
    this.client = new Client(DB_CONFIG);
  }

  async connect(): Promise<void> {
    await this.client.connect();
  }

  async disconnect(): Promise<void> {
    await this.client.end();
  }

  // ── Outbox Queries ──────────────────────────────────────────────────────

  async getOutboxRow(id: number): Promise<OutboxRow | null> {
    const res = await this.client.query<OutboxRow>(
      'SELECT * FROM outbox_events WHERE id = $1',
      [id],
    );
    return res.rows[0] ?? null;
  }

  async getOutboxRowByCorrelationId(correlationId: string): Promise<OutboxRow | null> {
    const res = await this.client.query<OutboxRow>(
      'SELECT * FROM outbox_events WHERE correlation_id = $1 ORDER BY id DESC LIMIT 1',
      [correlationId],
    );
    return res.rows[0] ?? null;
  }

  async getOutboxRowByEventId(eventId: string): Promise<OutboxRow | null> {
    const res = await this.client.query<OutboxRow>(
      'SELECT * FROM outbox_events WHERE event_id = $1',
      [eventId],
    );
    return res.rows[0] ?? null;
  }

  async countOutboxByStatus(status: string): Promise<number> {
    const res = await this.client.query<{ count: string }>(
      'SELECT COUNT(*) as count FROM outbox_events WHERE status = $1',
      [status],
    );
    return parseInt(res.rows[0]?.count ?? '0', 10);
  }

  async getAllPendingOutboxRows(): Promise<OutboxRow[]> {
    const res = await this.client.query<OutboxRow>(
      "SELECT * FROM outbox_events WHERE status = 'pending' ORDER BY id",
    );
    return res.rows;
  }

  async getAllFailedOutboxRows(): Promise<OutboxRow[]> {
    const res = await this.client.query<OutboxRow>(
      "SELECT * FROM outbox_events WHERE status = 'failed' ORDER BY id",
    );
    return res.rows;
  }

  /** Freeze an outbox row in pending/locked state — simulates crash before markSent */
  async freezeOutboxRowAsLocked(
    id: number,
    lockedBy: string = 'crashed-relay-instance',
  ): Promise<void> {
    await this.client.query(
      `UPDATE outbox_events
       SET locked_at = now(),
           locked_by = $1,
           status    = 'pending'
       WHERE id = $2`,
      [lockedBy, id],
    );
  }

  /** Reset lock to simulate stale-lock reaper clearing a crashed relay's claim */
  async clearOutboxLock(id: number): Promise<void> {
    await this.client.query(
      `UPDATE outbox_events
       SET locked_at = NULL,
           locked_by = NULL
       WHERE id = $1`,
      [id],
    );
  }

  /** Insert a raw outbox row — simulates the transaction having committed but relay not yet run */
  async insertOutboxRow(opts: {
    eventType: string;
    payload: unknown;
    correlationId?: string;
    eventId: string;
    status?: string;
    attempts?: number;
    maxAttempts?: number;
  }): Promise<number> {
    const res = await this.client.query<{ id: number }>(
      `INSERT INTO outbox_events
         (event_type, payload, correlation_id, status, attempts, max_attempts, next_retry_at, event_id, lock_version)
       VALUES ($1, $2, $3, $4, $5, $6, now(), $7, 0)
       RETURNING id`,
      [
        opts.eventType,
        JSON.stringify(opts.payload),
        opts.correlationId ?? null,
        opts.status ?? 'pending',
        opts.attempts ?? 0,
        opts.maxAttempts ?? 5,
        opts.eventId,
      ],
    );
    return res.rows[0]!.id;
  }

  /** Force an outbox row to 'failed' for DLQ replay tests */
  async forceOutboxFailed(id: number, lastError?: string): Promise<void> {
    await this.client.query(
      `UPDATE outbox_events
       SET status     = 'failed',
           attempts   = max_attempts,
           last_error = $2,
           locked_at  = NULL,
           locked_by  = NULL
       WHERE id = $1`,
      [id, lastError ?? 'Forced failure for test'],
    );
  }

  /** Reset a failed outbox row to pending (mirrors OutboxAdminService.replayById) */
  async replayOutboxRow(id: number): Promise<boolean> {
    const res = await this.client.query<{ id: number }>(
      `UPDATE outbox_events
       SET status        = 'pending',
           attempts      = 0,
           next_retry_at = now(),
           locked_at     = NULL,
           locked_by     = NULL
       WHERE id = $1 AND status = 'failed'
       RETURNING id`,
      [id],
    );
    return res.rows.length > 0;
  }

  /** Set next_retry_at into the past to make a row immediately claimable */
  async makeRetryImmediatelyClaimable(id: number): Promise<void> {
    await this.client.query(
      `UPDATE outbox_events SET next_retry_at = now() - interval '1 second' WHERE id = $1`,
      [id],
    );
  }

  // ── Processed Events ────────────────────────────────────────────────────

  async getProcessedEvent(eventId: string): Promise<ProcessedEventRow | null> {
    const res = await this.client.query<ProcessedEventRow>(
      'SELECT * FROM processed_events WHERE event_id = $1',
      [eventId],
    );
    return res.rows[0] ?? null;
  }

  async countProcessedEvents(eventId: string): Promise<number> {
    const res = await this.client.query<{ count: string }>(
      'SELECT COUNT(*) as count FROM processed_events WHERE event_id = $1',
      [eventId],
    );
    return parseInt(res.rows[0]?.count ?? '0', 10);
  }

  // ── Event Attempts ──────────────────────────────────────────────────────

  async getEventAttempts(eventId: string): Promise<EventAttemptRow | null> {
    const res = await this.client.query<EventAttemptRow>(
      'SELECT * FROM event_attempts WHERE event_id = $1',
      [eventId],
    );
    return res.rows[0] ?? null;
  }

  // ── Messages Table ──────────────────────────────────────────────────────

  async countMessagesByCorrelationId(correlationId: string): Promise<number> {
    const res = await this.client.query<{ count: string }>(
      'SELECT COUNT(*) as count FROM messages WHERE correlation_id = $1',
      [correlationId],
    );
    return parseInt(res.rows[0]?.count ?? '0', 10);
  }

  // ── Generic ─────────────────────────────────────────────────────────────

  async query<T extends Record<string, unknown>>(
    sql: string,
    params?: unknown[],
  ): Promise<T[]> {
    const res = await this.client.query<T>(sql, params);
    return res.rows;
  }

  async isReachable(): Promise<boolean> {
    try {
      await this.client.query('SELECT 1');
      return true;
    } catch {
      return false;
    }
  }
}

/**
 * Poll until a condition is true or the timeout is reached.
 * Used for eventually-consistent reliability assertions — the right tool for
 * "did the relay eventually publish this?" but NOT for initial infrastructure
 * readiness (that uses waitForHttpReady / waitForRabbitMqReady).
 */
export async function pollUntil(
  label: string,
  check: () => Promise<boolean>,
  opts: { timeoutMs: number; intervalMs?: number } = { timeoutMs: 30_000 },
): Promise<void> {
  const { timeoutMs, intervalMs = 500 } = opts;
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    if (await check()) return;
    await new Promise((r) => setTimeout(r, intervalMs));
  }

  throw new Error(`pollUntil("${label}") timed out after ${timeoutMs}ms`);
}
