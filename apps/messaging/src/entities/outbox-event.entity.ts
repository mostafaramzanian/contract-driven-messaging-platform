import {
  Entity,
  Column,
  PrimaryGeneratedColumn,
  CreateDateColumn,
  Index,
} from 'typeorm';

export type OutboxStatus = 'pending' | 'sent' | 'failed';

/**
 * OutboxEvent — transactional outbox ledger.
 *
 * Every domain event that must eventually be forwarded to a downstream
 * consumer is written here in the SAME logical unit of work (QueryRunner
 * transaction) as the business row it describes.
 *
 * ## Relay lifecycle
 *
 *  1. `OutboxTransactionService.runWithOutboxEvents()` inserts a row here
 *     atomically alongside the business write.  Initial state:
 *       status=pending, attempts=0, next_retry_at=now().
 *
 *  2. `OutboxRelayService` polls for `pending` rows with
 *     `next_retry_at <= now()` using a `SELECT … FOR UPDATE SKIP LOCKED`
 *     claim query, sets `locked_at` + `locked_by`, then publishes to
 *     RabbitMQ and transitions to `sent` or increments `attempts`.
 *
 *  3. If `attempts >= max_attempts` the relay transitions to `failed`.
 *     An operator can then invoke `OutboxAdminService.replayById()` or
 *     `replayAllFailed()` to reset a failed row back to `pending` so it
 *     re-enters the normal claim/publish cycle.
 *
 * ## Locking columns (added in migration 005)
 *
 *  locked_at   — timestamp when the relay instance claimed this row.
 *                NULL when not locked.
 *  locked_by   — instanceId of the relay that holds the lock.
 *                Informational only; correctness is guaranteed by
 *                Postgres SKIP LOCKED, not by this value.
 *  next_retry_at — earliest time this row may be claimed again after a
 *                  failed attempt (exponential back-off).
 *  max_attempts  — per-row retry budget (default 5).
 */
@Entity('outbox_events')
export class OutboxEvent {
  @PrimaryGeneratedColumn()
  id: number;

  @Index()
  @Column({ name: 'event_type', type: 'varchar', length: 255 })
  eventType: string;

  @Column({ type: 'jsonb' })
  payload: unknown;

  @Index()
  @Column({
    name: 'correlation_id',
    type: 'varchar',
    length: 36,
    nullable: true,
  })
  correlationId?: string;

  @Index()
  @Column({ type: 'varchar', length: 16, default: 'pending' })
  status: OutboxStatus;

  @Column({ name: 'attempts', type: 'int', default: 0 })
  attempts: number;

  @Column({ name: 'last_error', type: 'text', nullable: true })
  lastError?: string;

  @CreateDateColumn({ name: 'created_at' })
  createdAt: Date;

  @Column({ name: 'sent_at', type: 'timestamp', nullable: true })
  sentAt?: Date;

  // ── Relay-locking fields (migration 005) ───────────────────────────────

  /** Timestamp when a relay instance claimed this row for publishing.
   *  NULL = not currently locked. */
  @Column({ name: 'locked_at', type: 'timestamp', nullable: true })
  lockedAt?: Date;

  /** Instance-id of the relay that holds the lock.
   *  Informational — correctness is via SKIP LOCKED, not this value. */
  @Column({ name: 'locked_by', type: 'varchar', length: 128, nullable: true })
  lockedBy?: string;

  /** Earliest time this row may be re-claimed (back-off schedule).
   *  Set to now() at INSERT; updated to now()+backoff on each failure. */
  @Column({
    name: 'next_retry_at',
    type: 'timestamp',
    default: () => 'CURRENT_TIMESTAMP',
  })
  nextRetryAt: Date;

  /** Per-row retry budget.  Rows with attempts >= maxAttempts are failed. */
  @Column({ name: 'max_attempts', type: 'int', default: 5 })
  maxAttempts: number;

  // ── Production-readiness additions (migrations 007–008) ────────────────

  /**
   * Stable, application-level event identifier — generated once at row
   * creation and reused unchanged across every retry or operator-replay
   * of this same row (unlike `id`, the surrogate primary key, which has
   * no meaning to a downstream consumer's idempotency check).
   *
   * Nullable because rows created before migration 007 have none; new
   * rows always have one (see `OutboxTransactionService.runWithOutboxEvents`).
   */
  @Column({ name: 'event_id', type: 'varchar', length: 36, nullable: true })
  eventId?: string;

  /**
   * Fencing token for the relay claim/publish/markSent cycle. Incremented
   * on every successful claim; `OutboxRelayService.markSent()` only
   * succeeds if the caller's `lockVersion` still matches, which detects
   * (rather than silently allowing) a double-publish caused by
   * `reapStaleLocks()` releasing a lock held by a claimant that was slow,
   * not dead.
   */
  @Column({ name: 'lock_version', type: 'int', default: 0 })
  lockVersion: number;

  /**
   * W3C trace-context propagation carrier (e.g. `{ traceparent: "00-..." }`,
   * the same shape `propagation.inject`/`extract` from `@opentelemetry/api`
   * already use for AMQP headers elsewhere in this codebase — see
   * `libs/common/src/tracing/amqp-propagation.ts`), captured from the
   * active span at the moment this row was written (inside the original
   * HTTP request's transaction). `OutboxRelayService.publishOne` uses
   * `extractTraceContext`/`context.with(...)` against this carrier to
   * propagate the *original* producer's trace on publish, instead of
   * whatever ambient (parentless) context exists at relay-poll time.
   */
  @Column({ name: 'trace_context', type: 'jsonb', nullable: true })
  traceContext?: Record<string, string> | null;
}
