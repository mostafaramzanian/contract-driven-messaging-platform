import {
  Entity,
  Column,
  PrimaryGeneratedColumn,
  CreateDateColumn,
  Index,
} from 'typeorm';

/**
 * ProcessedEvent entity — idempotency ledger.
 *
 * One row per event_id that has been fully processed by the messaging
 * service.  The UNIQUE constraint on event_id (defined in the migration
 * and mirrored via @Index here) is the primary correctness guarantee:
 * any attempt to INSERT a duplicate event_id raises a PostgreSQL
 * unique-violation (code 23505) which the IdempotencyService catches
 * and converts into a "skip this duplicate" signal.
 *
 * The `result` column is typed as `unknown` in TypeScript but stored as
 * JSONB.  It carries a compact summary of what was produced (e.g.
 * `{ messageId: 42 }`) so that a duplicate delivery can return a cached
 * response without re-querying the messages table.
 */
@Entity('processed_events')
export class ProcessedEvent {
  @PrimaryGeneratedColumn()
  id: number;

  /**
   * The logical event identifier, set once by the producer and never
   * changed downstream.  Maps to EventEnvelope.eventId.
   */
  @Index({ unique: true })
  @Column({ name: 'event_id', type: 'varchar', length: 36, nullable: false })
  eventId: string;

  /**
   * Human-readable event type name (e.g. "CreateMessageEvent.v1").
   * Stored for dashboards and operational queries.
   */
  @Column({ name: 'event_type', type: 'varchar', length: 255, nullable: false })
  eventType: string;

  /**
   * Propagated correlationId from the originating HTTP request.
   * Nullable because the legacy `test-rabbit` handler does not always
   * carry a valid UUID correlation id.
   */
  @Index()
  @Column({
    name: 'correlation_id',
    type: 'varchar',
    length: 36,
    nullable: true,
  })
  correlationId?: string;

  /**
   * Optional compact summary of the processing outcome.  Stored as JSONB
   * so callers can skip re-processing and return cached data.
   * Example: `{ messageId: 42 }`
   */
  @Column({ type: 'jsonb', nullable: true })
  result?: unknown;

  /**
   * Wall-clock time the event was first successfully processed.
   * Managed by the DB default (CURRENT_TIMESTAMP); the application
   * never sets this field explicitly.
   */
  @CreateDateColumn({ name: 'processed_at' })
  processedAt: Date;
}
