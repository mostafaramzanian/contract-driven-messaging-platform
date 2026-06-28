import { MigrationInterface, QueryRunner, Table, TableIndex } from 'typeorm';

/**
 * Migration 006 — event_attempts
 *
 * Tracks a durable, per-eventId attempt counter, independent of the
 * `x-retry-count` AMQP header that `MessagingController` previously relied
 * on exclusively.
 *
 * ## Why this exists
 *
 * `x-retry-count` only survives because `RetryPublisherService` explicitly
 * carries it forward on every retry republish. Any redelivery path that
 * does NOT go through that exact code path — an operator manually
 * requeueing a DLQ message via the RabbitMQ management UI, or a future
 * outbox-relay-originated republish of a previously-failed event — starts
 * a fresh message with no `x-retry-count` header at all, silently
 * resetting the attempt counter to zero. That turns `RETRY_CONFIG.MAX_ATTEMPTS`
 * from a true lifetime cap on a logical event into a per-incident cap,
 * which is a materially weaker guarantee than what the retry/DLQ system
 * is documented as providing.
 *
 * This table makes the cap durable and global: every delivery attempt for
 * a given `event_id`, however it was redelivered, increments the same
 * counter, and that counter — not a header — is what
 * `MessagingController.handleMessage` consults when deciding retry vs DLQ.
 *
 * ## Schema decisions
 *
 *  - `event_id` is the primary key directly (not a surrogate id +
 *    unique index) since every access pattern is a point lookup or
 *    upsert by event_id; a surrogate key would add nothing.
 *  - `attempts` starts at 0 and is incremented via an atomic
 *    `INSERT ... ON CONFLICT DO UPDATE` (see IdempotencyService /
 *    MessagingController), never read-then-written, for the same
 *    race-safety reason `processed_events`' UNIQUE constraint exists.
 *  - `updated_at` supports the same kind of TTL-purge job recommended for
 *    `processed_events` (see IdempotencyCleanupService) — rows here are
 *    just as unbounded-growth-prone and should be purged on the same
 *    retention schedule once an event's processing is fully resolved
 *    (terminal success or DLQ).
 */
export class CreateEventAttemptsTable1700000000006 implements MigrationInterface {
  name = 'CreateEventAttemptsTable1700000000006';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.createTable(
      new Table({
        name: 'event_attempts',
        columns: [
          {
            name: 'event_id',
            type: 'varchar',
            length: '36',
            isPrimary: true,
          },
          {
            name: 'attempts',
            type: 'int',
            default: 0,
            isNullable: false,
          },
          {
            name: 'updated_at',
            type: 'timestamp',
            default: 'CURRENT_TIMESTAMP',
            isNullable: false,
          },
        ],
      }),
      true,
    );

    // Supports a future TTL-purge job, same rationale as
    // IDX_processed_events_processed_at in migration 003.
    await queryRunner.createIndex(
      'event_attempts',
      new TableIndex({
        name: 'IDX_event_attempts_updated_at',
        columnNames: ['updated_at'],
      }),
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.dropIndex(
      'event_attempts',
      'IDX_event_attempts_updated_at',
    );
    await queryRunner.dropTable('event_attempts');
  }
}
