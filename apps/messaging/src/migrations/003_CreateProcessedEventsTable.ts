import { MigrationInterface, QueryRunner, Table, TableIndex } from 'typeorm';

/**
 * Migration 003 — processed_events
 *
 * Creates the idempotency ledger.  Every time the messaging service
 * successfully processes an event it writes a row here keyed on event_id.
 * Before processing any event the controller checks this table; a hit
 * means the event was already processed and the current delivery is a
 * duplicate (broker retry or at-least-once delivery guarantee).
 *
 * Schema decisions:
 *  - event_id is VARCHAR(36) matching UUID v4 wire format; NOT a PG uuid
 *    type so we never need a cast when reading from AMQP headers.
 *  - The UNIQUE constraint on event_id is the correctness guarantee; the
 *    primary key is a surrogate for cheap FK references in future tables.
 *  - result JSONB allows storing a compact summary of what was produced
 *    (e.g. the persisted Message id) so callers can return a cached
 *    response on duplicate without re-querying the messages table.
 *  - processed_at defaults to NOW() so rows are self-describing for
 *    operational dashboards without requiring the application to supply
 *    the timestamp.
 *  - An index on correlation_id supports correlation-scoped queries
 *    (e.g. "did every event in this request chain land exactly once?").
 *  - An index on processed_at supports TTL purge jobs without a full
 *    table scan.
 */
export class CreateProcessedEventsTable1700000000003 implements MigrationInterface {
  name = 'CreateProcessedEventsTable1700000000003';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.createTable(
      new Table({
        name: 'processed_events',
        columns: [
          {
            name: 'id',
            type: 'integer',
            isPrimary: true,
            isGenerated: true,
            generationStrategy: 'increment',
          },
          {
            // UUID string from the event envelope (36 chars, no cast needed)
            name: 'event_id',
            type: 'varchar',
            length: '36',
            isNullable: false,
            isUnique: true, // ← idempotency guarantee
          },
          {
            name: 'event_type',
            type: 'varchar',
            length: '255',
            isNullable: false,
          },
          {
            name: 'correlation_id',
            type: 'varchar',
            length: '36',
            isNullable: true,
          },
          {
            // Optional serialised summary of the processing result
            // (e.g. { messageId: 42 }). Stored for cached-response use.
            name: 'result',
            type: 'jsonb',
            isNullable: true,
          },
          {
            name: 'processed_at',
            type: 'timestamp',
            default: 'CURRENT_TIMESTAMP',
            isNullable: false,
          },
        ],
      }),
      true, // ifNotExists
    );

    // Secondary index on correlation_id for correlation-scoped queries
    await queryRunner.createIndex(
      'processed_events',
      new TableIndex({
        name: 'IDX_processed_events_correlation_id',
        columnNames: ['correlation_id'],
      }),
    );

    // Secondary index on processed_at to support TTL purge without full scan
    await queryRunner.createIndex(
      'processed_events',
      new TableIndex({
        name: 'IDX_processed_events_processed_at',
        columnNames: ['processed_at'],
      }),
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.dropIndex(
      'processed_events',
      'IDX_processed_events_processed_at',
    );
    await queryRunner.dropIndex(
      'processed_events',
      'IDX_processed_events_correlation_id',
    );
    await queryRunner.dropTable('processed_events');
  }
}
