import { MigrationInterface, QueryRunner, Table, TableIndex } from 'typeorm';

/**
 * Migration 004 — outbox_events
 *
 * Transactional outbox ledger backing the `outbox_pending_events`
 * Prometheus gauge. See `OutboxEvent` entity for full field semantics.
 */
export class CreateOutboxEventsTable1700000000004 implements MigrationInterface {
  name = 'CreateOutboxEventsTable1700000000004';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.createTable(
      new Table({
        name: 'outbox_events',
        columns: [
          {
            name: 'id',
            type: 'integer',
            isPrimary: true,
            isGenerated: true,
            generationStrategy: 'increment',
          },
          {
            name: 'event_type',
            type: 'varchar',
            length: '255',
            isNullable: false,
          },
          {
            name: 'payload',
            type: 'jsonb',
            isNullable: false,
          },
          {
            name: 'correlation_id',
            type: 'varchar',
            length: '36',
            isNullable: true,
          },
          {
            name: 'status',
            type: 'varchar',
            length: '16',
            default: "'pending'",
            isNullable: false,
          },
          {
            name: 'attempts',
            type: 'int',
            default: 0,
            isNullable: false,
          },
          {
            name: 'last_error',
            type: 'text',
            isNullable: true,
          },
          {
            name: 'created_at',
            type: 'timestamp',
            default: 'CURRENT_TIMESTAMP',
            isNullable: false,
          },
          {
            name: 'sent_at',
            type: 'timestamp',
            isNullable: true,
          },
        ],
      }),
      true,
    );

    await queryRunner.createIndex(
      'outbox_events',
      new TableIndex({
        name: 'IDX_outbox_events_status',
        columnNames: ['status'],
      }),
    );

    await queryRunner.createIndex(
      'outbox_events',
      new TableIndex({
        name: 'IDX_outbox_events_event_type',
        columnNames: ['event_type'],
      }),
    );

    await queryRunner.createIndex(
      'outbox_events',
      new TableIndex({
        name: 'IDX_outbox_events_correlation_id',
        columnNames: ['correlation_id'],
      }),
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.dropIndex(
      'outbox_events',
      'IDX_outbox_events_correlation_id',
    );
    await queryRunner.dropIndex(
      'outbox_events',
      'IDX_outbox_events_event_type',
    );
    await queryRunner.dropIndex('outbox_events', 'IDX_outbox_events_status');
    await queryRunner.dropTable('outbox_events');
  }
}
