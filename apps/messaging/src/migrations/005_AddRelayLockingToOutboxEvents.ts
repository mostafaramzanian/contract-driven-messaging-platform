import {
  MigrationInterface,
  QueryRunner,
  TableColumn,
  TableIndex,
} from 'typeorm';

/**
 * Migration 005 — Add relay-locking fields to outbox_events
 *
 * Adds the columns required by the production OutboxRelayService:
 *
 *  locked_at     : timestamp  — when this row was claimed by a relay instance
 *  locked_by     : varchar    — relay instance-id that holds the lock (observability only)
 *  next_retry_at : timestamp  — earliest time the row may be re-claimed (NOT NULL, default now())
 *  max_attempts  : int        — per-row retry budget (default 5, matches RETRY_CONFIG.MAX_ATTEMPTS)
 *
 * Two new composite indexes speed up the relay's polling query:
 *  - (status, next_retry_at) : the primary claim filter
 *  - (status, locked_at)     : the stale-lock reaper query
 */
export class AddRelayLockingToOutboxEvents1700000000005 implements MigrationInterface {
  name = 'AddRelayLockingToOutboxEvents1700000000005';

  public async up(queryRunner: QueryRunner): Promise<void> {
    // ── New columns ──────────────────────────────────────────────────────
    await queryRunner.addColumn(
      'outbox_events',
      new TableColumn({
        name: 'locked_at',
        type: 'timestamp',
        isNullable: true,
      }),
    );

    await queryRunner.addColumn(
      'outbox_events',
      new TableColumn({
        name: 'locked_by',
        type: 'varchar',
        length: '128',
        isNullable: true,
      }),
    );

    await queryRunner.addColumn(
      'outbox_events',
      new TableColumn({
        name: 'next_retry_at',
        type: 'timestamp',
        isNullable: false,
        default: 'CURRENT_TIMESTAMP',
      }),
    );

    await queryRunner.addColumn(
      'outbox_events',
      new TableColumn({
        name: 'max_attempts',
        type: 'int',
        isNullable: false,
        default: 5,
      }),
    );

    // ── Composite indexes for relay polling and stale-lock reaping ───────
    await queryRunner.createIndex(
      'outbox_events',
      new TableIndex({
        name: 'IDX_outbox_events_status_next_retry_at',
        columnNames: ['status', 'next_retry_at'],
      }),
    );

    await queryRunner.createIndex(
      'outbox_events',
      new TableIndex({
        name: 'IDX_outbox_events_status_locked_at',
        columnNames: ['status', 'locked_at'],
      }),
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.dropIndex(
      'outbox_events',
      'IDX_outbox_events_status_locked_at',
    );
    await queryRunner.dropIndex(
      'outbox_events',
      'IDX_outbox_events_status_next_retry_at',
    );
    await queryRunner.dropColumn('outbox_events', 'max_attempts');
    await queryRunner.dropColumn('outbox_events', 'next_retry_at');
    await queryRunner.dropColumn('outbox_events', 'locked_by');
    await queryRunner.dropColumn('outbox_events', 'locked_at');
  }
}
