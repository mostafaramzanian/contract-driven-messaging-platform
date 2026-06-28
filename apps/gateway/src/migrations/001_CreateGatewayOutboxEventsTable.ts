import { MigrationInterface, QueryRunner, Table, TableIndex } from 'typeorm';

/**
 * Migration 001 (gateway) — gateway_outbox_events
 *
 * Producer-side transactional outbox table for the gateway service. Closes
 * CRITICAL ISSUE #1 (Producer Reliability Gap): every event the gateway
 * wants to emit is durably persisted here, inside the same Postgres
 * transaction as the HTTP-request-time write, BEFORE any attempt to reach
 * RabbitMQ is made.
 *
 * Unlike the messaging service's `outbox_events` table — which reached its
 * current shape across migrations 004 → 005 → 007 → 008 as the production-
 * readiness review progressively hardened it — this table starts directly
 * at that same, already-hardened shape in a single migration, since it is
 * a new table with no pre-existing rows or production history to migrate
 * forward incrementally. The column set is intentionally identical to
 * `outbox_events` (see `GatewayOutboxEvent` entity's doc comment) so
 * `GatewayOutboxRelayService` can reuse the exact same claim/publish/
 * markSent/reaper SQL shape as `OutboxRelayService`.
 *
 * Lives in `apps/gateway/src/migrations` (its own CLI DataSource, see
 * `apps/gateway/typeorm.config.ts`) — run via `npm run migration:run:gateway`.
 * Targets the SAME physical Postgres database (`DB_NAME`) the messaging
 * service's migrations already run against; no new database, no new
 * container, no new microservice.
 */
export class CreateGatewayOutboxEventsTable1700000000101 implements MigrationInterface {
  name = 'CreateGatewayOutboxEventsTable1700000000101';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.createTable(
      new Table({
        name: 'gateway_outbox_events',
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
          // ── Relay-locking fields (mirrors messaging migration 005) ──────
          {
            name: 'locked_at',
            type: 'timestamp',
            isNullable: true,
          },
          {
            name: 'locked_by',
            type: 'varchar',
            length: '128',
            isNullable: true,
          },
          {
            name: 'next_retry_at',
            type: 'timestamp',
            isNullable: false,
            default: 'CURRENT_TIMESTAMP',
          },
          {
            name: 'max_attempts',
            type: 'int',
            isNullable: false,
            default: 5,
          },
          // ── event_id / lock_version (mirrors messaging migration 007) ───
          {
            name: 'event_id',
            type: 'varchar',
            length: '36',
            isNullable: true,
          },
          {
            name: 'lock_version',
            type: 'int',
            isNullable: false,
            default: 0,
          },
          // ── trace_context (mirrors messaging migration 008) ─────────────
          {
            name: 'trace_context',
            type: 'jsonb',
            isNullable: true,
          },
        ],
      }),
      true,
    );

    await queryRunner.createIndex(
      'gateway_outbox_events',
      new TableIndex({
        name: 'IDX_gateway_outbox_events_status',
        columnNames: ['status'],
      }),
    );

    await queryRunner.createIndex(
      'gateway_outbox_events',
      new TableIndex({
        name: 'IDX_gateway_outbox_events_event_type',
        columnNames: ['event_type'],
      }),
    );

    await queryRunner.createIndex(
      'gateway_outbox_events',
      new TableIndex({
        name: 'IDX_gateway_outbox_events_correlation_id',
        columnNames: ['correlation_id'],
      }),
    );

    // Composite indexes for the relay's claim query and stale-lock reaper —
    // see OutboxRelayService's (messaging) equivalent migration 005 doc
    // comment for why these two specifically.
    await queryRunner.createIndex(
      'gateway_outbox_events',
      new TableIndex({
        name: 'IDX_gateway_outbox_events_status_next_retry_at',
        columnNames: ['status', 'next_retry_at'],
      }),
    );

    await queryRunner.createIndex(
      'gateway_outbox_events',
      new TableIndex({
        name: 'IDX_gateway_outbox_events_status_locked_at',
        columnNames: ['status', 'locked_at'],
      }),
    );

    // Partial unique index: event_id is unique whenever present, mirroring
    // messaging migration 007's IDX_outbox_events_event_id exactly.
    await queryRunner.query(`
      CREATE UNIQUE INDEX IDX_gateway_outbox_events_event_id
      ON gateway_outbox_events (event_id)
      WHERE event_id IS NOT NULL
    `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `DROP INDEX IF EXISTS IDX_gateway_outbox_events_event_id`,
    );
    await queryRunner.dropIndex(
      'gateway_outbox_events',
      'IDX_gateway_outbox_events_status_locked_at',
    );
    await queryRunner.dropIndex(
      'gateway_outbox_events',
      'IDX_gateway_outbox_events_status_next_retry_at',
    );
    await queryRunner.dropIndex(
      'gateway_outbox_events',
      'IDX_gateway_outbox_events_correlation_id',
    );
    await queryRunner.dropIndex(
      'gateway_outbox_events',
      'IDX_gateway_outbox_events_event_type',
    );
    await queryRunner.dropIndex(
      'gateway_outbox_events',
      'IDX_gateway_outbox_events_status',
    );
    await queryRunner.dropTable('gateway_outbox_events');
  }
}
