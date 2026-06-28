import { MigrationInterface, QueryRunner, TableColumn } from 'typeorm';

/**
 * Migration 008 — messages.createdAt index + outbox_events.trace_context
 *
 * Two independent, small additions bundled into one migration since
 * neither warrants its own:
 *
 * ## messages index on createdAt
 *
 * `MessagesService.findAll()` (apps/messaging/src/messages/messages.service.ts)
 * runs `ORDER BY "createdAt" DESC` with no supporting index — the
 * `messages` table (migration 001) was created with no indexes beyond the
 * implicit primary key. At low row counts this is invisible; at
 * production scale every `findAll()` call is a full table scan plus an
 * in-memory sort. This index alone does not fix the missing pagination
 * (see `MessagesService.findAll`'s own follow-up change, which adds a
 * `LIMIT`/cursor), but an unindexed ORDER BY is wrong regardless of
 * whether pagination is added on top of it.
 *
 * ## outbox_events.trace_context
 *
 * Without a place to store it, the original producer's OpenTelemetry
 * trace/span IDs cannot survive from the transactional write (inside an
 * HTTP request's trace) to the relay's later, asynchronous publish (which
 * runs on a `setInterval` timer with no parent span at all). This column
 * is written once, at outbox-row creation time, by
 * `OutboxTransactionService.runWithOutboxEvents` (capturing whatever span
 * is active in the calling request), and read by
 * `OutboxRelayService.publishOne` to inject the *original* trace context
 * into the AMQP headers it publishes, instead of the relay's own
 * disconnected ambient context.
 */
export class AddMessagesCreatedAtIndexAndOutboxTraceContext1700000000008 implements MigrationInterface {
  name = 'AddMessagesCreatedAtIndexAndOutboxTraceContext1700000000008';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `CREATE INDEX "IDX_messages_created_at" ON "messages" ("createdAt" DESC)`,
    );

    await queryRunner.addColumn(
      'outbox_events',
      new TableColumn({
        name: 'trace_context',
        type: 'jsonb',
        isNullable: true,
      }),
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.dropColumn('outbox_events', 'trace_context');
    await queryRunner.query(`DROP INDEX "IDX_messages_created_at"`);
  }
}
