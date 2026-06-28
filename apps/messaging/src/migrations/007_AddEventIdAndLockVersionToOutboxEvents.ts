import {
  MigrationInterface,
  QueryRunner,
  TableColumn,
  TableIndex,
} from 'typeorm';

/**
 * Migration 007 — outbox_events: event_id + lock_version
 *
 * Adds two independent fixes to `outbox_events`:
 *
 * ## event_id
 *
 * Previously, an outbox row's only identity was its surrogate primary
 * key `id`, which is meaningless to a downstream consumer's idempotency
 * check (it's an internal implementation detail of this table, not a
 * stable logical event identifier). `event_id` is generated once, at row
 * creation (see `OutboxTransactionService.runWithOutboxEvents`), and
 * reused unchanged across every retry or operator-triggered replay of
 * that same row (`OutboxAdminService.replayById`/`replayAllFailed`
 * reset `status`/`attempts`, never `event_id`). This is what lets a
 * downstream consumer's `IdempotencyService` correctly recognize "this is
 * the Nth delivery attempt of the same logical event" even when the
 * *attempt* came from a relay replay rather than an AMQP-level redelivery.
 *
 * Nullable at the column level (existing rows created before this
 * migration have no event_id and are not backfilled — they predate the
 * guarantee this column provides and are assumed to have already reached
 * a terminal state by the time this migration runs in any real
 * deployment), but enforced unique whenever present.
 *
 * ## lock_version
 *
 * A fencing token for the relay's claim/publish/markSent cycle.
 * `OutboxRelayService.claimBatch()` increments this on every successful
 * claim; `markSent()` now requires the caller to present the
 * `lock_version` it claimed with, and only succeeds if it still matches.
 *
 * This closes a real race: `reapStaleLocks()` clears any lock older than
 * `OUTBOX_LOCK_TTL_MS` (default 60s) unconditionally, on the assumption
 * that the original claimant must be dead. If the claimant is merely
 * slow (GC pause, slow network to RabbitMQ) rather than dead, a second
 * relay instance can claim and publish the same row while the first is
 * still mid-publish — a genuine double-publish, not a theoretical one.
 * With a fencing token, the original (now-stale) claimant's eventual
 * `markSent()` call fails its `WHERE lock_version = $expected` check
 * (because the reaper-then-reclaimer sequence already bumped it) and is
 * treated as a detectable, logged no-op instead of silently succeeding
 * after the fact.
 */
export class AddEventIdAndLockVersionToOutboxEvents1700000000007 implements MigrationInterface {
  name = 'AddEventIdAndLockVersionToOutboxEvents1700000000007';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.addColumn(
      'outbox_events',
      new TableColumn({
        name: 'event_id',
        type: 'varchar',
        length: '36',
        isNullable: true,
      }),
    );

    await queryRunner.addColumn(
      'outbox_events',
      new TableColumn({
        name: 'lock_version',
        type: 'int',
        isNullable: false,
        default: 0,
      }),
    );

    // Partial unique index: only enforced where event_id is present, so
    // pre-migration rows (event_id IS NULL) never collide with each other.
    await queryRunner.query(`
      CREATE UNIQUE INDEX IDX_outbox_events_event_id
      ON outbox_events (event_id)
      WHERE event_id IS NOT NULL
    `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP INDEX IDX_outbox_events_event_id`);
    await queryRunner.dropColumn('outbox_events', 'lock_version');
    await queryRunner.dropColumn('outbox_events', 'event_id');
  }
}
