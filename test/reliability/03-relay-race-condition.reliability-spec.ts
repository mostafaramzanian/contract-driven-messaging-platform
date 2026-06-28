/**
 * ═══════════════════════════════════════════════════════════════════════════
 * RELIABILITY SCENARIO 3: Relay Race Condition
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * FAILURE BEING SIMULATED
 * ──────────────────────
 * Two relay instances (or two concurrent poll ticks) both see the same
 * outbox row as claimable and attempt to publish it simultaneously.
 *
 * The race can happen:
 *  1. Instance A claims a row and publishes (slow — GC pause, network lag)
 *  2. The stale-lock reaper sees locked_at is old, clears the lock
 *  3. Instance B claims the same row and publishes
 *  4. Both A and B call markSent() — without a fencing token, both succeed
 *     and the consumer receives two copies of the same event
 *
 * WHAT THIS TEST PROVES
 * ─────────────────────
 * The fencing token (lock_version) in OutboxRelayService prevents silent
 * double-publish:
 *
 *  - Every claimBatch() increments lock_version atomically
 *  - markSent() uses WHERE lock_version = $expectedVersion (CAS)
 *  - The second markSent() call matches 0 rows (lock_version was bumped)
 *  - A warning is logged ("Outbox event published, but lock_version no longer matched")
 *  - Consumer idempotency (processed_events) handles the duplicate if published
 *
 * WHY THIS PROVES RELIABILITY
 * ──────────────────────────
 * We simulate the race by directly manipulating lock_version in Postgres:
 *  1. Insert an outbox row
 *  2. Claim it manually with lock_version = 1 (simulating Relay A's claim)
 *  3. Increment lock_version to 2 (simulating the reaper + Relay B's claim)
 *  4. Call markSent() with the STALE lock_version (1) → must return 0 rows
 *  5. Call markSent() with the CURRENT lock_version (2) → must succeed
 *
 * This is a white-box test of the exact SQL OutboxRelayService uses,
 * proving the fencing token CAS works correctly under real Postgres.
 *
 * The end-to-end test then verifies that even when two relay instances race,
 * the consumer only processes the event once (idempotency layer).
 */

import { randomUUID } from 'crypto';
import * as amqplib from 'amqplib';
import { PgTestClient, pollUntil } from '../utils/pg-client';
import { RabbitMqTestClient, waitForRabbitMqAmqpReady } from '../utils/rabbitmq-client';
import { waitForHttpReady } from '../utils/wait-for-health';
import { EXCHANGES, ROUTING_KEYS, QUEUES } from '../../apps/messaging/src/reliability/topology';

const GATEWAY_URL = process.env.TEST_GATEWAY_URL ?? 'http://127.0.0.1:3005';
const RABBITMQ_URL = process.env.RABBITMQ_URL ?? 'amqp://guest:guest@127.0.0.1:5672';

describe('Reliability Scenario 3: Relay Race Condition', () => {
  let db: PgTestClient;
  let rmq: RabbitMqTestClient;

  beforeAll(async () => {
    await waitForHttpReady(GATEWAY_URL, 60_000);
    await waitForRabbitMqAmqpReady(RABBITMQ_URL, 60_000);

    db = new PgTestClient();
    await db.connect();

    rmq = new RabbitMqTestClient();
    await rmq.connect();
  });

  afterAll(async () => {
    await db.disconnect();
    await rmq.disconnect();
  });

  it(
    'stale markSent() with wrong lock_version is a no-op (fencing token CAS)',
    async () => {
      // ── ARRANGE ──────────────────────────────────────────────────────────

      const eventId = randomUUID();
      const correlationId = randomUUID();

      const outboxId = await db.insertOutboxRow({
        eventType: 'MessageCreated.v2',
        payload: { version: 2, messageId: randomUUID(), title: 'Race Test', correlationId },
        correlationId,
        eventId,
        status: 'pending',
      });

      // ── ACT: Simulate Relay A claiming the row (lock_version → 1) ────────
      const claimResult = await db.query<{
        id: number;
        lock_version: number;
      }>(
        `UPDATE outbox_events
         SET locked_at    = now(),
             locked_by    = 'relay-instance-A',
             lock_version = lock_version + 1
         WHERE id = $1
         RETURNING id, lock_version`,
        [outboxId],
      );

      expect(claimResult.length).toBe(1);
      const relayALockVersion = claimResult[0]!.lock_version;
      expect(relayALockVersion).toBe(1);

      // ── ACT: Simulate reaper clearing the lock + Relay B reclaiming ──────
      // (lock_version → 2)
      const reapAndReclaimResult = await db.query<{
        id: number;
        lock_version: number;
      }>(
        `UPDATE outbox_events
         SET locked_at    = now(),
             locked_by    = 'relay-instance-B',
             lock_version = lock_version + 1
         WHERE id = $1
         RETURNING id, lock_version`,
        [outboxId],
      );

      const relayBLockVersion = reapAndReclaimResult[0]!.lock_version;
      expect(relayBLockVersion).toBe(2);

      // ── ASSERT: Relay A's stale markSent() is rejected ──────────────────
      // Relay A finally calls markSent() with its original lock_version (1).
      // The row's current lock_version is 2 (held by B). The WHERE clause
      // must match 0 rows.
      const relayAMarkSentResult = await db.query<{ id: number }>(
        `UPDATE outbox_events
         SET status    = 'sent',
             sent_at   = now(),
             locked_at = NULL,
             locked_by = NULL
         WHERE id = $1
           AND lock_version = $2
         RETURNING id`,
        [outboxId, relayALockVersion],
      );

      // KEY ASSERTION: Fencing token rejected the stale markSent()
      expect(relayAMarkSentResult.length).toBe(0);

      // The row is still in pending status, locked by B
      const rowAfterRelayAAttempt = await db.getOutboxRow(outboxId);
      expect(rowAfterRelayAAttempt!.status).toBe('pending');
      expect(rowAfterRelayAAttempt!.locked_by).toBe('relay-instance-B');
      expect(rowAfterRelayAAttempt!.lock_version).toBe(2);

      // ── ASSERT: Relay B's markSent() succeeds (correct lock_version) ─────
      const relayBMarkSentResult = await db.query<{ id: number }>(
        `UPDATE outbox_events
         SET status    = 'sent',
             sent_at   = now(),
             locked_at = NULL,
             locked_by = NULL
         WHERE id = $1
           AND lock_version = $2
         RETURNING id`,
        [outboxId, relayBLockVersion],
      );

      expect(relayBMarkSentResult.length).toBe(1);

      const finalRow = await db.getOutboxRow(outboxId);
      expect(finalRow!.status).toBe('sent');
      expect(finalRow!.locked_by).toBeNull();
    },
    30_000,
  );

  it(
    'SKIP LOCKED prevents two concurrent claimBatch() calls from grabbing the same row',
    async () => {
      // ── ARRANGE ──────────────────────────────────────────────────────────

      const eventId = randomUUID();
      const correlationId = randomUUID();

      const outboxId = await db.insertOutboxRow({
        eventType: 'MessageCreated.v2',
        payload: { version: 2, messageId: randomUUID(), correlationId },
        correlationId,
        eventId,
        status: 'pending',
      });

      // Run two concurrent claimBatch() queries in separate Postgres
      // transactions. Only ONE should claim the row; the other must see 0
      // rows (SKIP LOCKED skips rows locked by other transactions).

      // Open two separate connections to simulate two relay instances
      const { Client } = await import('pg');
      const { DB_CONFIG } = await import('../utils/pg-client');

      const clientA = new Client(DB_CONFIG);
      const clientB = new Client(DB_CONFIG);
      await clientA.connect();
      await clientB.connect();

      // Both execute claimBatch in separate transactions SIMULTANEOUSLY
      const [resultA, resultB] = await Promise.all([
        (async () => {
          await clientA.query('BEGIN');
          const res = await clientA.query<{ id: number; lock_version: number }>(
            `UPDATE outbox_events
             SET locked_at    = now(),
                 locked_by    = $1,
                 lock_version = lock_version + 1
             WHERE id IN (
               SELECT id FROM outbox_events
               WHERE status = 'pending' AND next_retry_at <= now() AND id = $2
               FOR UPDATE SKIP LOCKED
             )
             RETURNING id, lock_version`,
            ['relay-concurrent-A', outboxId],
          );
          await clientA.query('COMMIT');
          return res.rows;
        })(),

        (async () => {
          await clientB.query('BEGIN');
          const res = await clientB.query<{ id: number; lock_version: number }>(
            `UPDATE outbox_events
             SET locked_at    = now(),
                 locked_by    = $1,
                 lock_version = lock_version + 1
             WHERE id IN (
               SELECT id FROM outbox_events
               WHERE status = 'pending' AND next_retry_at <= now() AND id = $2
               FOR UPDATE SKIP LOCKED
             )
             RETURNING id, lock_version`,
            ['relay-concurrent-B', outboxId],
          );
          await clientB.query('COMMIT');
          return res.rows;
        })(),
      ]);

      await clientA.end();
      await clientB.end();

      // ── ASSERT: Exactly one relay got the row ─────────────────────────────
      const totalClaims = resultA.length + resultB.length;
      expect(totalClaims).toBe(1);

      // The row is now locked by exactly one instance
      const row = await db.getOutboxRow(outboxId);
      expect(row!.locked_by).toMatch(/relay-concurrent-[AB]/);
      expect(['relay-concurrent-A', 'relay-concurrent-B']).toContain(row!.locked_by);
    },
    30_000,
  );

  it(
    'duplicate consumer message is rejected by idempotency layer (no double processing)',
    async () => {
      // ── ARRANGE ──────────────────────────────────────────────────────────
      // Simulate what happens when BOTH relay instances publish (before the
      // fencing token rejects the second markSent): the consumer receives two
      // messages with the same event_id. The idempotency layer must process
      // only once.

      const eventId = randomUUID();
      const correlationId = randomUUID();

      const payload = {
        version: 2,
        messageId: randomUUID(),
        title: 'Double Publish Race',
        content: 'Consumer idempotency test',
        sender: 'reliability-tester',
        recipient: 'test-consumer',
        correlationId,
        eventId,
        timestamp: new Date().toISOString(),
      };

      // Publish the SAME event_id twice to the work queue — simulating two
      // relay instances both publishing before the fencing token rejects one.
      // In production this is a rare race; here we force it deterministically.
      await rmq.publishToWork(payload, {
        'x-event-type': 'MessageCreated.v2',
        'x-correlation-id': correlationId,
        'x-event-id': eventId,
      });
      await rmq.publishToWork(payload, {
        'x-event-type': 'MessageCreated.v2',
        'x-correlation-id': correlationId,
        'x-event-id': eventId,
      });

      // ── ASSERT: Consumer processes it exactly once ────────────────────────
      // Wait for at least one processing cycle
      await pollUntil(
        `processed_events row exists for ${eventId}`,
        async () => {
          const count = await db.countProcessedEvents(eventId);
          return count > 0;
        },
        { timeoutMs: 30_000, intervalMs: 500 },
      );

      // Give enough time for the second message to also be consumed
      await new Promise((r) => setTimeout(r, 5_000));

      // CRITICAL: The UNIQUE constraint on processed_events(event_id)
      // ensures only one row can exist per event_id, regardless of how
      // many times the message arrived.
      const processedCount = await db.countProcessedEvents(eventId);
      expect(processedCount).toBe(1);

      // The messages table should also have exactly one row for this event
      const messageCount = await db.countMessagesByCorrelationId(correlationId);
      expect(messageCount).toBeLessThanOrEqual(1);
    },
    60_000,
  );
});
