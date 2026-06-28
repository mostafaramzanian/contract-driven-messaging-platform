/**
 * ═══════════════════════════════════════════════════════════════════════════
 * RELIABILITY SCENARIO 5: DLQ Recovery
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * FAILURE BEING SIMULATED
 * ──────────────────────
 * An event has been exhausted through its retry budget (attempts >= max_attempts)
 * and has landed in the Dead Letter Queue (DLQ). An operator triggers a
 * manual replay via the outbox admin API:
 *   POST /internal/outbox/:id/replay
 *
 * This resets the outbox row to 'pending' (attempts = 0) so it re-enters
 * the normal relay cycle.
 *
 * WHAT THIS TEST PROVES
 * ─────────────────────
 *  1. The replay correctly transitions a 'failed' outbox row back to 'pending'
 *  2. The relay picks up the replayed row and publishes it
 *  3. If the consumer already processed the event (exists in processed_events),
 *     the idempotency layer prevents it from being processed a second time
 *  4. If the consumer never processed it (e.g. it was an outbox-level failure
 *     before the message ever reached the consumer), it is processed exactly once
 *  5. No duplicate entries appear in processed_events or messages
 *
 * WHY THIS PROVES RELIABILITY
 * ──────────────────────────
 * DLQ replay is an operator-triggered recovery action. It MUST be safe to
 * replay: the worst case is a message that was already processed — the
 * idempotency layer must reject the duplicate, not process it twice.
 *
 * The test exercises the full recovery cycle:
 *   Failed outbox row → admin replay → relay publish → consumer
 *   → idempotency check → exactly-once processing
 */

import { randomUUID } from 'crypto';
import request from 'supertest';
import { PgTestClient, pollUntil } from '../utils/pg-client';
import { RabbitMqTestClient, waitForRabbitMqAmqpReady } from '../utils/rabbitmq-client';
import { waitForHttpReady } from '../utils/wait-for-health';

const GATEWAY_URL = process.env.TEST_GATEWAY_URL ?? 'http://127.0.0.1:3005';
const RABBITMQ_URL = process.env.RABBITMQ_URL ?? 'amqp://guest:guest@127.0.0.1:5672';
const MESSAGING_INTERNAL_URL = process.env.MESSAGING_HEALTH_URL ?? 'http://127.0.0.1:3006';

describe('Reliability Scenario 5: DLQ Recovery', () => {
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
    'replayed outbox row eventually reaches sent status and consumer processes it exactly once',
    async () => {
      // ── ARRANGE: Create a failed outbox row ───────────────────────────────

      const eventId = randomUUID();
      const correlationId = randomUUID();

      const outboxId = await db.insertOutboxRow({
        eventType: 'MessageCreated.v2',
        payload: {
          version: 2,
          messageId: randomUUID(),
          title: 'DLQ Recovery Test',
          content: 'This event should be replayed and delivered exactly once',
          sender: 'reliability-tester',
          recipient: 'test-consumer',
          correlationId,
          eventId,
          timestamp: new Date().toISOString(),
        },
        correlationId,
        eventId,
        status: 'pending',
        attempts: 0,
        maxAttempts: 5,
      });

      // Force the row to 'failed' state (simulating exhausted retries)
      await db.forceOutboxFailed(
        outboxId,
        'Simulated: all 5 publish attempts failed — transient broker error',
      );

      const failedRow = await db.getOutboxRow(outboxId);
      expect(failedRow!.status).toBe('failed');
      expect(failedRow!.attempts).toBe(5);

      // ── ACT: Trigger replay via the outbox admin HTTP endpoint ────────────
      const replayResponse = await request(MESSAGING_INTERNAL_URL)
        .post(`/internal/outbox/${outboxId}/replay`)
        .set('x-internal-api-key', process.env.INTERNAL_API_KEY ?? 'test-internal-key')
        .expect(200);

      expect(replayResponse.body).toMatchObject({
        replayed: 1,
        ids: [outboxId],
      });

      // ── ASSERT: Row resets to pending ─────────────────────────────────────
      const replayedRow = await db.getOutboxRow(outboxId);
      expect(replayedRow!.status).toBe('pending');
      expect(replayedRow!.attempts).toBe(0);
      expect(replayedRow!.locked_at).toBeNull();
      expect(replayedRow!.locked_by).toBeNull();

      // The event_id must be preserved — this is what makes the replay
      // idempotent on the consumer side. Same event_id → same idempotency key.
      expect(replayedRow!.event_id).toBe(eventId);

      // ── ASSERT: Relay picks up the replayed row and publishes it ──────────
      await pollUntil(
        `replayed outbox row ${outboxId} reaches 'sent'`,
        async () => {
          const row = await db.getOutboxRow(outboxId);
          return row?.status === 'sent';
        },
        { timeoutMs: 60_000, intervalMs: 500 },
      );

      const sentRow = await db.getOutboxRow(outboxId);
      expect(sentRow!.status).toBe('sent');
      expect(sentRow!.sent_at).not.toBeNull();

      // ── ASSERT: Consumer processes the event exactly once ─────────────────
      // Wait for the messaging service to consume and process the event
      await pollUntil(
        `event ${eventId} appears in processed_events`,
        async () => {
          const count = await db.countProcessedEvents(eventId);
          return count > 0;
        },
        { timeoutMs: 30_000, intervalMs: 500 },
      );

      // Give extra time for any potential duplicate to arrive and be processed
      await new Promise((r) => setTimeout(r, 3_000));

      // EXACTLY ONE processed_events row (unique constraint enforcement)
      const processedCount = await db.countProcessedEvents(eventId);
      expect(processedCount).toBe(1);
    },
    120_000,
  );

  it(
    'replaying an already-processed event is idempotent — consumer ignores duplicate',
    async () => {
      // ── ARRANGE: Create a row that was ALREADY processed (processed_events exists) ──

      const eventId = randomUUID();
      const correlationId = randomUUID();

      // Pre-insert into processed_events (simulating the consumer already
      // processed this event in a previous run)
      await db.query(
        `INSERT INTO processed_events (event_id, event_type, correlation_id, processed_at)
         VALUES ($1, $2, $3, now())`,
        [eventId, 'MessageCreated.v2', correlationId],
      );

      // Also ensure a message row exists (business write already happened)
      await db.query(
        `INSERT INTO messages (title, content, sender, recipient, correlation_id)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT DO NOTHING`,
        ['Already Processed', 'Already done', 'reliability-tester', 'test-consumer', correlationId],
      );

      // Create an outbox row in 'failed' state for this already-processed event
      const outboxId = await db.insertOutboxRow({
        eventType: 'MessageCreated.v2',
        payload: {
          version: 2,
          messageId: randomUUID(),
          title: 'Already Processed',
          content: 'Already done',
          sender: 'reliability-tester',
          recipient: 'test-consumer',
          correlationId,
          eventId,
          timestamp: new Date().toISOString(),
        },
        correlationId,
        eventId,
        status: 'pending', // insert as pending first
      });
      await db.forceOutboxFailed(outboxId, 'Simulated failure after successful processing');

      // ── ACT: Replay the failed outbox row ────────────────────────────────
      const replayResponse = await request(MESSAGING_INTERNAL_URL)
        .post(`/internal/outbox/${outboxId}/replay`)
        .set('x-internal-api-key', process.env.INTERNAL_API_KEY ?? 'test-internal-key')
        .expect(200);

      expect(replayResponse.body.replayed).toBe(1);

      // Wait for relay to publish it (outbox row reaches 'sent')
      await pollUntil(
        `replayed already-processed outbox row ${outboxId} sent`,
        async () => {
          const row = await db.getOutboxRow(outboxId);
          return row?.status === 'sent';
        },
        { timeoutMs: 60_000, intervalMs: 500 },
      );

      // ── ASSERT: Consumer encounters duplicate → idempotency rejects it ─────
      // Wait for consumer to attempt processing (it will find the existing
      // processed_events row and reject the duplicate)
      await new Promise((r) => setTimeout(r, 5_000));

      // CRITICAL: Still exactly ONE processed_events row — idempotency worked
      const processedCount = await db.countProcessedEvents(eventId);
      expect(processedCount).toBe(1);

      // CRITICAL: Still exactly ONE messages row — no duplicate business write
      const msgCount = await db.countMessagesByCorrelationId(correlationId);
      expect(msgCount).toBe(1);
    },
    90_000,
  );

  it(
    'bulk replay-failed resets all failed rows and they eventually reach sent',
    async () => {
      // ── ARRANGE: Create multiple failed outbox rows ───────────────────────

      const numRows = 3;
      const outboxIds: number[] = [];
      const eventIds: string[] = [];

      for (let i = 0; i < numRows; i++) {
        const eventId = randomUUID();
        const correlationId = randomUUID();
        eventIds.push(eventId);

        const id = await db.insertOutboxRow({
          eventType: 'MessageCreated.v2',
          payload: {
            version: 2,
            messageId: randomUUID(),
            title: `Bulk Replay Test ${i + 1}`,
            content: `Bulk replay event ${i + 1}`,
            sender: 'reliability-tester',
            recipient: 'test-consumer',
            correlationId,
            eventId,
            timestamp: new Date().toISOString(),
          },
          correlationId,
          eventId,
          status: 'pending',
        });

        await db.forceOutboxFailed(id, 'Simulated bulk failure');
        outboxIds.push(id);
      }

      // ── ACT: Bulk replay ──────────────────────────────────────────────────
      const bulkReplayResponse = await request(MESSAGING_INTERNAL_URL)
        .post('/internal/outbox/replay-failed')
        .set('x-internal-api-key', process.env.INTERNAL_API_KEY ?? 'test-internal-key')
        .expect(200);

      expect(bulkReplayResponse.body.replayed).toBeGreaterThanOrEqual(numRows);

      // ── ASSERT: All rows eventually reach 'sent' ──────────────────────────
      await pollUntil(
        `all ${numRows} bulk-replayed rows reach 'sent'`,
        async () => {
          const results = await Promise.all(
            outboxIds.map((id) => db.getOutboxRow(id)),
          );
          return results.every((row) => row?.status === 'sent');
        },
        { timeoutMs: 90_000, intervalMs: 1_000 },
      );

      for (const id of outboxIds) {
        const row = await db.getOutboxRow(id);
        expect(row!.status).toBe('sent');
        expect(row!.sent_at).not.toBeNull();
      }
    },
    120_000,
  );
});
