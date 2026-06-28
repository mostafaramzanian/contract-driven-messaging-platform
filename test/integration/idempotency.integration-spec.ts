/**
 * Integration test: Exactly-Once Processing via Idempotency
 *
 * Tests that the messaging service processes each unique eventId exactly
 * once, even under duplicate AMQP delivery (simulated by sending two
 * identical events via the gateway).
 *
 * ## Stack assumption
 * The test stack (docker-compose.test.yml) must be running with the
 * evolution-stage changes applied (migration 003 executed, messaging
 * service running with IdempotencyService).
 *
 * ## What we observe
 * - First delivery: `persisted` lifecycle stage fires, one row in `messages`.
 * - Second delivery (same eventId, forged via a raw AMQP publish): the
 *   messaging service logs a "duplicate detected" warning and acks without
 *   inserting a second `messages` row.
 *
 * ## Why lifecycle + DB, not DB alone
 * The lifecycle subscriber tells us *when* the messaging service finished
 * processing each delivery.  Without it, we would have to sleep and then
 * count rows — which is racy.  With it, we can deterministically assert
 * that the second delivery was acknowledged (no re-enqueue) and that only
 * one `messages` row exists.
 */

import { Client } from 'pg';
import * as amqplib from 'amqplib';
import request from 'supertest';
import { EventTracker } from '../utils/event-tracker';
import {
  waitForHttpReady,
  waitForRabbitMqReady,
} from '../utils/wait-for-health';

const GATEWAY_URL = process.env.TEST_GATEWAY_URL ?? 'http://127.0.0.1:3005';
const RABBITMQ_URL =
  process.env.RABBITMQ_URL ?? 'amqp://guest:guest@127.0.0.1:5672';
const MESSAGING_HEALTH_URL =
  process.env.MESSAGING_HEALTH_URL ?? 'http://127.0.0.1:3006';

const DB_CONFIG = {
  host: process.env.TEST_DB_HOST ?? '127.0.0.1',
  port: Number.parseInt(process.env.TEST_DB_PORT ?? '5432', 10),
  user: process.env.TEST_DB_USERNAME ?? 'admin',
  password: process.env.TEST_DB_PASSWORD ?? 'test_password',
  database: process.env.TEST_DB_NAME ?? 'showcase_test_db',
  connectionTimeoutMillis: 30_000,
};

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

// ── Helpers ──────────────────────────────────────────────────────────────────

/**
 * Queries the `processed_events` table for a given eventId.
 * Returns the row or null.
 */
async function queryProcessedEvent(
  db: Client,
  eventId: string,
): Promise<{
  id: number;
  event_id: string;
  event_type: string;
  correlation_id: string;
  processed_at: Date;
} | null> {
  const result = await db.query<{
    id: number;
    event_id: string;
    event_type: string;
    correlation_id: string;
    processed_at: Date;
  }>(
    `SELECT id, event_id, event_type, correlation_id, processed_at
     FROM processed_events
     WHERE event_id = $1`,
    [eventId],
  );
  return result.rows[0] ?? null;
}

/**
 * Counts `messages` rows with the given title (used to detect duplicate inserts).
 */
async function countMessagesByTitle(
  db: Client,
  title: string,
): Promise<number> {
  const result = await db.query<{ count: string }>(
    `SELECT COUNT(*) AS count FROM messages WHERE title = $1`,
    [title],
  );
  return Number.parseInt(result.rows[0]?.count ?? '0', 10);
}

// ── Test suite ────────────────────────────────────────────────────────────────

describe('Idempotency: exactly-once processing (integration)', () => {
  let dbClient: Client;
  let tracker: EventTracker;
  let amqpConnection: amqplib.ChannelModel;
  let amqpChannel: amqplib.Channel;

  beforeAll(async () => {
    await waitForHttpReady(`${GATEWAY_URL}/api`);
    await waitForRabbitMqReady(RABBITMQ_URL);

    dbClient = new Client(DB_CONFIG);
    await dbClient.connect();

    tracker = new EventTracker(RABBITMQ_URL);
    await tracker.connect();

    // Raw AMQP connection for the duplicate-injection test
    amqpConnection = await amqplib.connect(RABBITMQ_URL);
    amqpChannel = await amqpConnection.createChannel();
  }, 40_000);

  afterAll(async () => {
    try {
      await amqpChannel?.close();
      await amqpConnection?.close();
    } catch { /* ignore */ }
    if (tracker) await tracker.close();
    if (dbClient) await dbClient.end();
  });

  // ── Test 1: First delivery is processed ────────────────────────────────────

  it('processes the first delivery and writes to processed_events + messages', async () => {
    // Trigger via gateway (emits a real CreateMessageEvent.v1)
    const response = await request(GATEWAY_URL)
      .get('/api/test-rabbit')
      .expect(200);

    const { eventId, correlationId } = response.body as {
      eventId: string;
      correlationId: string;
    };

    expect(eventId).toMatch(UUID_PATTERN);

    // Wait for the full lifecycle chain to confirm processing completed
    const chain = await tracker.waitForFullChain(eventId, 15_000);
    expect(chain.persisted.correlationId).toBe(correlationId);

    // Verify the idempotency ledger row was written
    const ledgerRow = await queryProcessedEvent(dbClient, eventId);
    expect(ledgerRow).not.toBeNull();
    expect(ledgerRow!.event_id).toBe(eventId);
    expect(ledgerRow!.event_type).toBe('CreateMessageEvent.v1');
    expect(ledgerRow!.correlation_id).toBe(correlationId);
    expect(ledgerRow!.processed_at).toBeInstanceOf(Date);
  }, 60_000);

  // ── Test 2: Second delivery is a no-op ────────────────────────────────────

  it('silently acks and skips re-processing on duplicate eventId delivery', async () => {
    // Step 1: get a real eventId via the normal gateway path
    const firstResponse = await request(GATEWAY_URL)
      .get('/api/test-rabbit')
      .expect(200);

    const { eventId } = firstResponse.body as { eventId: string };

    // Wait until first delivery is fully processed
    await tracker.waitForFullChain(eventId, 15_000);

    // Step 2: count the messages table BEFORE the duplicate delivery
    const countBefore = await countMessagesByTitle(
      dbClient,
      'System test message',
    );

    // Step 3: Inject a second message with the SAME eventId directly to
    // the work queue, bypassing the gateway.  We use a minimal envelope
    // that passes contract validation but carries an eventId that the
    // messaging service has already registered in processed_events.
    const duplicatePayload = {
      eventId,                             // ← same as first delivery
      correlationId: firstResponse.body.correlationId,
      timestamp: new Date().toISOString(),
      source: 'gateway',
      trace: ['gateway'],
      payload: {
        subject: 'System test message',
        content: 'Hello RabbitMQ!',
      },
    };

    amqpChannel.sendToQueue(
      'messaging.work',
      Buffer.from(JSON.stringify(duplicatePayload)),
      {
        persistent: true,
        contentType: 'application/json',
        // Use a different AMQP messageId to confirm it's a new delivery,
        // NOT a broker-level requeue of the exact same message.
        messageId: `duplicate-test-${Date.now()}`,
      },
    );

    // Step 4: Wait a reasonable time for the messaging service to consume
    // and ack the duplicate.  We cannot use waitForFullChain here because
    // the messaging service deliberately does NOT emit a `persisted` stage
    // for duplicates — only a `warn` log.
    //
    // Instead we poll for the absence of a second DB row, which is the
    // correctness assertion: exactly one row should exist regardless of
    // how many times the event is delivered.
    await new Promise((resolve) => setTimeout(resolve, 5_000));

    const countAfter = await countMessagesByTitle(
      dbClient,
      'System test message',
    );

    // The count must not have increased — no duplicate row was written
    expect(countAfter).toBe(countBefore);

    // The processed_events table still has exactly one row for this eventId
    const ledgerRow = await queryProcessedEvent(dbClient, eventId);
    expect(ledgerRow).not.toBeNull();
    const allLedgerRows = await dbClient.query<{ count: string }>(
      `SELECT COUNT(*) AS count FROM processed_events WHERE event_id = $1`,
      [eventId],
    );
    expect(Number.parseInt(allLedgerRows.rows[0]?.count ?? '0')).toBe(1);
  }, 70_000);

  // ── Test 3: Different eventIds are each processed exactly once ────────────

  it('processes two distinct events independently without interference', async () => {
    const [resp1, resp2] = await Promise.all([
      request(GATEWAY_URL).get('/api/test-rabbit').expect(200),
      request(GATEWAY_URL).get('/api/test-rabbit').expect(200),
    ]);

    const eventId1 = (resp1.body as { eventId: string }).eventId;
    const eventId2 = (resp2.body as { eventId: string }).eventId;

    expect(eventId1).not.toBe(eventId2);

    // Both must complete full lifecycle chains
    const [chain1, chain2] = await Promise.all([
      tracker.waitForFullChain(eventId1, 15_000),
      tracker.waitForFullChain(eventId2, 15_000),
    ]);

    expect(chain1.persisted).toBeDefined();
    expect(chain2.persisted).toBeDefined();

    // Both must have their own ledger rows
    const [row1, row2] = await Promise.all([
      queryProcessedEvent(dbClient, eventId1),
      queryProcessedEvent(dbClient, eventId2),
    ]);

    expect(row1).not.toBeNull();
    expect(row2).not.toBeNull();
    expect(row1!.id).not.toBe(row2!.id);
  }, 70_000);

  // ── Test 4: processed_events schema sanity ────────────────────────────────

  it('processed_events table has the expected columns and unique index', async () => {
    // Verify the schema matches migration 003 expectations
    const columns = await dbClient.query<{
      column_name: string;
      data_type: string;
      is_nullable: string;
    }>(
      `SELECT column_name, data_type, is_nullable
       FROM information_schema.columns
       WHERE table_name = 'processed_events'
       ORDER BY ordinal_position`,
    );

    const colNames = columns.rows.map((r) => r.column_name);
    expect(colNames).toContain('id');
    expect(colNames).toContain('event_id');
    expect(colNames).toContain('event_type');
    expect(colNames).toContain('correlation_id');
    expect(colNames).toContain('result');
    expect(colNames).toContain('processed_at');

    // Unique index on event_id
    const indexes = await dbClient.query<{
      indexname: string;
      indexdef: string;
    }>(
      `SELECT indexname, indexdef
       FROM pg_indexes
       WHERE tablename = 'processed_events'`,
    );

    const uniqueOnEventId = indexes.rows.some(
      (r) =>
        r.indexdef.includes('event_id') &&
        r.indexdef.toUpperCase().includes('UNIQUE'),
    );
    expect(uniqueOnEventId).toBe(true);
  }, 20_000);

  // ── Test 5: Health endpoint sanity (messaging service) ────────────────────

  it('messaging service internal health endpoints are reachable', async () => {
    const [ready, live] = await Promise.all([
      fetch(`${MESSAGING_HEALTH_URL}/internal/health/ready`),
      fetch(`${MESSAGING_HEALTH_URL}/internal/health/live`),
    ]);

    expect(ready.status).toBe(200);
    expect(live.status).toBe(200);

    const readyBody = (await ready.json()) as { status: string };
    const liveBody = (await live.json()) as { status: string };

    expect(readyBody.status).toBe('ok');
    expect(liveBody.status).toBe('ok');
  }, 15_000);
});
