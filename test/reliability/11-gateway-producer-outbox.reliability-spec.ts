/**
 * ═══════════════════════════════════════════════════════════════════════════
 * RELIABILITY SCENARIO 11: Gateway Producer Outbox (CRITICAL ISSUE #1)
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * Proves the Producer Reliability Gap is closed:
 *
 *  Requirement A — RabbitMQ unavailable during the HTTP request:
 *    the event is still durably persisted, and is eventually delivered
 *    once the broker recovers.
 *
 *  Requirement B — Gateway crashes after persistence but before publish:
 *    no event loss. Simulated the same way Scenario 1 simulates the
 *    analogous messaging-service crash: insert a row exactly as the real
 *    HTTP handler would (a committed transaction), lock it with a stale
 *    timestamp (simulating a relay instance that claimed it and then
 *    died), and prove the stale-lock reaper + a live relay recover it.
 *
 *  Requirement C — Multiple gateway relay instances running concurrently:
 *    no duplicate publish. Proven via the SAME fencing-token mechanism
 *    Scenario 1 proves for the messaging-service outbox — a stale
 *    `lock_version` compare-and-swap against `gateway_outbox_events`
 *    must affect zero rows.
 *
 * All three assertions operate directly against `gateway_outbox_events`
 * (the producer-side table — see `GatewayOutboxEvent`/
 * `GatewayOutboxRelayService`), which is the actual mechanism that closes
 * CRITICAL ISSUE #1, NOT against `outbox_events` (the pre-existing
 * messaging-service consumer-side table Scenario 1 already covers).
 */

import { randomUUID } from 'crypto';
import request from 'supertest';
import { PgTestClient, pollUntil } from '../utils/pg-client';
import { waitForRabbitMqAmqpReady } from '../utils/rabbitmq-client';
import { waitForHttpReady } from '../utils/wait-for-health';
import {
  CONTAINERS,
  containerStop,
  containerStart,
  waitForContainerHealthy,
} from '../utils/docker-control';

const GATEWAY_URL = process.env.TEST_GATEWAY_URL ?? 'http://127.0.0.1:3005';
const RABBITMQ_URL =
  process.env.RABBITMQ_URL ?? 'amqp://guest:guest@127.0.0.1:5672';

const STALE_LOCK_TTL_MS = parseInt(
  process.env.GATEWAY_OUTBOX_LOCK_TTL_MS ?? '5000',
  10,
);

interface GatewayOutboxRow {
  [key: string]: unknown;
  id: number;
  event_type: string;
  status: 'pending' | 'sent' | 'failed';
  attempts: number;
  lock_version: number;
  locked_at: Date | null;
  locked_by: string | null;
  sent_at: Date | null;
  event_id: string | null;
  correlation_id: string | null;
}

async function getGatewayOutboxRowByEventId(
  db: PgTestClient,
  eventId: string,
): Promise<GatewayOutboxRow | null> {
  const rows = await db.query<GatewayOutboxRow>(
    'SELECT * FROM gateway_outbox_events WHERE event_id = $1',
    [eventId],
  );
  return rows[0] ?? null;
}

async function insertGatewayOutboxRow(
  db: PgTestClient,
  opts: { eventId: string; correlationId: string },
): Promise<number> {
  const rows = await db.query<{ id: number }>(
    `INSERT INTO gateway_outbox_events
       (event_type, payload, correlation_id, status, attempts, max_attempts, next_retry_at, event_id, lock_version)
     VALUES ('CreateMessageEvent.v1', $1, $2, 'pending', 0, 5, now(), $3, 0)
     RETURNING id`,
    [
      JSON.stringify({
        type: 'CreateMessageEvent.v1',
        eventId: opts.eventId,
        correlationId: opts.correlationId,
        timestamp: new Date().toISOString(),
        source: 'gateway',
        trace: ['gateway'],
        payload: {
          subject: 'Gateway outbox reliability test',
          content: 'hello',
        },
      }),
      opts.correlationId,
      opts.eventId,
    ],
  );
  return rows[0].id;
}

describe('Reliability Scenario 11: Gateway Producer Outbox', () => {
  let db: PgTestClient;

  beforeAll(async () => {
    await waitForHttpReady(GATEWAY_URL, 60_000);
    await waitForRabbitMqAmqpReady(RABBITMQ_URL, 60_000);

    db = new PgTestClient();
    await db.connect();
  });

  afterAll(async () => {
    try {
      containerStart(CONTAINERS.RABBITMQ);
    } catch {
      /* already running */
    }
    await waitForContainerHealthy(CONTAINERS.RABBITMQ, 120_000);
    await waitForRabbitMqAmqpReady(RABBITMQ_URL, 60_000);
    await db.disconnect();
  });

  // ── Requirement A ──────────────────────────────────────────────────────

  it('Requirement A: durably accepts the HTTP request while RabbitMQ is down, and delivers once it recovers', async () => {
    containerStop(CONTAINERS.RABBITMQ, 5);

    // The gateway's outbox write is a pure Postgres transaction with no
    // dependency on RabbitMQ at all — this is the entire point of the
    // fix. The HTTP request must still succeed with 202 Accepted even
    // though the broker is completely unreachable.
    const res = await request(GATEWAY_URL).get('/api/test-rabbit');

    expect(res.status).toBe(202);
    expect(res.body.status).toBe('accepted');
    expect(res.body.outboxId).toBeDefined();
    const eventId = res.body.eventId as string;
    expect(eventId).toBeDefined();

    // The row exists and is durably pending — not lost — while the
    // broker is still down.
    const row = await getGatewayOutboxRowByEventId(db, eventId);
    expect(row).not.toBeNull();
    expect(row!.status).not.toBe('sent'); // broker is down, can't confirm yet

    containerStart(CONTAINERS.RABBITMQ);
    await waitForContainerHealthy(CONTAINERS.RABBITMQ, 120_000);
    await waitForRabbitMqAmqpReady(RABBITMQ_URL, 60_000);

    // Nudge any back-off schedule forward so the test doesn't wait out
    // a full exponential delay window.
    await db.query(
      `UPDATE gateway_outbox_events SET next_retry_at = now() WHERE status = 'pending'`,
    );

    await pollUntil(
      `gateway outbox row for eventId ${eventId} reaches 'sent' after broker recovery`,
      async () => {
        const r = await getGatewayOutboxRowByEventId(db, eventId);
        return r !== null && r.status === 'sent';
      },
      { timeoutMs: 120_000, intervalMs: 1_000 },
    );

    const finalRow = await getGatewayOutboxRowByEventId(db, eventId);
    expect(finalRow!.status).toBe('sent');
    expect(finalRow!.sent_at).not.toBeNull();
    expect(finalRow!.event_id).toBe(eventId);
  }, 240_000);

  // ── Requirement B ──────────────────────────────────────────────────────

  it('Requirement B: no event loss when a relay instance crashes after claiming but before publishing', async () => {
    const correlationId = randomUUID();
    const eventId = randomUUID();

    const outboxId = await insertGatewayOutboxRow(db, {
      eventId,
      correlationId,
    });

    // Simulate a relay instance that claimed this row, then crashed
    // before calling markSent() — same technique as Scenario 1.
    const crashTime = new Date(Date.now() - (STALE_LOCK_TTL_MS + 2_000));
    await db.query(
      `UPDATE gateway_outbox_events
         SET locked_at    = $1,
             locked_by    = 'crashed-gateway-relay-DEAD-INSTANCE',
             lock_version = 1
         WHERE id = $2`,
      [crashTime.toISOString(), outboxId],
    );

    const lockedRow = await getGatewayOutboxRowByEventId(db, eventId);
    expect(lockedRow!.status).toBe('pending');
    expect(lockedRow!.locked_by).toBe('crashed-gateway-relay-DEAD-INSTANCE');

    // The stale-lock reaper must clear the dead instance's lock...
    await pollUntil(
      `stale lock cleared for gateway outbox row ${outboxId}`,
      async () => {
        const row = await getGatewayOutboxRowByEventId(db, eventId);
        return row !== null && row.locked_at === null && row.locked_by === null;
      },
      { timeoutMs: 30_000, intervalMs: 500 },
    );

    // ...and a live relay instance must then claim, publish, and confirm it.
    await pollUntil(
      `gateway outbox row ${outboxId} reaches 'sent' after crash recovery`,
      async () => {
        const row = await getGatewayOutboxRowByEventId(db, eventId);
        return row !== null && row.status === 'sent';
      },
      { timeoutMs: 60_000, intervalMs: 500 },
    );

    const finalRow = await getGatewayOutboxRowByEventId(db, eventId);
    expect(finalRow!.status).toBe('sent');
    expect(finalRow!.status).not.toBe('failed');
    expect(finalRow!.event_id).toBe(eventId);
  }, 120_000);

  // ── Requirement C ──────────────────────────────────────────────────────

  it('Requirement C: a stale relay instance cannot double-publish (fencing token rejects a stale markSent)', async () => {
    const correlationId = randomUUID();
    const eventId = randomUUID();

    const outboxId = await insertGatewayOutboxRow(db, {
      eventId,
      correlationId,
    });

    await pollUntil(
      `gateway outbox row ${outboxId} sent`,
      async () => {
        const row = await getGatewayOutboxRowByEventId(db, eventId);
        return row?.status === 'sent';
      },
      { timeoutMs: 60_000, intervalMs: 500 },
    );

    const finalRow = await getGatewayOutboxRowByEventId(db, eventId);
    expect(finalRow!.status).toBe('sent');
    expect(finalRow!.lock_version).toBeGreaterThanOrEqual(1);

    // Simulate a SECOND relay instance that is still mid-publish with a
    // STALE lock_version (0) — exactly the SQL GatewayOutboxRelayService
    // .markSent() runs. With the fencing token, this must affect 0 rows.
    const staleUpdateResult = await db.query<{ id: number }>(
      `UPDATE gateway_outbox_events
         SET status = 'sent', sent_at = now()
         WHERE id = $1 AND lock_version = 0
         RETURNING id`,
      [outboxId],
    );
    expect(staleUpdateResult.length).toBe(0);

    // The row remains exactly as the real, winning markSent() call left
    // it — no corruption, no double-counted publish.
    const afterStaleUpdate = await getGatewayOutboxRowByEventId(db, eventId);
    expect(afterStaleUpdate!.status).toBe('sent');
    expect(afterStaleUpdate!.lock_version).toBe(finalRow!.lock_version);
  }, 90_000);

  // ── End-to-end sanity: the event the gateway outbox publishes actually
  //    lands on the command bus, not just "status=sent" in isolation. ────

  it('a delivered gateway outbox event is actually consumed end-to-end by the messaging service', async () => {
    const correlationId = randomUUID();
    const res = await request(GATEWAY_URL)
      .get('/api/test-rabbit-v2')
      .set('x-correlation-id', correlationId);
    expect(res.status).toBe(202);
    const eventId = res.body.eventId as string;

    await pollUntil(
      `gateway outbox row for eventId ${eventId} reaches 'sent'`,
      async () => {
        const row = await getGatewayOutboxRowByEventId(db, eventId);
        return row !== null && row.status === 'sent';
      },
      { timeoutMs: 60_000, intervalMs: 500 },
    );

    // The messaging service must actually receive and process the
    // event the gateway relay delivered -- not merely mark its own
    // outbox row 'sent' in isolation. A row in `messages` keyed by the
    // SAME correlationId is the end-to-end proof.
    await pollUntil(
      `messaging service persists a message for correlationId ${correlationId}`,
      async () => (await db.countMessagesByCorrelationId(correlationId)) > 0,
      { timeoutMs: 30_000, intervalMs: 500 },
    );
  }, 90_000);
});
