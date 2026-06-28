/**
 * ═══════════════════════════════════════════════════════════════════════════
 * RELIABILITY SCENARIO 12: Domain-Event / Command Routing Isolation
 * (Architectural Gap #2 — MessagePersisted Routing Loop)
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * FAILURE BEING FIXED
 * Before this fix, `MessagePersisted` outbox rows (written by the
 * messaging service as a side effect of handling a CreateMessageEvent)
 * were published to the SAME exchange/routing-key as commands
 * (`messaging.direct` / `messaging.work`), landing in `messaging.work` —
 * the exact queue `MessagingController.handleMessage` consumes, whose
 * `@MessagePattern` only matches `CreateMessageEvent.v1`/`.v2`. A
 * `MessagePersisted` event there is not ackable by that handler, and the
 * queue's own DLX wiring (which exists for genuine command failures) then
 * treats it as just another failed command: requeue → retry → DLQ,
 * entirely self-inflicted.
 *
 * WHAT THIS TEST PROVES
 *  D1. A real, end-to-end-triggered `MessagePersisted` event lands on the
 *      `messaging.events` audit queue (`resolveOutboxRoute()`'s domain
 *      event destination), NOT `messaging.work`.
 *  D2. `messaging.work`'s queue depth and the DLQ's queue depth are
 *      unaffected by that `MessagePersisted` event having been
 *      published — i.e. no work→nack→retry→work→retry→DLQ loop, and no
 *      DLQ pollution, exactly the two outcomes the prior bug produced.
 *  D3. Direct unit-level proof that `resolveOutboxRoute()` itself
 *      classifies `MessagePersisted` as a domain event (not a command),
 *      and `CreateMessageEvent.v1`/`.v2` as commands — the single
 *      chokepoint both relays depend on.
 */

import { randomUUID } from 'crypto';
import request from 'supertest';
import { PgTestClient, pollUntil } from '../utils/pg-client';
import {
  RabbitMqTestClient,
  waitForRabbitMqAmqpReady,
} from '../utils/rabbitmq-client';
import { waitForHttpReady } from '../utils/wait-for-health';
import {
  EXCHANGES,
  QUEUES,
  resolveOutboxRoute,
} from '../../apps/messaging/src/reliability/topology';

const GATEWAY_URL = process.env.TEST_GATEWAY_URL ?? 'http://127.0.0.1:3005';
const RABBITMQ_URL =
  process.env.RABBITMQ_URL ?? 'amqp://guest:guest@127.0.0.1:5672';

describe('Reliability Scenario 12: Domain-Event / Command Routing Isolation', () => {
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

  // ── D3: unit-level proof of the routing chokepoint itself ──────────────

  it('D3: resolveOutboxRoute classifies commands vs domain events correctly', () => {
    expect(resolveOutboxRoute('CreateMessageEvent.v1')).toEqual({
      exchange: EXCHANGES.MAIN,
      routingKey: 'messaging.work',
      kind: 'command',
    });
    expect(resolveOutboxRoute('CreateMessageEvent.v2')).toEqual({
      exchange: EXCHANGES.MAIN,
      routingKey: 'messaging.work',
      kind: 'command',
    });

    // The event at the center of the original bug report.
    expect(resolveOutboxRoute('MessagePersisted')).toEqual({
      exchange: EXCHANGES.EVENTS,
      routingKey: '',
      kind: 'domain-event',
    });

    // Fail-safe default: an unrecognized type is never accidentally
    // treated as a command.
    expect(resolveOutboxRoute('SomeFutureEventNobodyRegisteredYet').kind).toBe(
      'domain-event',
    );
  });

  // ── D1 + D2: end-to-end proof against the real running stack ───────────

  it('D1+D2: a real MessagePersisted event reaches the audit queue, never messaging.work, and never pollutes the DLQ', async () => {
    await rmq.purgeAllQueues();

    const workDepthBefore = await rmq.getQueueDepth(QUEUES.WORK);
    const dlqDepthBefore = await rmq.getQueueDepth(QUEUES.DLQ);

    const correlationId = randomUUID();

    // Trigger the full pipeline: gateway accepts → gateway outbox relay
    // delivers a CreateMessageEvent command → messaging service
    // consumes it, persists a Message, and writes its OWN
    // `MessagePersisted` outbox row as a side effect.
    const res = await request(GATEWAY_URL)
      .get('/api/test-rabbit-v2')
      .set('x-correlation-id', correlationId);
    expect(res.status).toBe(202);

    // Wait for the messaging service to actually persist the message --
    // proof the command itself was processed normally.
    await pollUntil(
      `messaging service persists a message for correlationId ${correlationId}`,
      async () => (await db.countMessagesByCorrelationId(correlationId)) > 0,
      { timeoutMs: 60_000, intervalMs: 500 },
    );

    // D1: the resulting MessagePersisted domain event shows up on the
    // audit queue bound to the NEW messaging.events fanout exchange.
    const auditMessage = await pollForAuditMessage(rmq, correlationId);
    expect(auditMessage).not.toBeNull();
    expect(auditMessage!.routingKey).toBe(''); // fanout: no routing key
    expect(auditMessage!.redelivered).toBe(false); // delivered once, no retry loop

    // D2: messaging.work and the DLQ are unaffected by this domain
    // event having existed at all -- precisely the "no self-generated
    // DLQ traffic" requirement.
    await pollUntil(
      'messaging.work queue depth returns to its pre-test baseline',
      async () => (await rmq.getQueueDepth(QUEUES.WORK)) <= workDepthBefore,
      { timeoutMs: 15_000, intervalMs: 500 },
    );

    const dlqDepthAfter = await rmq.getQueueDepth(QUEUES.DLQ);
    expect(dlqDepthAfter).toBe(dlqDepthBefore);
  }, 90_000);

  /**
   * Poll the audit queue with `drainQueue` (non-blocking `basic.get`) until
   * the `MessagePersisted` event correlated to this test run appears, or
   * give up after the timeout. Using `drainQueue` rather than
   * `waitForMessage` here because other concurrent test files in this
   * suite may also produce `MessagePersisted` events on the SAME shared
   * audit queue -- we need to find OUR event specifically, not just any
   * message, without consuming and discarding ones that belong to other
   * tests.
   */
  async function pollForAuditMessage(
    client: RabbitMqTestClient,
    correlationId: string,
    timeoutMs = 30_000,
  ): Promise<{ routingKey: string; redelivered: boolean } | null> {
    const deadline = Date.now() + timeoutMs;

    while (Date.now() < deadline) {
      const messages = await client.drainQueue(QUEUES.EVENTS_AUDIT, 50);
      const match = messages.find(
        (m) =>
          (m.content as { correlationId?: string }).correlationId ===
          correlationId,
      );
      if (match) {
        return { routingKey: match.routingKey, redelivered: match.redelivered };
      }
      await new Promise((r) => setTimeout(r, 500));
    }

    return null;
  }
});
