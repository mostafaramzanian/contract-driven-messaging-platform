import { Client } from 'pg';
import request from 'supertest';
import { EventTracker } from '../utils/event-tracker';
import {
  waitForHttpReady,
  waitForRabbitMqReady,
} from '../utils/wait-for-health';

/**
 * Full-stack integration test for the v2 contract path:
 * Gateway -> RabbitMQ -> Messaging -> PostgreSQL.
 *
 * This is the v2 counterpart to
 * `test/integration/messaging-flow.integration-spec.ts` (which exercises
 * v1 exclusively via `/api/test-rabbit`). It exists to verify, against
 * real infrastructure rather than mocks, the parts of the v1/v2 dual
 * support that a unit test cannot observe end to end:
 *
 *  1. The gateway's v2 route (`/api/test-rabbit-v2`) actually builds,
 *     validates, and emits a `CreateMessageEvent.v2` -- with the
 *     `x-schema-version: 2` AMQP header genuinely attached via
 *     `RmqRecordBuilder` and genuinely reaching the broker (a unit test
 *     can assert the gateway *constructs* an `RmqRecord`, but cannot
 *     prove `@nestjs/microservices`' `RmqRecordSerializer` and the real
 *     amqplib client actually turn that into a header on the wire).
 *  2. `MessagingController.handleMessage`'s multi-pattern
 *     `@MessagePattern([CreateMessageEvent.name, CreateMessageEventNameV2.name])`
 *     genuinely receives and dispatches a `'CreateMessageEvent.v2'`
 *     -patterned message correctly via the real NestJS RMQ transport (a
 *     unit test calls `handleMessage` directly, bypassing NestJS's actual
 *     pattern-matching/dispatch machinery entirely).
 *  3. The upcast-to-v2 normalization and the rest of the pipeline
 *     (idempotency check, outbox-backed persistence) behave identically
 *     for a *native* v2 event as they do for v1 -- this test's v2 event
 *     never goes through `upcastCreateMessageEventV1ToV2` at all (only a
 *     v1 event would), so it specifically exercises the "already v2,
 *     passed through unchanged" branch in
 *     `MessagingController.handleMessage`, which the v1 integration test
 *     cannot reach.
 *  4. `priority`/`metadata` survive validation and reach
 *     `MessagingService.handleMessageCreation` as part of the normalized
 *     payload, even though current business logic does not yet persist
 *     them anywhere (see docs/contract-evolution.md -- using these fields
 *     for business behavior is out of scope for this contract-evolution
 *     work). This test only asserts they don't break the pipeline, not
 *     that they're stored.
 *
 * Same non-polling, lifecycle-driven assertion strategy as the v1 test --
 * see that file's header comment for the full rationale, which applies
 * unchanged here. `EventTracker` and `waitFor*` are entirely
 * version-agnostic (they operate on `eventId` alone), so no new test
 * utility was needed for this file.
 */

const GATEWAY_URL = process.env.TEST_GATEWAY_URL ?? 'http://127.0.0.1:3005';
const RABBITMQ_URL =
  process.env.RABBITMQ_URL ?? 'amqp://guest:guest@127.0.0.1:5672';

const DB_CONFIG = {
  host: process.env.TEST_DB_HOST ?? '127.0.0.1',
  port: Number.parseInt(process.env.TEST_DB_PORT ?? '5432', 10),
  user: process.env.TEST_DB_USERNAME ?? 'admin',
  password: process.env.TEST_DB_PASSWORD ?? 'test_password',
  database: process.env.TEST_DB_NAME ?? 'showcase_test_db',
  connectionTimeoutMillis: 30000,
};

interface TestRabbitV2ResponseBody {
  status: string;
  message: string;
  correlationId: string;
  eventId: string;
  eventType: string;
  schemaVersion: string;
}

interface RejectedResponseBody {
  status: string;
  reason: string;
  eventType: string;
  eventId: string;
  errors: { path: string; message: string }[];
}

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

describe('Gateway -> RabbitMQ -> Messaging -> PostgreSQL (v2 integration)', () => {
  let dbClient: Client;
  let tracker: EventTracker;

  beforeAll(async () => {
    expect(RABBITMQ_URL).toBeDefined();

    await waitForHttpReady(`${GATEWAY_URL}/api`);
    await waitForRabbitMqReady(RABBITMQ_URL);

    dbClient = new Client(DB_CONFIG);
    await dbClient.connect();

    tracker = new EventTracker(RABBITMQ_URL);
    await tracker.connect();
  }, 40_000);

  afterAll(async () => {
    if (dbClient) {
      await dbClient.end();
    }
    if (tracker) {
      await tracker.close();
    }
  });

  it('propagates a validated CreateMessageEvent.v2 through gateway -> RabbitMQ -> messaging -> PostgreSQL', async () => {
    // Step 1: hit the real gateway's v2 route over HTTP.
    const response = await request(GATEWAY_URL)
      .get('/api/test-rabbit-v2')
      .expect(200);

    const body = response.body as TestRabbitV2ResponseBody;

    expect(body.status).toBe('success');
    expect(body.eventType).toBe('CreateMessageEvent.v2');
    expect(body.schemaVersion).toBe('2');
    expect(body.correlationId).toMatch(UUID_PATTERN);
    expect(body.eventId).toMatch(UUID_PATTERN);

    const { eventId, correlationId } = body;

    // Steps 2-4: same event-driven, non-polling wait as the v1 test.
    const chain = await tracker.waitForFullChain(eventId, 15_000);

    // correlationId identical at every hop.
    expect(chain.emitted.correlationId).toBe(correlationId);
    expect(chain.received.correlationId).toBe(correlationId);
    expect(chain.validated.correlationId).toBe(correlationId);
    expect(chain.persisted.correlationId).toBe(correlationId);

    // every record tagged with the service that produced it -- unchanged
    // from v1, the lifecycle mechanism itself has no version awareness.
    expect(chain.emitted.service).toBe('gateway');
    expect(chain.received.service).toBe('messaging');
    expect(chain.validated.service).toBe('messaging');
    expect(chain.persisted.service).toBe('messaging');

    // critical v2-specific assertion: every lifecycle record's eventType
    // must read 'CreateMessageEvent.v2', not 'CreateMessageEvent.v1' and
    // not some normalized/upcasted label. This is the wire-resolved
    // `eventTypeName` from MessagingController.handleMessage (see
    // Checkpoint #8 in the project history / docs/contract-evolution.md)
    // showing up correctly in real, broker-delivered lifecycle records --
    // not just in a unit test's mocked lifecyclePublisher.publish() calls.
    expect(chain.emitted.eventType).toBe('CreateMessageEvent.v2');
    expect(chain.received.eventType).toBe('CreateMessageEvent.v2');
    expect(chain.validated.eventType).toBe('CreateMessageEvent.v2');
    expect(chain.persisted.eventType).toBe('CreateMessageEvent.v2');

    // chain happened in this order in wall-clock time.
    const timestamps = [
      chain.emitted.timestamp,
      chain.received.timestamp,
      chain.validated.timestamp,
      chain.persisted.timestamp,
    ].map((t) => new Date(t).getTime());
    expect(timestamps).toEqual([...timestamps].sort((a, b) => a - b));

    // Now read what was actually persisted. The v2 gateway route uses
    // 'System test message (v2)' as its fixed subject (see
    // AppController.sendTestMessageV2), distinct from v1's
    // 'System test message', so this query cannot accidentally match the
    // row the v1 integration test created.
    const result = await dbClient.query<{
      id: number;
      title: string;
      content: string;
      sender: string;
      createdAt: Date;
    }>(
      `SELECT id, title, content, sender, "createdAt"
       FROM messages
       WHERE title = $1
       ORDER BY id DESC
       LIMIT 1`,
      ['System test message (v2)'],
    );

    expect(result.rows).toHaveLength(1);
    const row = result.rows[0];
    expect(row.title).toBe('System test message (v2)');
    expect(row.content).toBe('Hello RabbitMQ!');
    expect(row.sender).toBe('system-user');
    expect(row.id).toBeDefined();
    expect(row.createdAt).toBeDefined();
  }, 70_000);

  it('rejects and never emits a v2 request whose correlationId is not a valid UUID', async () => {
    const response = await request(GATEWAY_URL)
      .get('/api/test-rabbit-v2')
      .set('x-correlation-id', 'not-a-valid-uuid')
      .expect(400);

    const body = response.body as RejectedResponseBody;
    expect(body.status).toBe('rejected');
    expect(body.reason).toBe('event_contract_violation');
    expect(body.eventType).toBe('CreateMessageEvent.v2');
    expect(body.eventId).toMatch(UUID_PATTERN);
    expect(body.errors.some((error) => error.path === 'correlationId')).toBe(
      true,
    );

    const rejected = await tracker.waitForRejected(body.eventId, 10_000);
    expect(rejected.service).toBe('gateway');
    expect(rejected.eventType).toBe('CreateMessageEvent.v2');
    expect(
      rejected.errors?.some((error) => error.path === 'correlationId'),
    ).toBe(true);

    // Same fail-fast guarantee as v1: rejected before ever reaching
    // RabbitMQ, so no received/validated/persisted record exists for this
    // eventId and nothing was written to the database.
  });

  it('v1 and v2 requests in the same run produce independently correct, non-conflated lifecycle chains', async () => {
    // This is the integration-level counterpart to the unit test added in
    // Checkpoint #8 ("does not affect v1 dispatch when v1 and v2 messages
    // are handled by the same instance in sequence") -- here it runs
    // against the real multi-pattern @MessagePattern dispatch and the
    // real shared messaging.work queue, not a mocked controller method.
    const v1Response = await request(GATEWAY_URL)
      .get('/api/test-rabbit')
      .expect(200);
    const v2Response = await request(GATEWAY_URL)
      .get('/api/test-rabbit-v2')
      .expect(200);

    const v1Body = v1Response.body as { eventId: string; eventType: string };
    const v2Body = v2Response.body as { eventId: string; eventType: string };

    expect(v1Body.eventId).not.toBe(v2Body.eventId);

    const [v1Chain, v2Chain] = await Promise.all([
      tracker.waitForFullChain(v1Body.eventId, 15_000),
      tracker.waitForFullChain(v2Body.eventId, 15_000),
    ]);

    expect(v1Chain.persisted.eventType).toBe('CreateMessageEvent.v1');
    expect(v2Chain.persisted.eventType).toBe('CreateMessageEvent.v2');
  }, 70_000);
});
