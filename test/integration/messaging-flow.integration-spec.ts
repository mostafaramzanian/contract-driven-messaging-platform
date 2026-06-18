import { Client } from 'pg';
import request from 'supertest';
import { EventTracker } from '../utils/event-tracker';
import {
  waitForHttpReady,
  waitForRabbitMqReady,
} from '../utils/wait-for-health';

/**
 * Full-stack integration test: Gateway -> RabbitMQ -> Messaging -> PostgreSQL.
 *
 * This test does NOT use NestJS's TestingModule and does NOT mock RabbitMQ
 * or the database. It assumes the stack defined in docker-compose.test.yml
 * is already running (see package.json `test:integration` script) and
 * exercises it exactly as a real client would:
 *
 *  1. HTTP GET against the real gateway container.
 *  2. The gateway builds and validates a CreateMessageEvent.v1 (see
 *     libs/contracts) and emits it to the real RabbitMQ broker.
 *  3. The real messaging service consumes the event, validates it again
 *     independently, and persists a row via TypeORM into the real
 *     PostgreSQL database.
 *  4. Both services also publish lifecycle records (emitted / received /
 *     validated / persisted) to a separate, observability-only fanout
 *     exchange (see libs/contracts/src/lifecycle). This test subscribes
 *     to that exchange via EventTracker and asserts on the exact sequence
 *     of stages for this event's specific eventId, instead of polling the
 *     database for a row to eventually appear.
 *
 * Why this replaces DB polling: a polling loop only ever answers "did a
 * row with a matching value show up by the time I gave up looking?" -- it
 * cannot tell you the row came from *this* request, or that contract
 * validation happened on both ends, or what order things occurred in. The
 * lifecycle stages are emitted by the request-handling code at the moment
 * each step happens, so waiting on them is bounded by the real event
 * (resolves immediately on arrival, not on the next poll tick) and gives
 * a precise, eventId-scoped answer to "did a validated, schema-compliant
 * event propagate correctly through the distributed system?".
 *
 * The final DB read below is intentionally a single, non-polling query:
 * by the time `waitForPersisted` has resolved, the messaging service has
 * already told us (via the lifecycle exchange) that it finished the
 * `messageRepository.save()` call for this eventId, so there is nothing
 * left to wait for -- the row is already there, predictably.
 *
 * Ports used here (3005, 5432, 5672) are the host-mapped ports from
 * docker-compose.test.yml, which are deliberately the SAME ports the dev
 * stack (docker-compose.yml) uses -- there is exactly one address per
 * service, sourced from RABBITMQ_URL (and the TEST_DB_ and
 * TEST_GATEWAY_URL variables, which are still namespaced to avoid
 * accidentally picking up an unrelated DB_HOST a developer's shell might
 * have exported for some other project) rather than a test-specific
 * alternate port. The tradeoff is that this stack and the dev stack
 * cannot both be running at once -- see the header comment in
 * docker-compose.test.yml.
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

interface TestRabbitResponseBody {
  status: string;
  message: string;
  correlationId: string;
  eventId: string;
  eventType: string;
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

describe('Gateway -> RabbitMQ -> Messaging -> PostgreSQL (integration)', () => {
  let dbClient: Client;
  let tracker: EventTracker;

  beforeAll(async () => {
    // The test must have a real RabbitMQ address to connect to -- either
    // injected via the RABBITMQ_URL environment variable or the fallback
    // default above, but never silently undefined.
    expect(RABBITMQ_URL).toBeDefined();

    // Readiness gate: wait for the actual infrastructure to accept
    // connections before running any assertions. This is a bounded check
    // on dependency availability, not a substitute for the event-driven
    // assertions below.
    await waitForHttpReady(`${GATEWAY_URL}/api`);
    await waitForRabbitMqReady(RABBITMQ_URL);

    dbClient = new Client(DB_CONFIG);
    await dbClient.connect();

    tracker = new EventTracker(RABBITMQ_URL);
    await tracker.connect();
  }, 40_000);

  afterAll(async () => {
    // beforeAll may throw before dbClient/tracker are assigned (e.g. a
    // readiness timeout). Without these guards, that throw is masked by a
    // second, more confusing "Cannot read properties of undefined" error
    // from this hook, instead of the original, clearer setup failure.
    if (dbClient) {
      await dbClient.end();
    }
    if (tracker) {
      await tracker.close();
    }
  });

  it('propagates a validated CreateMessageEvent.v1 through gateway -> RabbitMQ -> messaging -> PostgreSQL', async () => {
    // Step 1: hit the real gateway over HTTP.
    const response = await request(GATEWAY_URL)
      .get('/api/test-rabbit')
      .expect(200);

    const body = response.body as TestRabbitResponseBody;

    expect(body.status).toBe('success');
    expect(body.eventType).toBe('CreateMessageEvent.v1');
    expect(body.correlationId).toMatch(UUID_PATTERN);
    expect(body.eventId).toMatch(UUID_PATTERN);

    const { eventId, correlationId } = body;

    // Steps 2-4: instead of polling the database, wait for the real
    // lifecycle records the gateway and messaging service publish as
    // they actually do the work, in order, scoped to this exact eventId.
    // waitForFullChain awaits four stages sequentially, each with its own
    // 15s budget (see test/utils/event-tracker.ts), so this test's own
    // timeout below is set well above 4 * 15s rather than relying on
    // jest-integration.json's 30s default -- in the normal case all four
    // stages resolve within milliseconds of each other, but a worst-case
    // slow-and-eventually-successful chain should fail with this test's
    // own clear timeout error, not a generic Jest "test timed out".
    const chain = await tracker.waitForFullChain(eventId, 15_000);

    // correlationId must be identical at every hop -- this is the
    // end-to-end correlationId check the prior DB-polling version of
    // this test explicitly could not perform (the Message entity has no
    // correlationId column; see docs/architecture.md "Limitations").
    expect(chain.emitted.correlationId).toBe(correlationId);
    expect(chain.received.correlationId).toBe(correlationId);
    expect(chain.validated.correlationId).toBe(correlationId);
    expect(chain.persisted.correlationId).toBe(correlationId);

    // every record must be tagged with the service that produced it
    expect(chain.emitted.service).toBe('gateway');
    expect(chain.received.service).toBe('messaging');
    expect(chain.validated.service).toBe('messaging');
    expect(chain.persisted.service).toBe('messaging');

    // and the chain must have happened in this order in wall-clock time
    const timestamps = [
      chain.emitted.timestamp,
      chain.received.timestamp,
      chain.validated.timestamp,
      chain.persisted.timestamp,
    ].map((t) => new Date(t).getTime());
    expect(timestamps).toEqual([...timestamps].sort((a, b) => a - b));

    // Now that `persisted` has been confirmed, a single non-polling read
    // confirms what was actually written, instead of asserting blindly.
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
      ['System test message'],
    );

    expect(result.rows).toHaveLength(1);
    const row = result.rows[0];
    expect(row.title).toBe('System test message');
    expect(row.content).toBe('Hello RabbitMQ!');
    expect(row.sender).toBe('system-user');
    expect(row.id).toBeDefined();
    expect(row.createdAt).toBeDefined();
  }, 70_000);

  it('rejects and never emits a request whose correlationId is not a valid UUID', async () => {
    const response = await request(GATEWAY_URL)
      .get('/api/test-rabbit')
      .set('x-correlation-id', 'not-a-valid-uuid')
      .expect(400);

    const body = response.body as RejectedResponseBody;
    expect(body.status).toBe('rejected');
    expect(body.reason).toBe('event_contract_violation');
    expect(body.eventType).toBe('CreateMessageEvent.v1');
    expect(body.eventId).toMatch(UUID_PATTERN);
    expect(body.errors.some((error) => error.path === 'correlationId')).toBe(
      true,
    );

    // Deterministic negative-path signal: the gateway publishes a
    // `rejected` lifecycle record for this exact eventId at the moment it
    // refuses the event, before the HTTP response is even sent. Waiting
    // for that record (rather than sleeping a fixed duration and then
    // checking the database) confirms the rejection path actually ran,
    // without depending on timing.
    const rejected = await tracker.waitForRejected(body.eventId, 10_000);
    expect(rejected.service).toBe('gateway');
    expect(
      rejected.errors?.some((error) => error.path === 'correlationId'),
    ).toBe(true);

    // This event was rejected before ever being emitted to RabbitMQ, so
    // it never reaches messaging at all -- there is no `received`,
    // `validated`, or `persisted` record for this eventId to wait for,
    // and therefore nothing for messaging to write to the database. That
    // absence is exactly what "fail fast: reject, log, drop" means here.
  });
});
