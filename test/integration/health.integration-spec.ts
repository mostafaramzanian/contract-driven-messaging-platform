/**
 * Integration test: Health Checks
 *
 * Verifies that both the gateway and messaging service expose correctly
 * functioning liveness and readiness endpoints.
 *
 * ## What we check
 *
 * Gateway:
 *   GET /health/ready  → 200 { status: 'ok' }  (messaging service reachable)
 *   GET /health/live   → 200 { status: 'ok' }  (process alive)
 *
 * Messaging service (internal HTTP on port 3006):
 *   GET /internal/health/ready  → 200 { status: 'ok' }  (DB + RabbitMQ up)
 *   GET /internal/health/live   → 200 { status: 'ok' }  (process alive)
 *
 * ## Response shape
 * @nestjs/terminus responses follow this schema:
 * ```json
 * {
 *   "status": "ok" | "error" | "shutting_down",
 *   "info":    { "<key>": { "status": "up" | "down", ...details } },
 *   "error":   { "<key>": { "status": "down", "message": "..." } },
 *   "details": { "<key>": { "status": "up" | "down", ...details } }
 * }
 * ```
 * We assert on `status: 'ok'` at the top level and the presence of
 * specific indicator keys in `info` / `details`.
 *
 * ## Degraded-state test
 * We intentionally do NOT test the "dependency down" path in integration
 * tests because taking down Postgres or RabbitMQ mid-test would affect
 * other test suites running in the same process.  That coverage belongs
 * in unit tests for the individual health indicators.
 */

import request from 'supertest';
import {
  waitForHttpReady,
  waitForRabbitMqReady,
} from '../utils/wait-for-health';

const GATEWAY_URL = process.env.TEST_GATEWAY_URL ?? 'http://127.0.0.1:3005';
const RABBITMQ_URL =
  process.env.RABBITMQ_URL ?? 'amqp://guest:guest@127.0.0.1:5672';
const MESSAGING_INTERNAL_URL =
  process.env.MESSAGING_HEALTH_URL ?? 'http://127.0.0.1:3006';

// ── Type helpers ──────────────────────────────────────────────────────────────

interface TerminusResponse {
  status: 'ok' | 'error' | 'shutting_down';
  info: Record<string, { status: 'up' | 'down'; [key: string]: unknown }>;
  error: Record<string, { status: 'down'; message?: string }>;
  details: Record<string, { status: 'up' | 'down'; [key: string]: unknown }>;
}

// ── Suite ─────────────────────────────────────────────────────────────────────

describe('Health checks (integration)', () => {
  beforeAll(async () => {
    await waitForHttpReady(`${GATEWAY_URL}/api`);
    await waitForRabbitMqReady(RABBITMQ_URL);
    // Give the messaging health HTTP server a moment to start
    await waitForHttpReady(`${MESSAGING_INTERNAL_URL}/internal/health/live`);
  }, 40_000);

  // ── Gateway health ──────────────────────────────────────────────────────────

  describe('Gateway /health', () => {
    it('GET /health/live returns 200 with status ok', async () => {
      const response = await request(GATEWAY_URL)
        .get('/health/live')
        .expect(200);

      const body = response.body as TerminusResponse;
      expect(body.status).toBe('ok');
      // Liveness has no checks → empty info/details
      expect(body.error).toEqual({});
    });

    it('GET /health/ready returns 200 with status ok when messaging is up', async () => {
      const response = await request(GATEWAY_URL)
        .get('/health/ready')
        .expect(200);

      const body = response.body as TerminusResponse;
      expect(body.status).toBe('ok');

      // The readiness check probes the messaging service via HTTP
      expect(body.info['messaging-service']).toBeDefined();
      expect(body.info['messaging-service'].status).toBe('up');
    });

    it('GET /health/ready includes the messaging-service indicator in details', async () => {
      const response = await request(GATEWAY_URL)
        .get('/health/ready')
        .expect(200);

      const body = response.body as TerminusResponse;
      expect(body.details['messaging-service']).toBeDefined();
      expect(body.details['messaging-service'].status).toBe('up');
    });

    it('GET /health/live returns correct Content-Type application/json', async () => {
      await request(GATEWAY_URL)
        .get('/health/live')
        .expect('Content-Type', /application\/json/)
        .expect(200);
    });

    it('GET /health/ready is not prefixed with /api (health routes are outside global prefix)', async () => {
      // Global prefix is 'api', but health checks must work WITHOUT the prefix
      // so Kubernetes probes don't need to know the prefix.
      // This asserts the health module is mounted outside the global prefix.
      await request(GATEWAY_URL).get('/health/live').expect(200);
      // The prefixed path should return 404
      await request(GATEWAY_URL).get('/api/health/live').expect(404);
    });
  });

  // ── Messaging internal health ───────────────────────────────────────────────

  describe('Messaging /internal/health', () => {
    it('GET /internal/health/live returns 200 with status ok', async () => {
      const response = await request(MESSAGING_INTERNAL_URL)
        .get('/internal/health/live')
        .expect(200);

      const body = response.body as TerminusResponse;
      expect(body.status).toBe('ok');
      expect(body.error).toEqual({});
    });

    it('GET /internal/health/ready returns 200 with status ok when DB and RabbitMQ are up', async () => {
      const response = await request(MESSAGING_INTERNAL_URL)
        .get('/internal/health/ready')
        .expect(200);

      const body = response.body as TerminusResponse;
      expect(body.status).toBe('ok');
    });

    it('GET /internal/health/ready includes a database indicator', async () => {
      const response = await request(MESSAGING_INTERNAL_URL)
        .get('/internal/health/ready')
        .expect(200);

      const body = response.body as TerminusResponse;
      expect(body.info['database']).toBeDefined();
      expect(body.info['database'].status).toBe('up');
    });

    it('GET /internal/health/ready includes a rabbitmq indicator', async () => {
      const response = await request(MESSAGING_INTERNAL_URL)
        .get('/internal/health/ready')
        .expect(200);

      const body = response.body as TerminusResponse;
      expect(body.info['rabbitmq']).toBeDefined();
      expect(body.info['rabbitmq'].status).toBe('up');
    });

    it('RabbitMQ indicator in details does not expose credentials', async () => {
      const response = await request(MESSAGING_INTERNAL_URL)
        .get('/internal/health/ready')
        .expect(200);

      const body = response.body as TerminusResponse;
      const rmqDetails = body.details['rabbitmq'];
      if (rmqDetails && typeof rmqDetails['url'] === 'string') {
        // Credentials must be masked in the health response
        expect(rmqDetails['url']).not.toMatch(/guest/);
      }
    });

    it('liveness probe does not include infrastructure indicator checks', async () => {
      const response = await request(MESSAGING_INTERNAL_URL)
        .get('/internal/health/live')
        .expect(200);

      const body = response.body as TerminusResponse;
      // Liveness intentionally has no database or rabbitmq checks
      expect(body.info['database']).toBeUndefined();
      expect(body.info['rabbitmq']).toBeUndefined();
    });
  });

  // ── Response time SLA ───────────────────────────────────────────────────────

  describe('Health endpoint response time SLA', () => {
    const SLA_MS = 3_000; // health probes must respond within 3 s

    it('gateway /health/live responds within SLA', async () => {
      const start = Date.now();
      await request(GATEWAY_URL).get('/health/live').expect(200);
      expect(Date.now() - start).toBeLessThan(SLA_MS);
    });

    it('messaging /internal/health/live responds within SLA', async () => {
      const start = Date.now();
      await request(MESSAGING_INTERNAL_URL)
        .get('/internal/health/live')
        .expect(200);
      expect(Date.now() - start).toBeLessThan(SLA_MS);
    });

    it('messaging /internal/health/ready (DB + RabbitMQ probe) responds within SLA', async () => {
      const start = Date.now();
      await request(MESSAGING_INTERNAL_URL)
        .get('/internal/health/ready')
        .expect(200);
      // Readiness checks dial RabbitMQ; allow longer SLA
      expect(Date.now() - start).toBeLessThan(SLA_MS * 2);
    });
  });
});
