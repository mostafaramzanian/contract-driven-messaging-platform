/**
 * Integration test: Observability (Metrics + Tracing surface)
 *
 * Verifies that both services expose a working Prometheus /metrics
 * endpoint, that the required metric names from the observability spec
 * are present, and that driving a real message through the gateway →
 * RabbitMQ → messaging → PostgreSQL pipeline causes the relevant
 * counters/histograms to move.
 *
 * This suite intentionally runs with OTEL_SDK_DISABLED=true (see
 * docker-compose.test.yml) — there is no OTel Collector in the test
 * stack. We are validating the Prometheus metrics surface and the
 * application-level instrumentation hooks, not the OTLP export path
 * itself (that would require a collector + Jaeger in the test compose
 * file, which is out of scope for a fast integration suite).
 */

import request from 'supertest';
import { waitForHttpReady } from '../utils/wait-for-health';

const GATEWAY_URL = process.env.TEST_GATEWAY_URL ?? 'http://127.0.0.1:3005';
const MESSAGING_INTERNAL_URL =
  process.env.MESSAGING_HEALTH_URL ?? 'http://127.0.0.1:3006';

const REQUIRED_METRIC_NAMES = [
  'messages_processed_total',
  'messages_failed_total',
  'dlq_messages_total',
  'retry_count_total',
  'processing_duration_seconds',
  'outbox_pending_events',
];

/** Parse Prometheus text exposition format into a map of metric name -> raw block. */
function parseMetricsText(text: string): Map<string, string> {
  const blocks = new Map<string, string>();
  const lines = text.split('\n');
  let currentName: string | null = null;
  let currentBlock: string[] = [];

  for (const line of lines) {
    const helpMatch = line.match(/^# HELP (\w+)/);
    if (helpMatch) {
      if (currentName) blocks.set(currentName, currentBlock.join('\n'));
      currentName = helpMatch[1];
      currentBlock = [line];
      continue;
    }
    if (currentName) currentBlock.push(line);
  }
  if (currentName) blocks.set(currentName, currentBlock.join('\n'));
  return blocks;
}

function extractMetricValue(
  metricsText: string,
  metricName: string,
  labelFilter?: Record<string, string>,
): number | undefined {
  const lines = metricsText.split('\n');
  for (const line of lines) {
    if (!line.startsWith(metricName)) continue;
    if (labelFilter) {
      const matchesAll = Object.entries(labelFilter).every(([k, v]) =>
        line.includes(`${k}="${v}"`),
      );
      if (!matchesAll) continue;
    }
    const parts = line.trim().split(' ');
    const value = Number.parseFloat(parts[parts.length - 1]);
    if (!Number.isNaN(value)) return value;
  }
  return undefined;
}

describe('Observability (integration)', () => {
  beforeAll(async () => {
    await waitForHttpReady(`${GATEWAY_URL}/api`);
    await waitForHttpReady(`${MESSAGING_INTERNAL_URL}/internal/health/live`);
  }, 40_000);

  // ── /metrics endpoint exposure ──────────────────────────────────────────

  describe('Gateway /metrics', () => {
    it('returns 200 with Prometheus text exposition content-type', async () => {
      const response = await request(GATEWAY_URL)
        .get('/metrics')
        .expect(200);

      expect(response.headers['content-type']).toMatch(/text\/plain/);
      expect(response.text.length).toBeGreaterThan(0);
    });

    it('is not behind the global /api prefix', async () => {
      // Health and metrics endpoints must be reachable without the
      // global API prefix so scrapers/probes don't need to know it.
      await request(GATEWAY_URL).get('/metrics').expect(200);
    });

    it('exposes default Node.js process metrics', async () => {
      const response = await request(GATEWAY_URL).get('/metrics').expect(200);
      expect(response.text).toMatch(/process_process_resident_memory_bytes/);
    });

    it('exposes the service label on emitted metrics', async () => {
      const response = await request(GATEWAY_URL).get('/metrics').expect(200);
      expect(response.text).toMatch(/service="gateway"/);
    });
  });

  describe('Messaging /metrics', () => {
    it('returns 200 with Prometheus text exposition content-type', async () => {
      const response = await request(MESSAGING_INTERNAL_URL)
        .get('/metrics')
        .expect(200);

      expect(response.headers['content-type']).toMatch(/text\/plain/);
    });

    it('exposes every required metric name from the observability spec', async () => {
      const response = await request(MESSAGING_INTERNAL_URL)
        .get('/metrics')
        .expect(200);

      const blocks = parseMetricsText(response.text);
      for (const metricName of REQUIRED_METRIC_NAMES) {
        expect(blocks.has(metricName)).toBe(true);
      }
    });

    it('exposes the service label as "messaging"', async () => {
      const response = await request(MESSAGING_INTERNAL_URL)
        .get('/metrics')
        .expect(200);
      expect(response.text).toMatch(/service="messaging"/);
    });

    it('outbox_pending_events is a non-negative number', async () => {
      const response = await request(MESSAGING_INTERNAL_URL)
        .get('/metrics')
        .expect(200);

      const value = extractMetricValue(response.text, 'outbox_pending_events');
      expect(value).toBeDefined();
      expect(value as number).toBeGreaterThanOrEqual(0);
    });
  });

  // ── Driving real traffic and observing metric movement ──────────────────

  describe('Metric counters move with real traffic', () => {
    it('messages_processed_total increases after a successful test-rabbit emission and processing', async () => {
      const before = await request(MESSAGING_INTERNAL_URL).get('/metrics');
      const beforeValue =
        extractMetricValue(before.text, 'messages_processed_total', {
          event_type: 'test-rabbit',
        }) ?? 0;

      await request(GATEWAY_URL).get('/api/test-rabbit').expect(200);

      // Give the async consumer time to process and persist the message.
      await new Promise((resolve) => setTimeout(resolve, 3_000));

      const after = await request(MESSAGING_INTERNAL_URL).get('/metrics');
      const afterValue =
        extractMetricValue(after.text, 'messages_processed_total', {
          event_type: 'test-rabbit',
        }) ?? 0;

      expect(afterValue).toBeGreaterThan(beforeValue);
    }, 15_000);

    it('gateway messages_processed_total (outcome=emitted) increases after emitting', async () => {
      const before = await request(GATEWAY_URL).get('/metrics');
      const beforeValue =
        extractMetricValue(before.text, 'messages_processed_total', {
          outcome: 'emitted',
        }) ?? 0;

      await request(GATEWAY_URL).get('/api/test-rabbit').expect(200);

      const after = await request(GATEWAY_URL).get('/metrics');
      const afterValue =
        extractMetricValue(after.text, 'messages_processed_total', {
          outcome: 'emitted',
        }) ?? 0;

      expect(afterValue).toBeGreaterThan(beforeValue);
    }, 15_000);

    it('processing_duration_seconds histogram records observations after traffic', async () => {
      await request(GATEWAY_URL).get('/api/test-rabbit').expect(200);
      await new Promise((resolve) => setTimeout(resolve, 3_000));

      const response = await request(MESSAGING_INTERNAL_URL).get('/metrics');
      const count = extractMetricValue(
        response.text,
        'processing_duration_seconds_count',
        { event_type: 'test-rabbit' },
      );

      expect(count).toBeDefined();
      expect(count as number).toBeGreaterThan(0);
    }, 15_000);
  });
});
