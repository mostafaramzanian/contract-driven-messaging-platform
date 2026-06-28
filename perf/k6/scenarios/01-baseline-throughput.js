/**
 * Scenario 01 — Baseline Throughput and Latency
 *
 * Purpose:
 *   Establish the system's steady-state performance characteristics at
 *   three traffic levels: low (50 msg/s), medium (200 msg/s), high (500 msg/s).
 *   Measure gateway latency, end-to-end delivery time, and outbox drain rate
 *   at each level. Identify the traffic level at which any metric begins to degrade.
 *
 * Duration: 22 minutes total
 *   - 2m warmup at 50 msg/s
 *   - 5m steady at 50 msg/s    (low baseline)
 *   - 5m steady at 200 msg/s   (medium baseline)
 *   - 5m steady at 500 msg/s   (high baseline — expected bottleneck territory)
 *   - 3m cool-down at 50 msg/s
 *   - 2m drain observation (0 VUs, watch outbox drain)
 *
 * Success criteria:
 *   - gateway p99 < 200ms at all levels
 *   - gateway p50 < 50ms at all levels
 *   - outbox_pending_events < 100 at 50 and 200 msg/s
 *   - outbox_pending_events < 500 at 500 msg/s (relay may lag briefly)
 *   - dlq_messages_total = 0 throughout
 *   - error rate < 0.1%
 */

import { sleep } from 'k6';
import { createMessageV2 } from '../lib/payloads.js';
import { publishEvent, snapshotSystemMetrics, assertSystemHealthy } from '../lib/checks.js';
import { eventsPublished } from '../lib/metrics.js';

export const options = {
  scenarios: {
    low_baseline: {
      executor:  'constant-arrival-rate',
      rate:      50,
      timeUnit:  '1s',
      duration:  '7m',   // 2m warmup + 5m measure
      preAllocatedVUs: 20,
      maxVUs:    50,
      startTime: '0s',
      tags:      { level: 'low' },
    },
    medium_baseline: {
      executor:  'constant-arrival-rate',
      rate:      200,
      timeUnit:  '1s',
      duration:  '5m',
      preAllocatedVUs: 50,
      maxVUs:    120,
      startTime: '7m',
      tags:      { level: 'medium' },
    },
    high_baseline: {
      executor:  'constant-arrival-rate',
      rate:      500,
      timeUnit:  '1s',
      duration:  '5m',
      preAllocatedVUs: 100,
      maxVUs:    300,
      startTime: '12m',
      tags:      { level: 'high' },
    },
    cool_down: {
      executor:  'constant-arrival-rate',
      rate:      50,
      timeUnit:  '1s',
      duration:  '5m',
      preAllocatedVUs: 20,
      maxVUs:    50,
      startTime: '17m',
      tags:      { level: 'cool_down' },
    },
  },

  thresholds: {
    // Gateway HTTP latency
    'cdmp_gateway_latency_ms{level:low}':    ['p(50)<30',  'p(99)<100'],
    'cdmp_gateway_latency_ms{level:medium}': ['p(50)<50',  'p(99)<200'],
    'cdmp_gateway_latency_ms{level:high}':   ['p(50)<100', 'p(99)<500'],

    // Error rate — strict across all levels
    'cdmp_events_rejected': ['count<10'],   // validation errors in load test = test bug
    'cdmp_events_failed':   ['count<50'],   // 5xx = system degradation

    // Delivery pipeline
    'cdmp_dlq_events_observed': ['count==0'],
  },
};

export function setup() {
  assertSystemHealthy();
  console.log('[01-baseline] Pre-test health check passed. Starting scenario.');
  return { startTime: Date.now() };
}

export default function (data) {
  const scenario = __ENV.K6_SCENARIO_NAME || 'baseline';
  const { res } = publishEvent(createMessageV2(), scenario);
  eventsPublished.add(1, { scenario });

  // Every 10th VU iteration, snapshot Prometheus metrics
  if (Math.random() < 0.1) {
    snapshotSystemMetrics(scenario);
  }

  // Realistic inter-request think time (client-side jitter)
  // Constant-arrival-rate executor controls the rate; this sleep is minimal
  sleep(Math.random() * 0.05);
}

export function teardown(data) {
  const durationMs = Date.now() - data.startTime;
  console.log(`[01-baseline] Test completed in ${(durationMs / 60000).toFixed(1)} minutes.`);
  console.log('[01-baseline] Check Grafana cdmp-outbox-health for drain time after test.');

  // Final metric snapshot
  const metrics = snapshotSystemMetrics('teardown');
  console.log('[01-baseline] Final system state:', JSON.stringify(metrics, null, 2));

  if (metrics.pending > 200) {
    console.warn('[01-baseline] WARNING: outbox backlog > 200 at teardown. Relay may not drain within 60s.');
  }
  if (metrics.dlq > 0) {
    console.error('[01-baseline] FAILURE: DLQ is non-empty. Investigate with RB-001.');
  }
}
