/**
 * Scenario 02 — Ramp to Peak (Bottleneck Identification)
 *
 * Purpose:
 *   Continuously increase traffic from 0 to 2,000 msg/s over 20 minutes.
 *   Identify the exact traffic level at which the FIRST metric breaches its
 *   threshold. That level defines the system's saturation point for the
 *   current configuration.
 *
 * What we expect to see (in order of likely failure):
 *   1. outbox_pending_events begins rising (relay is the first bottleneck)
 *   2. gateway p99 latency rises (Node.js event loop saturation)
 *   3. retry rate increases (consumer falling behind, RabbitMQ flow control)
 *   4. HTTP 5xx errors (gateway overloaded, reject incoming requests)
 *
 * Duration: 30 minutes
 *   - 20m ramp: 0 → 2000 msg/s
 *   - 10m observation at 2000 msg/s (or test auto-stops at saturation)
 *
 * The test uses abortOnFail on critical thresholds so it stops the moment
 * the system enters an unrecoverable state, avoiding long DLQ accumulation.
 */

import { sleep } from 'k6';
import { createMessageV2 } from '../lib/payloads.js';
import { publishEvent, snapshotSystemMetrics, assertSystemHealthy, queryPrometheus } from '../lib/checks.js';
import { eventsPublished, outboxBacklogGrowth } from '../lib/metrics.js';

export const options = {
  scenarios: {
    ramp_to_peak: {
      executor: 'ramping-arrival-rate',
      startRate: 10,
      timeUnit: '1s',
      preAllocatedVUs: 100,
      maxVUs: 1000,
      stages: [
        { duration: '2m',  target: 50   },
        { duration: '3m',  target: 200  },
        { duration: '3m',  target: 500  },
        { duration: '3m',  target: 800  },
        { duration: '3m',  target: 1200 },
        { duration: '3m',  target: 1600 },
        { duration: '3m',  target: 2000 },
        { duration: '10m', target: 2000 },  // hold at peak
      ],
    },
  },

  thresholds: {
    // Gateway must stay responsive — abortOnFail stops the test immediately
    'http_req_duration{name:POST /api/messages}': [
      { threshold: 'p(99)<2000', abortOnFail: true, delayAbortEval: '30s' },
    ],

    // DLQ is a hard stop — any DLQ message during ramp = the system has failed
    'cdmp_dlq_events_observed': [
      { threshold: 'count==0', abortOnFail: true, delayAbortEval: '10s' },
    ],

    // Error rate hard ceiling
    'cdmp_events_failed': [
      { threshold: 'rate<0.05', abortOnFail: true, delayAbortEval: '30s' },
    ],

    // Soft thresholds — recorded but don't abort
    'cdmp_gateway_latency_ms': ['p(50)<200', 'p(95)<800', 'p(99)<2000'],
    'cdmp_outbox_pending_current': ['value<1000'],
  },
};

// Saturation tracking state
let saturationLevel = null;
let saturationTime  = null;
let previousPending = 0;
let iterationCount  = 0;

export function setup() {
  assertSystemHealthy();
  console.log('[02-ramp] Ramp test starting. Will auto-abort on saturation.');
  return { startTime: Date.now() };
}

export default function () {
  iterationCount++;
  const { res } = publishEvent(createMessageV2(), 'ramp');
  eventsPublished.add(1);

  // Every 50 iterations, take a system snapshot and check for saturation
  if (iterationCount % 50 === 0) {
    const metrics = snapshotSystemMetrics('ramp');
    const currentPending = metrics.pending || 0;

    // Track outbox backlog growth rate
    if (previousPending !== null) {
      const growth = (currentPending - previousPending);
      outboxBacklogGrowth.add(growth);
    }
    previousPending = currentPending;

    // Log saturation point when outbox starts growing
    if (currentPending > 200 && saturationLevel === null) {
      const currentRate = queryPrometheus(
        'rate(cdmp_events_published[30s])'
      );
      saturationLevel = currentRate;
      saturationTime  = Date.now();
      console.warn(
        `[02-ramp] SATURATION DETECTED at ~${Math.round(currentRate || 0)} msg/s ` +
        `(outbox pending: ${currentPending}). ` +
        `This is the relay throughput ceiling for the current configuration.`
      );
    }
  }

  sleep(Math.random() * 0.02);
}

export function teardown(data) {
  const duration = ((Date.now() - data.startTime) / 60000).toFixed(1);
  console.log(`[02-ramp] Test ran for ${duration} minutes.`);

  if (saturationLevel !== null) {
    const saturationOffset = ((saturationTime - data.startTime) / 60000).toFixed(1);
    console.log(`[02-ramp] ══════════════════════════════════════════`);
    console.log(`[02-ramp] SATURATION POINT: ~${Math.round(saturationLevel)} msg/s`);
    console.log(`[02-ramp] Reached at:       t+${saturationOffset} minutes`);
    console.log(`[02-ramp] First bottleneck: outbox relay (outbox_pending_events > 200)`);
    console.log(`[02-ramp] Next step:        Scale relay instances or increase RELAY_BATCH_SIZE`);
    console.log(`[02-ramp] ══════════════════════════════════════════`);
  } else {
    console.log('[02-ramp] No saturation detected up to 2000 msg/s.');
    console.log('[02-ramp] System may support higher throughput. Increase maxRate and re-run.');
  }
}
