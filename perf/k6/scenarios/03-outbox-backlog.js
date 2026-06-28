/**
 * Scenario 03 — Outbox Backlog Accumulation and Drain
 *
 * Purpose:
 *   Simulate a relay constraint (slow broker, single relay instance at batch limit)
 *   while the gateway continues accepting events at high rate. Measure:
 *     - How fast the outbox accumulates when the relay cannot keep up
 *     - How long it takes the system to drain after traffic stops
 *     - Whether retry budgets expire during a sustained backlog (DLQ risk window)
 *
 * Test structure:
 *   Phase 1 (5m): Publish at 2× the relay's sustainable throughput.
 *                 The outbox will grow. Measure growth rate.
 *   Phase 2 (0 VUs, 10m): Stop publishing. Watch the relay drain.
 *                 Measure drain rate and time to empty.
 *   Phase 3 (5m): Publish at normal rate. Confirm system is back to baseline.
 *
 * Key measurements:
 *   - outbox_pending_events peak value
 *   - Backlog growth rate (rows/second) during Phase 1
 *   - Drain rate (rows/second) during Phase 2
 *   - Time to drain from peak to < 20 pending
 *   - DLQ events during Phase 1 (messages should NOT expire during short backlog)
 */

import { sleep } from 'k6';
import { createMessageV2 } from '../lib/payloads.js';
import { publishEvent, snapshotSystemMetrics, assertSystemHealthy, queryPrometheus, PROM_URL } from '../lib/checks.js';
import { eventsPublished, outboxBacklogGrowth, outboxPendingGauge } from '../lib/metrics.js';

// Target: 2× the estimated relay ceiling (~250/s → publish at 600/s)
const OVERLOAD_RATE      = 600;   // msg/s
const OVERLOAD_DURATION  = '5m';
const DRAIN_WINDOW       = '10m'; // observe relay drain with no new publishes
const RECOVERY_RATE      = 100;   // msg/s — normal load after drain
const RECOVERY_DURATION  = '5m';

export const options = {
  scenarios: {
    // Phase 1: Overload the relay
    overload: {
      executor:        'constant-arrival-rate',
      rate:            OVERLOAD_RATE,
      timeUnit:        '1s',
      duration:        OVERLOAD_DURATION,
      preAllocatedVUs: 150,
      maxVUs:          400,
      startTime:       '0s',
      tags:            { phase: 'overload' },
    },
    // Phase 2: Drain observation (uses a single low-rate poller to keep metrics flowing)
    drain_observe: {
      executor:        'constant-arrival-rate',
      rate:            1,       // minimal, just to keep k6 running and snapshot metrics
      timeUnit:        '5s',
      duration:        DRAIN_WINDOW,
      preAllocatedVUs: 1,
      maxVUs:          2,
      startTime:       OVERLOAD_DURATION,
      tags:            { phase: 'drain' },
    },
    // Phase 3: Recovery — normal load post-drain
    recovery: {
      executor:        'constant-arrival-rate',
      rate:            RECOVERY_RATE,
      timeUnit:        '1s',
      duration:        RECOVERY_DURATION,
      preAllocatedVUs: 30,
      maxVUs:          80,
      startTime:       '15m',
      tags:            { phase: 'recovery' },
    },
  },

  thresholds: {
    // DLQ must remain empty even during backlog
    // (messages should not expire during a 5-minute backlog window)
    'cdmp_dlq_events_observed': ['count==0'],

    // Gateway must still accept requests even during relay backlog
    // (the outbox pattern decouples gateway availability from relay throughput)
    'cdmp_gateway_latency_ms{phase:overload}': ['p(99)<500'],
    'cdmp_events_failed':                      ['count<20'],

    // After recovery, outbox must have drained
    'cdmp_outbox_pending_current': ['value<50'],
  },
};

const phaseMetrics = {
  overload: { peakPending: 0, peakTime: null },
  drain:    { drainStartPending: null, drainRate: [] },
};

export function setup() {
  assertSystemHealthy();
  console.log('[03-backlog] Starting outbox backlog accumulation test.');
  console.log(`[03-backlog] Overload phase: ${OVERLOAD_RATE} msg/s for ${OVERLOAD_DURATION}`);
  console.log(`[03-backlog] Drain window: ${DRAIN_WINDOW} with 0 publish VUs`);
  return { startTime: Date.now() };
}

export default function () {
  const phase = __ENV.K6_SCENARIO_NAME?.includes('drain') ? 'drain'
              : __ENV.K6_SCENARIO_NAME?.includes('recovery') ? 'recovery'
              : 'overload';

  if (phase === 'drain') {
    // During drain phase: only snapshot metrics, don't publish
    const metrics = snapshotSystemMetrics('drain');
    const pending = metrics.pending || 0;
    outboxPendingGauge.add(pending, { phase: 'drain' });

    if (phaseMetrics.drain.drainStartPending === null && pending > 0) {
      phaseMetrics.drain.drainStartPending = pending;
    }

    if (pending === 0) {
      console.log('[03-backlog] ✓ Outbox fully drained.');
    }
    sleep(5);
    return;
  }

  // Overload and recovery phases: publish events
  publishEvent(createMessageV2(), phase);
  eventsPublished.add(1, { phase });

  if (Math.random() < 0.05) {
    const metrics = snapshotSystemMetrics(phase);
    const pending = metrics.pending || 0;
    outboxPendingGauge.add(pending, { phase });
    outboxBacklogGrowth.add(pending, { phase });

    if (phase === 'overload' && pending > phaseMetrics.overload.peakPending) {
      phaseMetrics.overload.peakPending = pending;
      phaseMetrics.overload.peakTime    = Date.now();
    }
  }

  sleep(Math.random() * 0.02);
}

export function teardown(data) {
  const totalDuration = ((Date.now() - data.startTime) / 60000).toFixed(1);
  const metrics       = snapshotSystemMetrics('teardown');

  console.log(`[03-backlog] ══ Results ════════════════════════════════`);
  console.log(`[03-backlog] Total duration:       ${totalDuration} min`);
  console.log(`[03-backlog] Peak outbox backlog:  ${phaseMetrics.overload.peakPending} rows`);
  console.log(`[03-backlog] DLQ events observed:  check cdmp_dlq_events_observed metric`);
  console.log(`[03-backlog] Final pending count:  ${metrics.pending}`);
  console.log('');
  console.log('[03-backlog] Interpretation:');
  console.log(`  - A peak of ${phaseMetrics.overload.peakPending} rows at ${OVERLOAD_RATE} msg/s`);
  console.log(`    means the relay sustains ~${OVERLOAD_RATE - (phaseMetrics.overload.peakPending / 300)} msg/s`);
  console.log('  - If DLQ events > 0: retry budget expiring during backlog.');
  console.log('    Consider increasing MAX_RETRY_ATTEMPTS or retry window.');
  console.log('  - Drain rate indicates relay throughput ceiling without incoming load.');
  console.log(`[03-backlog] ══════════════════════════════════════════`);
}
