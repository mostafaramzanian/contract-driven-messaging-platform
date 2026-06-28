/**
 * Scenario 04 — Retry Amplification Under Consumer Failure
 *
 * Purpose:
 *   Measure how a consumer-side failure amplifies message volume through the
 *   retry queue. When a consumer fails and retries, the effective message
 *   throughput through the broker becomes:
 *     effective_volume = publish_rate × (1 + avg_retry_count_per_message)
 *
 *   At 5 retries per message, a 100 msg/s publish rate generates 600 msg/s
 *   of total broker throughput. This test measures:
 *     - Retry amplification factor at the broker level
 *     - Queue depth under retry storm
 *     - Whether the retry queue's per-message TTL correctly staggers redeliveries
 *     - DLQ growth rate when consumer failures are sustained
 *     - Whether the idempotency layer handles duplicate deliveries correctly
 *
 * Test structure:
 *   Phase 1 (5m): Publish valid events. Consumer operates normally.
 *                 Establish baseline retry rate (should be ~0).
 *   Phase 2 (5m): Enable consumer failure simulation (FAIL_RATE env var or test hook).
 *                 Measure retry amplification and queue depth growth.
 *   Phase 3 (5m): Disable consumer failure. Consumer recovers.
 *                 Measure how long until retry storm subsides.
 *   Phase 4 (5m): Normal operation. Confirm system returns to baseline.
 *
 * NOTE: Phase 2 requires a test hook in the consumer that artificially fails
 * on messages tagged with 'force-transient-failure'. This hook must be enabled
 * in the test environment (not production). The hook is controlled by the
 * CONSUMER_FAIL_RATE environment variable (0.0–1.0).
 */

import { sleep } from 'k6';
import http from 'k6/http';
import { check } from 'k6';
import { createMessageV2 } from '../lib/payloads.js';
import {
  publishEvent,
  snapshotSystemMetrics,
  assertSystemHealthy,
  queryPrometheus,
  BASE_URL
} from '../lib/checks.js';
import {
  eventsPublished,
  retryRate,
  dlqTotal,
  idempotencyHits,
} from '../lib/metrics.js';

const NORMAL_RATE       = 100;  // msg/s
const PHASE_DURATION    = '5m';

export const options = {
  scenarios: {
    normal_baseline: {
      executor:        'constant-arrival-rate',
      rate:            NORMAL_RATE,
      timeUnit:        '1s',
      duration:        PHASE_DURATION,
      preAllocatedVUs: 30,
      maxVUs:          60,
      startTime:       '0s',
      tags:            { phase: 'baseline' },
    },
    consumer_failure: {
      executor:        'constant-arrival-rate',
      rate:            NORMAL_RATE,
      timeUnit:        '1s',
      duration:        PHASE_DURATION,
      preAllocatedVUs: 30,
      maxVUs:          60,
      startTime:       '5m',
      tags:            { phase: 'failure' },
    },
    recovery_window: {
      executor:        'constant-arrival-rate',
      rate:            NORMAL_RATE,
      timeUnit:        '1s',
      duration:        PHASE_DURATION,
      preAllocatedVUs: 30,
      maxVUs:          60,
      startTime:       '10m',
      tags:            { phase: 'recovery' },
    },
    steady_state: {
      executor:        'constant-arrival-rate',
      rate:            NORMAL_RATE,
      timeUnit:        '1s',
      duration:        PHASE_DURATION,
      preAllocatedVUs: 30,
      maxVUs:          60,
      startTime:       '15m',
      tags:            { phase: 'steady_state' },
    },
  },

  thresholds: {
    // During failure phase, some retries are expected — but the budget must hold
    // DLQ events during the failure phase = retry budget exhausted
    // We allow up to 5 DLQ events (some messages may exhaust the budget intentionally)
    'cdmp_dlq_events_observed':           ['count<10'],

    // Idempotency should catch all duplicates delivered by the retry mechanism
    // A high idempotency hit rate during recovery is expected and correct
    // A zero hit rate during failure phase means retries aren't being caught
    'cdmp_idempotency_duplicates':        ['count>0'], // must be > 0 if retries occurred

    // After recovery, retry rate must return to baseline
    'cdmp_retry_rate{phase:steady_state}': ['value<0.01'],

    // Gateway must remain available throughout
    'cdmp_gateway_latency_ms':            ['p(99)<300'],
    'cdmp_events_failed':                 ['count<5'],
  },
};

export function setup() {
  assertSystemHealthy();

  // Verify test hook is available
  const hookCheck = http.get(`${BASE_URL}/internal/test-hooks/status`, { timeout: '3s' });
  if (hookCheck.status !== 200) {
    console.warn('[04-retry] Test hook endpoint not available. Consumer failures will be simulated via tagged payloads only.');
  }

  return {
    startTime:     Date.now(),
    failureActive: false,
  };
}

export function setup_consumer_failure() {
  // Enable consumer failure simulation at the start of the failure phase
  const res = http.post(
    `${BASE_URL}/internal/test-hooks/consumer-fail-rate`,
    JSON.stringify({ rate: 0.8 }),  // 80% of messages will fail transiently
    { headers: { 'Content-Type': 'application/json' }, timeout: '3s' }
  );
  if (res.status !== 200) {
    console.warn('[04-retry] Could not enable consumer failure hook. Using payload tags instead.');
  } else {
    console.log('[04-retry] Consumer failure hook enabled at 80% fail rate.');
  }
}

let lastRetryCount   = 0;
let phaseStartRetry  = {};
let iterationCount   = 0;

export default function () {
  iterationCount++;
  const phase = __ENV.K6_SCENARIO_NAME?.replace(/[^a-z_]/g, '') || 'baseline';

  // During failure phase, tag messages to trigger the consumer test hook
  const payload = (phase === 'consumer_failure')
    ? createMessageV2({ payload: { tags: ['force-transient-failure', 'load-test'] } })
    : createMessageV2({ payload: { tags: ['load-test'] } });

  publishEvent(payload, phase);
  eventsPublished.add(1, { phase });

  // Snapshot system state every 30 iterations
  if (iterationCount % 30 === 0) {
    const metrics = snapshotSystemMetrics(phase);

    // Calculate retry amplification
    const currentRetryTotal = queryPrometheus('retry_count_total') || 0;
    const retryDelta        = currentRetryTotal - lastRetryCount;
    lastRetryCount          = currentRetryTotal;
    retryRate.add(retryDelta, { phase });

    // Check idempotency catch rate
    const idemHits   = queryPrometheus('increase(idempotency_duplicates_prevented_total[30s])') || 0;
    const redeliveries = queryPrometheus('increase(messages_redelivered_total[30s])') || 0;

    if (idemHits > 0) idempotencyHits.add(idemHits, { phase });

    if (phase === 'consumer_failure' && iterationCount % 300 === 0) {
      const catchRate = redeliveries > 0
        ? ((idemHits / redeliveries) * 100).toFixed(1)
        : 'N/A';
      const retryAmp = queryPrometheus(
        'rate(retry_count_total[30s]) / rate(messages_processed_total[30s])'
      );
      console.log(`[04-retry] Failure phase metrics:`);
      console.log(`  Retry amplification factor: ${retryAmp?.toFixed(2) || 'N/A'}`);
      console.log(`  Idempotency catch rate:     ${catchRate}%`);
      console.log(`  Outbox pending:             ${metrics.pending}`);
      console.log(`  DLQ events this window:     ${metrics.dlq}`);
    }
  }

  sleep(Math.random() * 0.02);
}

export function teardown(data) {
  // Disable consumer failure hook
  http.post(
    `${BASE_URL}/internal/test-hooks/consumer-fail-rate`,
    JSON.stringify({ rate: 0 }),
    { headers: { 'Content-Type': 'application/json' }, timeout: '3s' }
  );

  const totalDuration = ((Date.now() - data.startTime) / 60000).toFixed(1);
  const finalMetrics  = snapshotSystemMetrics('teardown');
  const totalRetries  = queryPrometheus('retry_count_total') || 0;
  const totalPublished = queryPrometheus('outbox_published_total') || 0;
  const ampFactor     = totalPublished > 0
    ? ((totalRetries / totalPublished) * 100).toFixed(1)
    : 'N/A';

  console.log(`[04-retry] ══ Results ════════════════════════════════`);
  console.log(`[04-retry] Duration:             ${totalDuration} min`);
  console.log(`[04-retry] Total published:      ${Math.round(totalPublished)}`);
  console.log(`[04-retry] Total retry messages: ${Math.round(totalRetries)}`);
  console.log(`[04-retry] Retry amplification:  ${ampFactor}% overhead`);
  console.log(`[04-retry] Final DLQ count:      ${finalMetrics.dlq}`);
  console.log(`[04-retry] Final pending:        ${finalMetrics.pending}`);
  console.log('');
  console.log('[04-retry] Interpretation:');
  console.log('  - Retry amplification measures the broker throughput overhead from retries.');
  console.log('  - At 80% transient failure rate + 5 retry attempts:');
  console.log('    Expected amplification ≈ 4× (each message tries 5 times).');
  console.log('  - If idempotency catch rate < 95%: duplicate business writes are occurring.');
  console.log('  - If DLQ > 0 during the failure phase: retry budget is too small for the');
  console.log('    transient failure window. Consider increasing MAX_RETRY_ATTEMPTS.');
  console.log(`[04-retry] ══════════════════════════════════════════`);
}
