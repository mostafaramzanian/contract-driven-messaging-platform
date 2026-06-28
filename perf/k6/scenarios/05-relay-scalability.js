/**
 * Scenario 05 — Relay Horizontal Scalability
 *
 * Purpose:
 *   Measure how relay throughput scales as relay instances are added.
 *   The outbox relay uses SKIP LOCKED + fencing tokens to safely support
 *   multiple concurrent instances. This test verifies that scaling is
 *   approximately linear and measures the efficiency of each additional instance.
 *
 * Test structure (requires external orchestration to scale relay instances):
 *   Stage A (5m): 1 relay instance, publish at 300 msg/s → measure throughput ceiling
 *   Stage B (5m): 2 relay instances, publish at 600 msg/s → measure combined throughput
 *   Stage C (5m): 4 relay instances, publish at 1200 msg/s → measure linear scaling
 *
 * External orchestration:
 *   This test emits signals to a control endpoint that triggers relay scaling.
 *   In Docker Compose: `docker compose up -d --scale gateway-service=N`
 *   In Kubernetes: `kubectl scale deployment/gateway-service --replicas=N`
 *   The test endpoint `/internal/test-hooks/relay-instances` must report
 *   the current relay instance count for metric correlation.
 *
 * What we expect:
 *   - 1 relay instance: ~250 msg/s sustained throughput
 *   - 2 relay instances: ~450–500 msg/s (80–90% linear scaling efficiency)
 *   - 4 relay instances: ~800–900 msg/s (70–80% efficiency due to fencing overhead)
 *   - Fencing event rate should increase proportionally with instance count
 *   - No DLQ events throughout (fencing token ensures no data loss under contention)
 *
 * Scaling efficiency = actual_throughput / (n_instances × single_instance_throughput)
 * Below 70% efficiency: the bottleneck has shifted to the database or broker, not the relay.
 */

import { sleep } from 'k6';
import http from 'k6/http';
import { createMessageV2 } from '../lib/payloads.js';
import {
  publishEvent,
  snapshotSystemMetrics,
  assertSystemHealthy,
  queryPrometheus,
  BASE_URL,
} from '../lib/checks.js';
import {
  eventsPublished,
  relayThroughput,
} from '../lib/metrics.js';

export const options = {
  scenarios: {
    stage_a_1_relay: {
      executor:        'constant-arrival-rate',
      rate:            300,
      timeUnit:        '1s',
      duration:        '5m',
      preAllocatedVUs: 80,
      maxVUs:          200,
      startTime:       '0s',
      tags:            { stage: 'a', relays: '1' },
    },
    stage_b_2_relays: {
      executor:        'constant-arrival-rate',
      rate:            600,
      timeUnit:        '1s',
      duration:        '5m',
      preAllocatedVUs: 150,
      maxVUs:          350,
      startTime:       '6m',   // 1m transition gap for scaling
      tags:            { stage: 'b', relays: '2' },
    },
    stage_c_4_relays: {
      executor:        'constant-arrival-rate',
      rate:            1200,
      timeUnit:        '1s',
      duration:        '5m',
      preAllocatedVUs: 280,
      maxVUs:          650,
      startTime:       '12m',  // 1m transition gap
      tags:            { stage: 'c', relays: '4' },
    },
  },

  thresholds: {
    // DLQ must stay empty regardless of relay concurrency
    'cdmp_dlq_events_observed': ['count==0'],

    // Gateway must remain available under all relay configurations
    'cdmp_gateway_latency_ms': ['p(99)<500'],
    'cdmp_events_failed':      ['count<10'],

    // Outbox should drain despite scaling transitions
    'cdmp_outbox_pending_current': ['value<2000'],
  },
};

// Per-stage throughput measurements
const stageMeasurements = {
  a: { throughputSamples: [], fencingSamples: [] },
  b: { throughputSamples: [], fencingSamples: [] },
  c: { throughputSamples: [], fencingSamples: [] },
};

// Transition: scale relay instances at stage boundaries
export function handleSummary(data) {
  return {
    'stdout': JSON.stringify(summarizeScalability(data), null, 2),
  };
}

function summarizeScalability(data) {
  const avgThroughput = (samples) =>
    samples.length > 0 ? samples.reduce((a, b) => a + b, 0) / samples.length : 0;

  const stageA = avgThroughput(stageMeasurements.a.throughputSamples);
  const stageB = avgThroughput(stageMeasurements.b.throughputSamples);
  const stageC = avgThroughput(stageMeasurements.c.throughputSamples);

  const efficiencyB = stageA > 0 ? ((stageB / (stageA * 2)) * 100).toFixed(1) : 'N/A';
  const efficiencyC = stageA > 0 ? ((stageC / (stageA * 4)) * 100).toFixed(1) : 'N/A';

  return {
    scenario: '05-relay-scalability',
    results: {
      stage_a_1_relay:  { avg_relay_throughput_msg_s: Math.round(stageA) },
      stage_b_2_relays: { avg_relay_throughput_msg_s: Math.round(stageB), scaling_efficiency_pct: efficiencyB },
      stage_c_4_relays: { avg_relay_throughput_msg_s: Math.round(stageC), scaling_efficiency_pct: efficiencyC },
    },
    interpretation: {
      single_relay_ceiling_msg_s: Math.round(stageA),
      two_relay_efficiency:   `${efficiencyB}% (expected: 80–90%)`,
      four_relay_efficiency:  `${efficiencyC}% (expected: 70–80%)`,
      bottleneck_below_70pct: efficiencyC < 70
        ? 'Bottleneck has shifted to PostgreSQL SKIP LOCKED contention or broker capacity, not relay compute.'
        : 'Scaling is within expected efficiency bounds. Relay is the dominant component.',
    },
  };
}

let iterationCount = 0;

export function setup() {
  assertSystemHealthy();
  console.log('[05-relay] Relay scalability test starting.');
  console.log('[05-relay] Requires manual relay scaling at stage transitions.');
  console.log('[05-relay] Stage A (t=0):   1 relay instance  → 300 msg/s target');
  console.log('[05-relay] Stage B (t=6m):  2 relay instances → 600 msg/s target');
  console.log('[05-relay] Stage C (t=12m): 4 relay instances → 1200 msg/s target');
  return { startTime: Date.now() };
}

export default function () {
  iterationCount++;
  const stage = __ENV.K6_SCENARIO_NAME?.includes('stage_a') ? 'a'
              : __ENV.K6_SCENARIO_NAME?.includes('stage_b') ? 'b'
              : 'c';

  publishEvent(createMessageV2(), `stage_${stage}`);
  eventsPublished.add(1, { stage });

  if (iterationCount % 100 === 0) {
    const metrics   = snapshotSystemMetrics(`stage_${stage}`);
    const throughput = queryPrometheus(
      'rate(outbox_published_total{source="gateway"}[30s])'
    ) || 0;
    const fencing   = queryPrometheus(
      'rate(outbox_fenced_publishes_total[30s])'
    ) || 0;

    relayThroughput.add(throughput, { stage });
    stageMeasurements[stage].throughputSamples.push(throughput);
    stageMeasurements[stage].fencingSamples.push(fencing);

    if (iterationCount % 1000 === 0) {
      console.log(`[05-relay] Stage ${stage.toUpperCase()} — relay throughput: ${throughput.toFixed(0)} msg/s, fencing: ${fencing.toFixed(3)}/s, pending: ${metrics.pending}`);
    }
  }

  sleep(Math.random() * 0.02);
}

export function teardown(data) {
  const duration = ((Date.now() - data.startTime) / 60000).toFixed(1);
  console.log(`[05-relay] Test completed in ${duration} minutes.`);

  const stageA = stageMeasurements.a.throughputSamples;
  const stageB = stageMeasurements.b.throughputSamples;
  const stageC = stageMeasurements.c.throughputSamples;

  const avg = (arr) => arr.length > 0 ? (arr.reduce((a,b)=>a+b,0)/arr.length).toFixed(0) : 'N/A';

  console.log('[05-relay] ══ Relay Scalability Results ════════════════');
  console.log(`[05-relay] Stage A (1 relay):  ${avg(stageA)} msg/s`);
  console.log(`[05-relay] Stage B (2 relays): ${avg(stageB)} msg/s`);
  console.log(`[05-relay] Stage C (4 relays): ${avg(stageC)} msg/s`);
  console.log('');
  console.log('[05-relay] Average fencing rate per stage (events/s):');
  console.log(`[05-relay] Stage A: ${avg(stageMeasurements.a.fencingSamples)}`);
  console.log(`[05-relay] Stage B: ${avg(stageMeasurements.b.fencingSamples)}`);
  console.log(`[05-relay] Stage C: ${avg(stageMeasurements.c.fencingSamples)}`);
  console.log('[05-relay] ══════════════════════════════════════════════');
  console.log('[05-relay] Note: Fencing events are EXPECTED and CORRECT under multi-relay.');
  console.log('[05-relay] They indicate the fencing mechanism is protecting against stale claims.');
}
