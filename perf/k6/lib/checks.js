/**
 * Shared check helpers and threshold assertions.
 * Import these in every scenario to get consistent pass/fail criteria.
 */
import { check } from 'k6';
import http from 'k6/http';
import {
  eventsAccepted,
  eventsRejected,
  eventsFailed,
  gatewayLatency,
  outboxPendingGauge,
  dlqTotal,
} from './metrics.js';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:3000';
const PROM_URL = __ENV.PROMETHEUS_URL || 'http://localhost:9090';

/**
 * Publish a single event to the gateway and record all metrics.
 * Returns the response object.
 */
export function publishEvent(payload, scenario = 'default') {
  const start = Date.now();
  const res = http.post(
    `${BASE_URL}/api/messages`,
    JSON.stringify(payload),
    {
      headers: {
        'Content-Type': 'application/json',
        'x-scenario':   scenario,
      },
      timeout: '5s',
      tags: { scenario },
    }
  );

  gatewayLatency.add(Date.now() - start, { scenario });

  const passed = check(res, {
    'gateway status is 202': (r) => r.status === 202,
    'gateway response has eventId': (r) => {
      try { return JSON.parse(r.body).eventId !== undefined; }
      catch { return false; }
    },
  });

  if (res.status === 202) {
    eventsAccepted.add(1, { scenario });
  } else if (res.status >= 400 && res.status < 500) {
    eventsRejected.add(1, { scenario });
  } else {
    eventsFailed.add(1, { scenario });
  }

  return { res, passed };
}

/**
 * Query Prometheus for a current metric value.
 * Returns the numeric value or null if unavailable.
 */
export function queryPrometheus(promql) {
  try {
    const res = http.get(
      `${PROM_URL}/api/v1/query?query=${encodeURIComponent(promql)}`,
      { timeout: '3s', tags: { type: 'prometheus' } }
    );
    if (res.status !== 200) return null;
    const data = JSON.parse(res.body);
    const result = data?.data?.result;
    if (!result || result.length === 0) return null;
    return parseFloat(result[0].value[1]);
  } catch {
    return null;
  }
}

/**
 * Snapshot all relevant Prometheus metrics for the current moment.
 * Call this periodically during a test to track system health.
 */
export function snapshotSystemMetrics(scenario = 'default') {
  const pending = queryPrometheus('outbox_pending_events{source="gateway"}');
  const dlq     = queryPrometheus('increase(dlq_messages_total[1m])');
  const relay   = queryPrometheus('rate(outbox_published_total{source="gateway"}[30s])');
  const p99     = queryPrometheus(
    'histogram_quantile(0.99, rate(outbox_relay_latency_ms_bucket[30s]))'
  );
  const confirms = queryPrometheus('increase(publisher_confirm_failures_total[1m])');

  if (pending !== null)  outboxPendingGauge.add(pending,      { scenario });
  if (dlq !== null && dlq > 0)     dlqTotal.add(dlq,          { scenario });
  if (p99 !== null)      relayLatencyP99.add(p99,             { scenario });

  return { pending, dlq, relay, p99, confirms };
}

/**
 * Check if the system is in a healthy baseline state before starting a test.
 * Fails fast rather than running a load test against an already-degraded system.
 */
export function assertSystemHealthy() {
  const health = http.get(`${BASE_URL}/health`, { timeout: '5s' });
  check(health, {
    'system health check passes': (r) => r.status === 200,
  });

  const pending = queryPrometheus('outbox_pending_events{source="gateway"}');
  check({ pending }, {
    'outbox is not already backed up': ({ pending }) => pending === null || pending < 50,
  });

  const dlq = queryPrometheus('rabbitmq_queue_messages{queue="messaging.dlq"}');
  check({ dlq }, {
    'DLQ is empty before test': ({ dlq }) => dlq === null || dlq === 0,
  });
}

export { BASE_URL, PROM_URL };
