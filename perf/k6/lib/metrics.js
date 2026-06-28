/**
 * Custom k6 metrics for contract-driven-messaging-platform load tests.
 * Import this in every scenario file.
 */
import { Counter, Gauge, Rate, Trend } from 'k6/metrics';

// ── Throughput ──────────────────────────────────────────────────────────────
export const eventsPublished     = new Counter('cdmp_events_published');
export const eventsAccepted      = new Counter('cdmp_events_accepted');      // HTTP 202
export const eventsRejected      = new Counter('cdmp_events_rejected');      // HTTP 4xx
export const eventsFailed        = new Counter('cdmp_events_failed');        // HTTP 5xx / timeout

// ── HTTP latency (per endpoint) ─────────────────────────────────────────────
export const gatewayLatency      = new Trend('cdmp_gateway_latency_ms',  true);
export const healthLatency       = new Trend('cdmp_health_latency_ms',   true);

// ── End-to-end delivery ─────────────────────────────────────────────────────
// Measured by polling /internal/status/:correlationId after publish
export const e2eDeliveryTime     = new Trend('cdmp_e2e_delivery_ms',     true);
export const e2eDeliveryRate     = new Rate('cdmp_e2e_delivery_success');

// ── Outbox health (polled from Prometheus) ──────────────────────────────────
export const outboxPendingGauge  = new Gauge('cdmp_outbox_pending_current');
export const outboxBacklogGrowth = new Trend('cdmp_outbox_backlog_growth_rate'); // rows/s

// ── Reliability signals ─────────────────────────────────────────────────────
export const retryRate           = new Trend('cdmp_retry_rate');
export const dlqTotal            = new Counter('cdmp_dlq_events_observed');
export const idempotencyHits     = new Counter('cdmp_idempotency_duplicates');
export const confirmFailures     = new Counter('cdmp_confirm_failures_observed');

// ── Relay performance ───────────────────────────────────────────────────────
export const relayThroughput     = new Gauge('cdmp_relay_throughput_per_sec');
export const relayLatencyP99     = new Gauge('cdmp_relay_latency_p99_ms');
