import { Injectable } from '@nestjs/common';
import {
  Registry,
  Counter,
  Histogram,
  Gauge,
  collectDefaultMetrics,
} from 'prom-client';

/**
 * MetricsService
 *
 * Single Prometheus registry shared by the whole process. Both the
 * gateway and messaging services import `MetricsModule`, each getting
 * its own registry instance (one per process) — Prometheus distinguishes
 * them at scrape time by target (job/instance labels in prometheus.yml).
 *
 * ## Metric catalogue (per observability requirements)
 *
 *  messages_processed_total{service,event_type,outcome}  Counter
 *  messages_failed_total{service,event_type,error_class} Counter
 *  dlq_messages_total{service,error_class}                Counter
 *  retry_count_total{service,attempt}                     Counter
 *  processing_duration_seconds{service,event_type,outcome} Histogram
 *  outbox_pending_events{service}                          Gauge
 *  outbox_fenced_publishes_total{service}                  Counter
 */
@Injectable()
export class MetricsService {
  readonly registry = new Registry();

  readonly messagesProcessedTotal: Counter<string>;
  readonly messagesFailedTotal: Counter<string>;
  readonly dlqMessagesTotal: Counter<string>;
  readonly retryCountTotal: Counter<string>;
  readonly processingDurationSeconds: Histogram<string>;
  readonly outboxPendingEvents: Gauge<string>;
  readonly outboxFencedPublishesTotal: Counter<string>;

  // HTTP-level metrics (gateway) — useful context alongside the
  // messaging-specific metrics above.
  readonly httpRequestDurationSeconds: Histogram<string>;
  readonly httpRequestsTotal: Counter<string>;

  constructor() {
    const serviceLabel = process.env.SERVICE_NAME ?? 'unknown';
    this.registry.setDefaultLabels({ service: serviceLabel });

    collectDefaultMetrics({ register: this.registry, prefix: 'process_' });

    this.messagesProcessedTotal = new Counter({
      name: 'messages_processed_total',
      help: 'Total number of messages successfully processed end-to-end',
      labelNames: ['service', 'event_type', 'outcome'],
      registers: [this.registry],
    });

    this.messagesFailedTotal = new Counter({
      name: 'messages_failed_total',
      help: 'Total number of messages that failed processing',
      labelNames: ['service', 'event_type', 'error_class'],
      registers: [this.registry],
    });

    this.dlqMessagesTotal = new Counter({
      name: 'dlq_messages_total',
      help: 'Total number of messages routed to the dead-letter queue',
      labelNames: ['service', 'error_class', 'original_queue'],
      registers: [this.registry],
    });

    this.retryCountTotal = new Counter({
      name: 'retry_count_total',
      help: 'Total number of retry publishes, labelled by attempt number',
      labelNames: ['service', 'attempt'],
      registers: [this.registry],
    });

    this.processingDurationSeconds = new Histogram({
      name: 'processing_duration_seconds',
      help: 'End-to-end message processing duration in seconds',
      labelNames: ['service', 'event_type', 'outcome'],
      buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
      registers: [this.registry],
    });

    this.outboxPendingEvents = new Gauge({
      name: 'outbox_pending_events',
      help: 'Current number of unpublished events sitting in the outbox table',
      labelNames: ['service'],
      registers: [this.registry],
    });

    // Incremented when OutboxRelayService.markSent()'s fencing-token
    // compare-and-swap loses a race — i.e. a relay instance successfully
    // published a row to the broker, but by the time it tried to mark
    // the row 'sent', another instance's reapStaleLocks()-triggered
    // reclaim had already bumped lock_version past what this instance
    // claimed with. This is NOT data loss (the message was published
    // either way; downstream idempotency handles any resulting
    // duplicate) but IS a signal that OUTBOX_LOCK_TTL_MS may be set too
    // low relative to actual publish latency under current load — a
    // sustained non-zero rate here is the operational trigger to
    // increase the TTL or investigate why publishes are taking long
    // enough to be reaped.
    this.outboxFencedPublishesTotal = new Counter({
      name: 'outbox_fenced_publishes_total',
      help:
        'Count of outbox publishes where the fencing-token compare-and-swap ' +
        'at markSent() time failed because another relay instance had already ' +
        'reclaimed the row after a stale-lock reap. Not data loss; a high rate ' +
        'signals OUTBOX_LOCK_TTL_MS may be too aggressive for current publish latency.',
      labelNames: ['service'],
      registers: [this.registry],
    });

    this.httpRequestDurationSeconds = new Histogram({
      name: 'http_request_duration_seconds',
      help: 'HTTP request duration in seconds',
      labelNames: ['service', 'method', 'route', 'status_code'],
      buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
      registers: [this.registry],
    });

    this.httpRequestsTotal = new Counter({
      name: 'http_requests_total',
      help: 'Total number of HTTP requests',
      labelNames: ['service', 'method', 'route', 'status_code'],
      registers: [this.registry],
    });
  }

  /** Convenience: time an async operation and observe it in one call. */
  async timeAsync<T>(
    histogramLabels: Record<string, string>,
    fn: () => Promise<T>,
  ): Promise<T> {
    const end = this.processingDurationSeconds.startTimer(histogramLabels);
    try {
      const result = await fn();
      end();
      return result;
    } catch (err) {
      end();
      throw err;
    }
  }

  async metricsText(): Promise<string> {
    return this.registry.metrics();
  }

  get contentType(): string {
    return this.registry.contentType;
  }
}
