import {
  Injectable,
  Logger,
  OnModuleInit,
  OnModuleDestroy,
} from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import type { Repository } from 'typeorm';
import { OutboxEvent } from '../entities/outbox-event.entity';
import { MetricsService, TracingService } from '@app/common';

const POLL_INTERVAL_MS = 5_000;
const GAUGE_REFRESH_INTERVAL_MS = 10_000;
const BATCH_SIZE = 25;

/**
 * OutboxService
 *
 * Minimal transactional-outbox implementation:
 *
 *  1. `record()` — called by business logic in the SAME request/handler
 *     that persists the primary row (e.g. after saving a `Message`).
 *     Writes a `pending` row to `outbox_events`.
 *
 *  2. A background loop polls for `pending` rows in small batches,
 *     "publishes" them (here: a structured log + lifecycle event — in a
 *     production system this would be a real broker publish to a
 *     downstream exchange), and flips them to `sent`.
 *
 *  3. A second, slower loop refreshes the `outbox_pending_events` gauge
 *     from `COUNT(*) WHERE status = 'pending'`. Decoupled from the
 *     publish loop so the gauge still reflects reality even if the
 *     publisher itself is stuck.
 */
@Injectable()
export class OutboxService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OutboxService.name);
  private publishTimer?: NodeJS.Timeout;
  private gaugeTimer?: NodeJS.Timeout;

  constructor(
    @InjectRepository(OutboxEvent)
    private readonly repo: Repository<OutboxEvent>,
    private readonly metrics: MetricsService,
    private readonly tracing: TracingService,
  ) {}

  onModuleInit(): void {
    this.publishTimer = setInterval(() => {
      void this.publishPendingBatch();
    }, POLL_INTERVAL_MS);

    this.gaugeTimer = setInterval(() => {
      void this.refreshPendingGauge();
    }, GAUGE_REFRESH_INTERVAL_MS);

    // Prime the gauge immediately at startup rather than waiting for the
    // first interval tick, so dashboards aren't blank for 10s after boot.
    void this.refreshPendingGauge();
  }

  onModuleDestroy(): void {
    if (this.publishTimer) clearInterval(this.publishTimer);
    if (this.gaugeTimer) clearInterval(this.gaugeTimer);
  }

  /** Record a new outbox event. Call within the same logical operation
   * that persists the business row it describes. */
  async record(
    eventType: string,
    payload: unknown,
    correlationId?: string,
  ): Promise<OutboxEvent> {
    const entry = this.repo.create({
      eventType,
      payload,
      correlationId,
      status: 'pending',
    });
    return this.repo.save(entry);
  }

  private async publishPendingBatch(): Promise<void> {
    return this.tracing.withSpan('outbox.publish_batch', async (span) => {
      const batch = await this.repo.find({
        where: { status: 'pending' },
        order: { createdAt: 'ASC' },
        take: BATCH_SIZE,
      });

      span.setAttribute('outbox.batch_size', batch.length);
      if (batch.length === 0) return;

      for (const entry of batch) {
        try {
          // Simulated downstream publish. Replace with a real broker
          // publish (e.g. to a notifications exchange) in production.
          this.logger.debug(
            `Outbox event published (id=${entry.id}, type=${entry.eventType}, correlationId=${entry.correlationId ?? 'unknown'})`,
          );
          entry.status = 'sent';
          entry.sentAt = new Date();
          await this.repo.save(entry);
        } catch (err) {
          entry.attempts += 1;
          entry.lastError = err instanceof Error ? err.message : String(err);
          if (entry.attempts >= 5) entry.status = 'failed';
          await this.repo.save(entry);
          this.logger.error(
            `Outbox publish failed (id=${entry.id}, attempts=${entry.attempts})`,
            err instanceof Error ? err.stack : undefined,
          );
        }
      }
    });
  }

  private async refreshPendingGauge(): Promise<void> {
    try {
      const pendingCount = await this.repo.count({
        where: { status: 'pending' },
      });
      this.metrics.outboxPendingEvents.set(
        { service: 'messaging' },
        pendingCount,
      );
    } catch (err) {
      this.logger.error(
        'Failed to refresh outbox_pending_events gauge',
        err instanceof Error ? err.stack : undefined,
      );
    }
  }
}
