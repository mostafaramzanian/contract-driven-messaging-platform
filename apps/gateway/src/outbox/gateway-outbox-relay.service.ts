import {
  Injectable,
  Logger,
  OnModuleInit,
  OnModuleDestroy,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectDataSource } from '@nestjs/typeorm';
import type { DataSource } from 'typeorm';
import * as amqplib from 'amqplib';
import { context } from '@opentelemetry/api';
import {
  MetricsService,
  injectTraceContext,
  extractTraceContext,
} from '@app/common';
import {
  resolveOutboxRoute,
  computeFailureOutcome,
  generateInstanceId,
} from '@app/contracts';

// ── Config defaults ────────────────────────────────────────────────────────

const DEFAULT_POLL_INTERVAL_MS = 5_000;
const DEFAULT_BATCH_SIZE = 25;
const DEFAULT_LOCK_TTL_MS = 60_000;
const DEFAULT_GAUGE_REFRESH_MS = 10_000;
const DEFAULT_REAPER_INTERVAL_MS = 30_000;
const DEFAULT_RABBITMQ_URL = 'amqp://guest:guest@localhost:5672';

// ── Row shape returned by claimBatch ──────────────────────────────────────

interface ClaimedRow {
  id: number;
  event_type: string;
  payload: unknown;
  correlation_id: string | null;
  attempts: number;
  max_attempts: number;
  lock_version: number;
  trace_context: Record<string, string> | null;
  event_id: string | null;
}

/**
 * GatewayOutboxRelayService
 *
 * Producer-side outbox relay — the asynchronous half of CRITICAL ISSUE #1's
 * fix. Polls `gateway_outbox_events` for `pending` rows written by
 * `GatewayOutboxTransactionService.record()` and publishes them to
 * RabbitMQ with publisher confirms, retrying with exponential back-off on
 * failure and dead-lettering (status='failed') once the per-row attempt
 * budget is exhausted.
 *
 * This class is a deliberate, near line-for-line adaptation of the
 * messaging service's `OutboxRelayService` — same claim query (`SELECT …
 * FOR UPDATE SKIP LOCKED` with a `lock_version` fencing token), same
 * publisher-confirms-before-markSent discipline, same stale-lock reaper,
 * same lazy-reconnecting `amqplib.ConfirmChannel`. See that class's doc
 * comments for the full rationale behind each mechanism; this file's
 * comments focus on what's specific to the PRODUCER side.
 *
 * ## Publish destination
 *
 * Routes every row through `resolveOutboxRoute()` (`@app/contracts`)
 * rather than hardcoding a destination. In practice every row the gateway
 * ever writes is a COMMAND (`CreateMessageEvent.v1`/`.v2`), so this always
 * resolves to `messaging.direct` / `messaging.work` today — but using the
 * shared resolver (instead of re-hardcoding `EXCHANGES.MAIN` /
 * `ROUTING_KEYS.WORK` a second time) means if the gateway is ever extended
 * to also emit a genuine domain event of its own, it is automatically
 * routed onto the event bus, not the command queue, with zero changes to
 * this class. This is the SAME chokepoint that fixes Architectural Gap #2
 * on the messaging side — reused here for consistency, not duplicated.
 *
 * ## Horizontal scaling (SKIP LOCKED)
 *
 * Multiple gateway replicas (and therefore multiple relay instances) are
 * safe to run concurrently. The claim query's `FOR UPDATE SKIP LOCKED`
 * guarantees each instance atomically acquires a disjoint batch — no row
 * is published twice because two instances raced for the same claim.
 *
 * ## Crash recovery
 *
 * If a gateway process crashes after an HTTP request commits an outbox
 * row but before the relay (in this process or another) publishes it, the
 * row simply sits `pending` — durable in Postgres — until ANY live relay
 * instance (this one on restart, or a different replica) claims and
 * publishes it. If a relay instance crashes mid-publish (after claiming,
 * before `markSent()`), `reapStaleLocks()` clears the stale lock after
 * `OUTBOX_LOCK_TTL_MS` so another instance can reclaim it; the
 * `lock_version` fencing token (see `claimBatch()`/`markSent()`) prevents
 * a double-publish if the original claimant was merely slow rather than
 * actually dead.
 *
 * ## Config env vars
 *
 *  GATEWAY_OUTBOX_POLL_INTERVAL_MS   (default 5000)
 *  GATEWAY_OUTBOX_BATCH_SIZE         (default 25)
 *  GATEWAY_OUTBOX_LOCK_TTL_MS        (default 60000)
 *  GATEWAY_OUTBOX_GAUGE_REFRESH_MS   (default 10000)
 *  GATEWAY_OUTBOX_REAPER_INTERVAL_MS (default 30000)
 *  RABBITMQ_URL                      (default amqp://guest:guest@localhost:5672)
 */
@Injectable()
export class GatewayOutboxRelayService
  implements OnModuleInit, OnModuleDestroy
{
  private readonly logger = new Logger(GatewayOutboxRelayService.name);

  /** Unique id for this relay instance — observability only */
  private readonly instanceId = generateInstanceId();

  private publishTimer?: NodeJS.Timeout;
  private gaugeTimer?: NodeJS.Timeout;
  private reaperTimer?: NodeJS.Timeout;

  // ── AMQP lazy-connect state ───────────────────────────────────────────
  private amqpConnection?: amqplib.ChannelModel;
  private amqpChannel?: amqplib.ConfirmChannel;
  private connecting?: Promise<void>;

  constructor(
    @InjectDataSource()
    private readonly dataSource: DataSource,
    private readonly metrics: MetricsService,
    private readonly configService: ConfigService,
  ) {}

  // ── Lifecycle ──────────────────────────────────────────────────────────

  onModuleInit(): void {
    const pollMs = this.cfg(
      'GATEWAY_OUTBOX_POLL_INTERVAL_MS',
      DEFAULT_POLL_INTERVAL_MS,
    );
    const gaugeMs = this.cfg(
      'GATEWAY_OUTBOX_GAUGE_REFRESH_MS',
      DEFAULT_GAUGE_REFRESH_MS,
    );
    const reaperMs = this.cfg(
      'GATEWAY_OUTBOX_REAPER_INTERVAL_MS',
      DEFAULT_REAPER_INTERVAL_MS,
    );

    this.publishTimer = setInterval(() => {
      void this.runRelayTick();
    }, pollMs);

    this.gaugeTimer = setInterval(() => {
      void this.refreshPendingGauge();
    }, gaugeMs);

    this.reaperTimer = setInterval(() => {
      void this.reapStaleLocks();
    }, reaperMs);

    // Prime the gauge immediately so dashboards show a value at startup.
    void this.refreshPendingGauge();

    this.logger.log(
      `GatewayOutboxRelayService started (instanceId=${this.instanceId}, ` +
        `poll=${pollMs}ms, batch=${this.cfg('GATEWAY_OUTBOX_BATCH_SIZE', DEFAULT_BATCH_SIZE)}, ` +
        `lockTtl=${this.cfg('GATEWAY_OUTBOX_LOCK_TTL_MS', DEFAULT_LOCK_TTL_MS)}ms)`,
    );
  }

  async onModuleDestroy(): Promise<void> {
    if (this.publishTimer) clearInterval(this.publishTimer);
    if (this.gaugeTimer) clearInterval(this.gaugeTimer);
    if (this.reaperTimer) clearInterval(this.reaperTimer);
    await this.closeAmqp();
  }

  // ── Core relay tick ────────────────────────────────────────────────────

  /** Claim a batch of pending rows and publish each one. */
  async runRelayTick(): Promise<void> {
    let rows: ClaimedRow[];
    try {
      rows = await this.claimBatch();
    } catch (err: unknown) {
      this.logger.error(
        'claimBatch failed',
        err instanceof Error ? err.stack : String(err),
      );
      return;
    }

    if (rows.length === 0) return;

    this.logger.debug(
      `Claimed ${rows.length} gateway outbox row(s) for publish (instance=${this.instanceId})`,
    );

    for (const row of rows) {
      await this.publishOne(row);
    }
  }

  // ── Claim (SKIP LOCKED) ────────────────────────────────────────────────

  /**
   * Atomically lock a batch of pending rows ready to publish, incrementing
   * each row's `lock_version` fencing token in the same UPDATE. Identical
   * mechanism (and identical rationale) to
   * `OutboxRelayService.claimBatch()` in the messaging app — see that
   * method's doc comment for the full explanation of why `SKIP LOCKED`
   * alone is insufficient and `lock_version` closes the remaining gap.
   */
  async claimBatch(): Promise<ClaimedRow[]> {
    const batchSize = this.cfg('GATEWAY_OUTBOX_BATCH_SIZE', DEFAULT_BATCH_SIZE);

    const rows: ClaimedRow[] = await this.dataSource.query(
      `
      UPDATE gateway_outbox_events
      SET    locked_at    = now(),
             locked_by    = $1,
             lock_version = lock_version + 1
      WHERE  id IN (
        SELECT id
        FROM   gateway_outbox_events
        WHERE  status        = 'pending'
          AND  next_retry_at <= now()
        ORDER BY next_retry_at ASC
        LIMIT  $2
        FOR UPDATE SKIP LOCKED
      )
      RETURNING
        id,
        event_type,
        payload,
        correlation_id,
        attempts,
        max_attempts,
        lock_version,
        trace_context,
        event_id
      `,
      [this.instanceId, batchSize],
    );

    return rows;
  }

  // ── Publish ────────────────────────────────────────────────────────────

  /**
   * Publish a single claimed row to RabbitMQ, then mark it sent or failed.
   *
   * Same three guarantees as the messaging service's
   * `OutboxRelayService.publishOne()`:
   *  1. Publisher confirms — `markSent()` is only reached after
   *     `ch.waitForConfirms()` resolves, i.e. the BROKER (not just the
   *     local write buffer) has acknowledged the message.
   *  2. Fencing token — `row.lock_version` is threaded through to
   *     `markSent()`/`markFailedAttempt()`, both compare-and-swap against
   *     the row's CURRENT `lock_version` rather than unconditionally
   *     overwriting it.
   *  3. Trace propagation — if `row.trace_context` was captured at write
   *     time, it is restored via `extractTraceContext()` + `context.with()`
   *     so the publish carries the ORIGINAL HTTP request's trace, not this
   *     poll tick's ambient one.
   */
  async publishOne(row: ClaimedRow): Promise<void> {
    try {
      const ch = await this.getChannel();
      const route = resolveOutboxRoute(row.event_type);

      const publishWithRestoredTrace = (): boolean => {
        const headers = injectTraceContext({
          'x-event-type': row.event_type,
          'x-correlation-id': row.correlation_id ?? '',
          'x-gateway-outbox-id': String(row.id),
          'x-event-id': row.event_id ?? '',
        });

        const body = Buffer.from(JSON.stringify(row.payload));

        return ch.publish(route.exchange, route.routingKey, body, {
          persistent: true,
          contentType: 'application/json',
          headers,
        });
      };

      const published = row.trace_context
        ? context.with(
            extractTraceContext(
              row.trace_context as amqplib.MessagePropertyHeaders,
            ),
            publishWithRestoredTrace,
          )
        : publishWithRestoredTrace();

      if (!published) {
        throw new Error(
          `channel.publish() returned false (back-pressure) for gateway outbox id=${row.id}`,
        );
      }

      // Requirement: wait for the BROKER's confirmation, not just the
      // local write-buffer's acceptance, before this row can be marked sent.
      await ch.waitForConfirms();

      const stillOwned = await this.markSent(row.id, row.lock_version);

      if (!stillOwned) {
        this.metrics.outboxFencedPublishesTotal.inc({ service: 'gateway' });
        this.logger.warn(
          `Gateway outbox event ${row.id} was published, but lock_version no longer ` +
            `matched at markSent() time (instance=${this.instanceId}) — another ` +
            `relay instance reclaimed this row's lock after a stale-lock reap. ` +
            `The message has already been published to the broker by THIS call ` +
            `regardless — downstream consumer idempotency (eventId-keyed) is what ` +
            `prevents this from being processed twice, not relay-side suppression.`,
        );
        return;
      }

      this.logger.debug(
        `Gateway outbox event published and broker-confirmed (id=${row.id}, ` +
          `type=${row.event_type}, exchange=${route.exchange}, ` +
          `correlationId=${row.correlation_id ?? 'none'}, lockVersion=${row.lock_version})`,
      );
    } catch (err: unknown) {
      await this.markFailedAttempt(row, err);
    }
  }

  // ── Mark sent ──────────────────────────────────────────────────────────

  /**
   * Mark a row `sent`, but ONLY if its `lock_version` still equals
   * `expectedLockVersion`. See `OutboxRelayService.markSent()` (messaging
   * app) for the full race this fencing token prevents — identical here.
   */
  async markSent(id: number, expectedLockVersion: number): Promise<boolean> {
    const result: { id: number }[] = await this.dataSource.query(
      `UPDATE gateway_outbox_events
       SET status     = 'sent',
           sent_at    = now(),
           locked_at  = NULL,
           locked_by  = NULL
       WHERE id = $1
         AND lock_version = $2
       RETURNING id`,
      [id, expectedLockVersion],
    );

    return result.length > 0;
  }

  // ── Mark failed attempt ────────────────────────────────────────────────

  /**
   * Record a failed publish attempt, applying the SAME fencing-token
   * compare-and-swap as `markSent()`.
   */
  async markFailedAttempt(row: ClaimedRow, err: unknown): Promise<void> {
    const errMsg = err instanceof Error ? err.message : String(err);
    const outcome = computeFailureOutcome(row.attempts, row.max_attempts);

    const newStatus = outcome.action === 'dead-letter' ? 'failed' : 'pending';
    const nextRetryAt =
      outcome.action === 'retry' ? outcome.nextRetryAt : new Date();

    const result: { id: number }[] = await this.dataSource.query(
      `UPDATE gateway_outbox_events
       SET status        = $1,
           attempts      = $2,
           last_error    = $3,
           next_retry_at = $4,
           locked_at     = NULL,
           locked_by     = NULL
       WHERE id = $5
         AND lock_version = $6
       RETURNING id`,
      [
        newStatus,
        outcome.newAttempts,
        errMsg,
        nextRetryAt,
        row.id,
        row.lock_version,
      ],
    );

    const stillOwned = result.length > 0;

    if (!stillOwned) {
      this.metrics.outboxFencedPublishesTotal.inc({ service: 'gateway' });
      this.logger.warn(
        `Gateway outbox event ${row.id} failed to publish, but lock_version no longer ` +
          `matched at markFailedAttempt() time (instance=${this.instanceId}) — ` +
          `another relay instance reclaimed this row after a stale-lock reap. ` +
          `This instance's failure bookkeeping was NOT applied.`,
      );
      return;
    }

    if (outcome.action === 'dead-letter') {
      this.logger.error(
        `Gateway outbox event dead-lettered (id=${row.id}, type=${row.event_type}, ` +
          `attempts=${outcome.newAttempts}/${row.max_attempts}): ${errMsg}`,
      );
    } else {
      this.logger.warn(
        `Gateway outbox publish failed, scheduled retry (id=${row.id}, ` +
          `attempt=${outcome.newAttempts}/${row.max_attempts}, ` +
          `nextRetryAt=${nextRetryAt.toISOString()}): ${errMsg}`,
      );
    }
  }

  // ── Stale-lock reaper ──────────────────────────────────────────────────

  /**
   * Release locks held by crashed or timed-out relay instances. Identical
   * mechanism to `OutboxRelayService.reapStaleLocks()` (messaging app).
   */
  async reapStaleLocks(): Promise<void> {
    const ttlMs = this.cfg('GATEWAY_OUTBOX_LOCK_TTL_MS', DEFAULT_LOCK_TTL_MS);
    try {
      const result: { id: number }[] = await this.dataSource.query(
        `UPDATE gateway_outbox_events
         SET locked_at = NULL,
             locked_by = NULL
         WHERE status    = 'pending'
           AND locked_at IS NOT NULL
           AND locked_at < now() - (interval '1 millisecond' * $1)
         RETURNING id`,
        [ttlMs],
      );

      if (result.length > 0) {
        this.logger.warn(
          `Reaped ${result.length} stale gateway outbox lock(s) ` +
            `(ttl=${ttlMs}ms, instance=${this.instanceId})`,
        );
      }
    } catch (err: unknown) {
      this.logger.error(
        'reapStaleLocks failed',
        err instanceof Error ? err.stack : String(err),
      );
    }
  }

  // ── Gauge ──────────────────────────────────────────────────────────────

  async refreshPendingGauge(): Promise<void> {
    try {
      const rows: [{ count: number }] = await this.dataSource.query(
        `SELECT COUNT(*)::int AS count FROM gateway_outbox_events WHERE status = 'pending'`,
      );
      const count = rows[0]?.count ?? 0;
      this.metrics.outboxPendingEvents.set({ service: 'gateway' }, count);
    } catch (err: unknown) {
      this.logger.error(
        'refreshPendingGauge failed',
        err instanceof Error ? err.stack : String(err),
      );
    }
  }

  // ── AMQP lazy-connect ──────────────────────────────────────────────────

  /**
   * `protected`, not `private` — the only deliberate testability seam in
   * this class, mirroring `OutboxRelayService.getChannel()` in the
   * messaging app for the same reason: tests override this with a fake
   * `ConfirmChannel` double instead of attempting a real AMQP connection,
   * without changing any other production code path.
   */
  protected async getChannel(): Promise<amqplib.ConfirmChannel> {
    if (this.amqpChannel) return this.amqpChannel;
    if (this.connecting) {
      await this.connecting;
      return this.amqpChannel!;
    }
    this.connecting = this.connectAmqp();
    await this.connecting;
    return this.amqpChannel!;
  }

  private async connectAmqp(): Promise<void> {
    const url =
      this.configService.get<string>('RABBITMQ_URL') ?? DEFAULT_RABBITMQ_URL;

    const conn = await amqplib.connect(url);
    this.amqpConnection = conn;
    this.amqpChannel = await conn.createConfirmChannel();

    const reset = () => {
      this.amqpChannel = undefined;
      this.amqpConnection = undefined;
      this.connecting = undefined;
    };

    conn.on('error', (err: Error) => {
      this.logger.error(
        `GatewayOutboxRelay AMQP error: ${err.message}`,
        err.stack,
      );
      reset();
    });
    conn.on('close', () => {
      this.logger.warn('GatewayOutboxRelay AMQP connection closed');
      reset();
    });
  }

  private async closeAmqp(): Promise<void> {
    try {
      await this.amqpChannel?.close();
      await this.amqpConnection?.close();
    } catch {
      /* ignore on shutdown */
    }
  }

  // ── Helpers ────────────────────────────────────────────────────────────

  private cfg(key: string, defaultValue: number): number {
    const raw = this.configService.get<string>(key);
    const parsed = raw ? Number.parseInt(raw, 10) : NaN;
    return Number.isFinite(parsed) ? parsed : defaultValue;
  }
}
