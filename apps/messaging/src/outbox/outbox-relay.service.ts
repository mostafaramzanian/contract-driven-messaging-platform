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
import { resolveOutboxRoute } from '../reliability/topology';
import {
  computeFailureOutcome,
  generateInstanceId,
} from './outbox-retry-policy';

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
  /**
   * Fencing token for this claim. Returned by claimBatch() at the exact
   * moment the row is locked (after the UPDATE's increment), and must be
   * presented unchanged to markSent()/markFailedAttempt() — see those
   * methods' doc comments for why this prevents the double-publish race
   * left open when only SKIP LOCKED is relied upon (Requirement 2).
   */
  lock_version: number;
  /**
   * W3C trace-context propagation carrier captured at outbox-row write
   * time (see OutboxTransactionService.insertOutboxEvents), or null for
   * rows written before this column existed / with no active span at
   * write time. Used by publishOne() to restore the ORIGINAL producer's
   * trace instead of the relay's own ambient context (Requirement 3).
   */
  trace_context: Record<string, string> | null;
  /**
   * Stable application-level event identifier (see OutboxEvent.eventId's
   * doc comment). Included in the AMQP headers on publish so a
   * downstream consumer's idempotency check has a stable id to key on
   * even across an operator-triggered replay of this same row.
   */
  event_id: string | null;
}

/**
 * OutboxRelayService
 *
 * Polls the `outbox_events` table for `pending` rows and publishes them to
 * RabbitMQ using whichever exchange/routing-key `resolveOutboxRoute()`
 * (from `@app/contracts`, re-exported via `../reliability/topology`)
 * returns for that row's `event_type` — commands (`CreateMessageEvent.v1`/
 * `.v2`) go to `messaging.direct` / `messaging.work`; everything else (e.g.
 * `MessagePersisted`) is treated as a domain event and goes to the
 * `messaging.events` fanout exchange instead. See that function's doc
 * comment for the full rationale (Architectural Gap #2 — this is what
 * stops a domain event from ever re-entering the command queue and
 * creating a self-generated retry/DLQ loop).
 *
 * ## Horizontal scaling (SKIP LOCKED)
 *
 * Multiple relay instances running concurrently are safe.  The claim query
 * uses `SELECT … FOR UPDATE SKIP LOCKED` so each instance atomically
 * acquires a disjoint batch — no row is published twice.
 *
 * ## Failure / retry
 *
 * On publish failure `computeFailureOutcome()` (from outbox-retry-policy.ts)
 * is consulted.  The row is either:
 *  - scheduled for retry after an exponential back-off delay (2 s → 4 s → …)
 *  - transitioned to `failed` when attempts >= max_attempts
 *
 * ## Stale-lock reaper
 *
 * A second timer calls `reapStaleLocks()` every OUTBOX_REAPER_INTERVAL_MS
 * (default 30 s).  Any `pending` row locked_at older than OUTBOX_LOCK_TTL_MS
 * (default 60 s) has its lock cleared so it can be reclaimed.
 *
 * ## AMQP connection
 *
 * Keeps a single lazy-connected channel (same pattern as RetryPublisherService).
 * The channel is reused across polls; a connection error clears the cached
 * handle so the next poll triggers a reconnect.
 *
 * ## Config env vars
 *
 *  OUTBOX_POLL_INTERVAL_MS   (default 5000)
 *  OUTBOX_BATCH_SIZE         (default 25)
 *  OUTBOX_LOCK_TTL_MS        (default 60000)
 *  OUTBOX_GAUGE_REFRESH_MS   (default 10000)
 *  OUTBOX_REAPER_INTERVAL_MS (default 30000)
 *  RABBITMQ_URL              (default amqp://guest:guest@localhost:5672)
 */
@Injectable()
export class OutboxRelayService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OutboxRelayService.name);

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
      'OUTBOX_POLL_INTERVAL_MS',
      DEFAULT_POLL_INTERVAL_MS,
    );
    const gaugeMs = this.cfg(
      'OUTBOX_GAUGE_REFRESH_MS',
      DEFAULT_GAUGE_REFRESH_MS,
    );
    const reaperMs = this.cfg(
      'OUTBOX_REAPER_INTERVAL_MS',
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
      `OutboxRelayService started (instanceId=${this.instanceId}, ` +
        `poll=${pollMs}ms, batch=${this.cfg('OUTBOX_BATCH_SIZE', DEFAULT_BATCH_SIZE)}, ` +
        `lockTtl=${this.cfg('OUTBOX_LOCK_TTL_MS', DEFAULT_LOCK_TTL_MS)}ms)`,
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
      `Claimed ${rows.length} outbox row(s) for publish (instance=${this.instanceId})`,
    );

    for (const row of rows) {
      await this.publishOne(row);
    }
  }

  // ── Claim (SKIP LOCKED) ────────────────────────────────────────────────

  /**
   * Atomically lock a batch of pending rows that are ready to publish,
   * incrementing each row's `lock_version` fencing token in the same
   * UPDATE.
   *
   * `SKIP LOCKED` alone guarantees that two relay instances never claim
   * the SAME row in the SAME claimBatch() call — that part was already
   * correct. What it does NOT guarantee is that a claimant which is
   * merely slow (a GC pause, a slow network round-trip to RabbitMQ —
   * not necessarily dead) can't have its row reassigned to a second
   * claimant by `reapStaleLocks()`, after which BOTH the original, slow
   * claimant and the new claimant believe they own the row and may both
   * publish it (Requirement 2 — see markSent()/markFailedAttempt() for
   * the other half of this fix).
   *
   * `lock_version` closes that gap: every successful claim — whether the
   * row was previously unclaimed or whether `reapStaleLocks()` just
   * cleared a stale lock — increments this counter. The caller of
   * claimBatch() must carry the returned `lock_version` through to
   * markSent()/markFailedAttempt(), which only apply their effect if the
   * row's CURRENT lock_version still matches what was claimed with. A
   * stale claimant's eventual write loses the compare-and-swap and is
   * logged as a detected (not silently-allowed) lost race.
   */
  async claimBatch(): Promise<ClaimedRow[]> {
    const batchSize = this.cfg('OUTBOX_BATCH_SIZE', DEFAULT_BATCH_SIZE);

    const rows: ClaimedRow[] = await this.dataSource.query(
      `
      UPDATE outbox_events
      SET    locked_at    = now(),
             locked_by    = $1,
             lock_version = lock_version + 1
      WHERE  id IN (
        SELECT id
        FROM   outbox_events
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
   * ## Requirement 1 — publisher confirms
   *
   * `ch.publish()`'s synchronous boolean return reflects ONLY local
   * write-buffer back-pressure — it says nothing about whether the
   * broker actually received and durably queued the message. Previously,
   * a `true` return was sufficient to call `markSent()`. Now, after a
   * `true` (locally-accepted) publish, this method additionally awaits
   * `ch.waitForConfirms()` on the now-confirm-mode channel, which only
   * resolves once the broker has acknowledged (or rejects if the broker
   * nacks) this publish. `markSent()` is only reached after that await
   * succeeds — an event can only be marked `sent` once the broker has
   * actually confirmed it, not merely accepted a local write.
   *
   * ## Requirement 2 — fencing token
   *
   * `row.lock_version` (captured at claim time, see claimBatch()) is
   * threaded through to `markSent()`/`markFailedAttempt()`, both of
   * which now perform a compare-and-swap against the row's CURRENT
   * lock_version rather than an unconditional UPDATE. See those methods.
   *
   * ## Requirement 3 — trace propagation
   *
   * If `row.trace_context` was captured at outbox-row write time (see
   * `OutboxTransactionService.insertOutboxEvents`), this method restores
   * it via `extractTraceContext()` + `context.with()` BEFORE building
   * headers and publishing — so `injectTraceContext()` (called from
   * inside that restored context) emits the ORIGINAL producer's
   * `traceparent`, not whatever ambient context exists on this
   * `setInterval` poll tick (which has no parent span at all). This is
   * what keeps Gateway → Outbox → Relay → RabbitMQ → Consumer as a
   * single distributed trace instead of two disconnected ones that
   * happen to share a correlationId.
   */
  async publishOne(row: ClaimedRow): Promise<void> {
    try {
      const ch = await this.getChannel();

      const route = resolveOutboxRoute(row.event_type);

      const publishWithRestoredTrace = (): boolean => {
        const headers = injectTraceContext({
          'x-event-type': row.event_type,
          'x-correlation-id': row.correlation_id ?? '',
          'x-outbox-id': String(row.id),
          'x-event-id': row.event_id ?? '',
        });

        const body = Buffer.from(JSON.stringify(row.payload));

        return ch.publish(route.exchange, route.routingKey, body, {
          persistent: true,
          contentType: 'application/json',
          headers,
        });
      };

      // Restore the ORIGINAL producer's trace context (if one was
      // captured) for the duration of header-building + publish, so
      // injectTraceContext() picks it up via context.active() instead of
      // this poll tick's ambient (parentless) context. extractTraceContext
      // + context.with() are both safe no-ops when no real ContextManager
      // is registered (e.g. OTEL_SDK_DISABLED=true in tests) — they do
      // not throw, they simply don't change what gets injected.
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
          `channel.publish() returned false (back-pressure) for outbox id=${row.id}`,
        );
      }

      // Requirement 1: wait for the BROKER's confirmation, not just the
      // local write-buffer's acceptance, before this row can be marked
      // sent. Rejects if the broker nacks the publish.
      await ch.waitForConfirms();

      // Requirement 2: markSent() is a compare-and-swap against
      // row.lock_version, not an unconditional UPDATE. If this instance's
      // claim was reaped and reclaimed by another instance in the
      // meantime, this returns false and is logged as a lost race rather
      // than silently succeeding after the fact.
      const stillOwned = await this.markSent(row.id, row.lock_version);

      if (!stillOwned) {
        this.metrics.outboxFencedPublishesTotal.inc({ service: 'messaging' });
        this.logger.warn(
          `Outbox event ${row.id} was published, but lock_version no longer ` +
            `matched at markSent() time (instance=${this.instanceId}) — another ` +
            `relay instance reclaimed this row's lock after a stale-lock reap. ` +
            `The row's status was NOT changed by this call; whichever instance's ` +
            `markSent() call wins the compare-and-swap is authoritative. The ` +
            `message has already been published to the broker by THIS call ` +
            `regardless — downstream consumer idempotency (eventId-keyed) is what ` +
            `prevents this from being processed twice, not relay-side suppression.`,
        );
        return;
      }

      this.logger.debug(
        `Outbox event published and broker-confirmed (id=${row.id}, type=${row.event_type}, ` +
          `correlationId=${row.correlation_id ?? 'none'}, lockVersion=${row.lock_version})`,
      );
    } catch (err: unknown) {
      await this.markFailedAttempt(row, err);
    }
  }

  // ── Mark sent ──────────────────────────────────────────────────────────

  /**
   * Mark a row `sent`, but ONLY if its `lock_version` still equals
   * `expectedLockVersion` — the fencing token returned by claimBatch()
   * at the moment this instance claimed the row.
   *
   * ## Why a compare-and-swap, not an unconditional UPDATE
   *
   * `SKIP LOCKED` guarantees no two instances claim the same row in the
   * same claimBatch() call. It does NOT guarantee that a row's lock
   * can't be reassigned mid-flight: `reapStaleLocks()` releases any lock
   * older than `OUTBOX_LOCK_TTL_MS`, on the assumption the original
   * claimant is dead. If that claimant is merely slow (a GC pause, a
   * slow round-trip to a struggling broker — not necessarily dead), a
   * second instance can claim and publish the SAME row while the first
   * is still mid-publish. Without a fencing token, whichever instance's
   * markSent() call runs LAST would unconditionally win, silently
   * overwriting `locked_by`/`sent_at` with no record that two instances
   * ever raced for this row.
   *
   * With the fencing token: `WHERE id = $1 AND lock_version = $2` only
   * matches if THIS call's claim is still the most recent one. If a
   * second instance reclaimed the row in between (bumping lock_version),
   * this UPDATE matches zero rows, returns `false`, and the caller
   * (`publishOne`) logs it as a detected lost race rather than silently
   * succeeding. The row's `status`/`sent_at` are left exactly as the
   * WINNING instance's call set them.
   *
   * @returns `true` if this call's fencing token was still current (the
   *          row is now `sent`); `false` if another instance's claim had
   *          already superseded it (the row was NOT modified by this call).
   */
  async markSent(id: number, expectedLockVersion: number): Promise<boolean> {
    const result: { id: number }[] = await this.dataSource.query(
      `UPDATE outbox_events
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
   * compare-and-swap as markSent() (see that method's doc comment for
   * the full race this prevents).
   *
   * Without this, a stale claimant whose publish failed (e.g. its AMQP
   * connection died right as `reapStaleLocks()` released its lock) could
   * otherwise reset `status`/`attempts`/`next_retry_at` on a row that a
   * SECOND instance has since reclaimed and may already be successfully
   * republishing — clobbering that second instance's in-flight work with
   * stale failure bookkeeping.
   */
  async markFailedAttempt(row: ClaimedRow, err: unknown): Promise<void> {
    const errMsg = err instanceof Error ? err.message : String(err);
    const outcome = computeFailureOutcome(row.attempts, row.max_attempts);

    const newStatus = outcome.action === 'dead-letter' ? 'failed' : 'pending';
    const nextRetryAt =
      outcome.action === 'retry' ? outcome.nextRetryAt : new Date();

    const result: { id: number }[] = await this.dataSource.query(
      `UPDATE outbox_events
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
      this.metrics.outboxFencedPublishesTotal.inc({ service: 'messaging' });
      this.logger.warn(
        `Outbox event ${row.id} failed to publish, but lock_version no longer ` +
          `matched at markFailedAttempt() time (instance=${this.instanceId}) — ` +
          `another relay instance reclaimed this row after a stale-lock reap. ` +
          `This instance's failure bookkeeping was NOT applied; the reclaiming ` +
          `instance's own outcome (success or failure) is authoritative.`,
      );
      return;
    }

    if (outcome.action === 'dead-letter') {
      this.logger.error(
        `Outbox event dead-lettered (id=${row.id}, type=${row.event_type}, ` +
          `attempts=${outcome.newAttempts}/${row.max_attempts}): ${errMsg}`,
      );
    } else {
      this.logger.warn(
        `Outbox publish failed, scheduled retry (id=${row.id}, ` +
          `attempt=${outcome.newAttempts}/${row.max_attempts}, ` +
          `nextRetryAt=${nextRetryAt.toISOString()}): ${errMsg}`,
      );
    }
  }

  // ── Stale-lock reaper ──────────────────────────────────────────────────

  /**
   * Release locks held by crashed or timed-out relay instances.
   *
   * Any `pending` row with `locked_at` older than OUTBOX_LOCK_TTL_MS is
   * assumed to have been orphaned.  Clearing the lock makes the row
   * eligible for the next claim cycle.
   */
  async reapStaleLocks(): Promise<void> {
    const ttlMs = this.cfg('OUTBOX_LOCK_TTL_MS', DEFAULT_LOCK_TTL_MS);
    try {
      const result: { id: number }[] = await this.dataSource.query(
        `UPDATE outbox_events
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
          `Reaped ${result.length} stale outbox lock(s) ` +
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
        `SELECT COUNT(*)::int AS count FROM outbox_events WHERE status = 'pending'`,
      );
      const count = rows[0]?.count ?? 0;
      this.metrics.outboxPendingEvents.set({ service: 'messaging' }, count);
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
   * this class. Tests override this (via a thin test subclass, see
   * outbox-relay.service.spec.ts) to inject a fake ConfirmChannel/
   * DataSource double instead of attempting a real AMQP connection,
   * without changing any production code path: onModuleInit, the timers,
   * and connectAmqp() itself are completely untouched by this seam and
   * behave identically whether or not a test overrides getChannel().
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
    // createConfirmChannel, not createChannel — see publishOne's doc
    // comment for why this is load-bearing: it is what makes
    // waitForConfirms() available, which is the actual broker-durability
    // guarantee markSent() now depends on (Requirement 1).
    this.amqpChannel = await conn.createConfirmChannel();

    const reset = () => {
      this.amqpChannel = undefined;
      this.amqpConnection = undefined;
      this.connecting = undefined;
    };

    conn.on('error', (err: Error) => {
      this.logger.error(`OutboxRelay AMQP error: ${err.message}`, err.stack);
      reset();
    });
    conn.on('close', () => {
      this.logger.warn('OutboxRelay AMQP connection closed');
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
